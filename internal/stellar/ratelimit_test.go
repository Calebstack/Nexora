package stellar

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestLimiter(t *testing.T, cfg RateLimitConfig) (*RateLimiter, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rl := NewRateLimiter(rdb, cfg)
	return rl, mr
}

func freezeClock(rl *RateLimiter) time.Time {
	t0 := time.Unix(1700000000, 0)
	rl.now = func() time.Time { return t0 }
	return t0
}

func TestRateLimiter_NilAlwaysAdmits(t *testing.T) {
	var rl *RateLimiter
	if err := rl.Wait(context.Background(), PriorityHigh); err != nil {
		t.Fatalf("nil limiter Wait: %v", err)
	}
	rl2 := NewRateLimiter(nil, DefaultRateLimitConfig())
	ok, _, err := rl2.TryAcquire(context.Background(), PriorityNormal)
	if err != nil || !ok {
		t.Fatalf("nil-redis limiter should admit: ok=%v err=%v", ok, err)
	}
}

func TestRateLimiter_AdmitsUpToCeilingThenThrottles(t *testing.T) {
	rl, _ := newTestLimiter(t, RateLimitConfig{
		Capacity: 10, RefillPerSec: 1, HighReserveFraction: 0.2, LowCeilingFraction: 0.6,
	})
	freezeClock(rl) // no leak during the test
	ctx := context.Background()

	// Normal ceiling = capacity*(1-0.2) = 8.
	for i := 0; i < 8; i++ {
		ok, _, err := rl.TryAcquire(ctx, PriorityNormal)
		if err != nil || !ok {
			t.Fatalf("normal admit %d: ok=%v err=%v", i, ok, err)
		}
	}
	if ok, wait, _ := rl.TryAcquire(ctx, PriorityNormal); ok {
		t.Fatal("9th normal request should be throttled")
	} else if wait <= 0 {
		t.Fatal("throttled request should report a positive retry-after")
	}

	// High priority draws on the reserved headroom (ceiling = full capacity 10).
	if ok, _, _ := rl.TryAcquire(ctx, PriorityHigh); !ok {
		t.Fatal("high-priority request should use reserved headroom (level 8→9)")
	}
	if ok, _, _ := rl.TryAcquire(ctx, PriorityHigh); !ok {
		t.Fatal("high-priority request should use reserved headroom (level 9→10)")
	}
	if ok, _, _ := rl.TryAcquire(ctx, PriorityHigh); ok {
		t.Fatal("high-priority request should be throttled once full capacity is reached")
	}

	m := rl.Metrics()
	if m.Admitted != 10 {
		t.Fatalf("admitted = %d, want 10", m.Admitted)
	}
	if m.Throttled != 2 {
		t.Fatalf("throttled = %d, want 2", m.Throttled)
	}
}

func TestRateLimiter_LowPriorityCeiling(t *testing.T) {
	rl, _ := newTestLimiter(t, RateLimitConfig{
		Capacity: 10, RefillPerSec: 1, HighReserveFraction: 0.2, LowCeilingFraction: 0.6,
	})
	freezeClock(rl)
	ctx := context.Background()

	for i := 0; i < 6; i++ { // low ceiling = 10*0.6 = 6
		if ok, _, _ := rl.TryAcquire(ctx, PriorityLow); !ok {
			t.Fatalf("low admit %d should succeed", i)
		}
	}
	if ok, _, _ := rl.TryAcquire(ctx, PriorityLow); ok {
		t.Fatal("7th low request should be throttled at the low ceiling")
	}
	// Normal still has headroom (ceiling 8) above the low ceiling.
	if ok, _, _ := rl.TryAcquire(ctx, PriorityNormal); !ok {
		t.Fatal("normal request should still be admitted above the low ceiling")
	}
}

func TestRateLimiter_LeaksOverTime(t *testing.T) {
	rl, _ := newTestLimiter(t, RateLimitConfig{
		Capacity: 5, RefillPerSec: 10, HighReserveFraction: 0.2, LowCeilingFraction: 0.6,
	})
	t0 := time.Unix(1700000000, 0)
	cur := t0
	rl.now = func() time.Time { return cur }
	ctx := context.Background()

	// High ceiling = 5; fill it.
	for i := 0; i < 5; i++ {
		if ok, _, _ := rl.TryAcquire(ctx, PriorityHigh); !ok {
			t.Fatalf("fill admit %d failed", i)
		}
	}
	if ok, _, _ := rl.TryAcquire(ctx, PriorityHigh); ok {
		t.Fatal("bucket should be full")
	}
	// 10 req/s = 1 leak per 100ms. Advance 500ms → ~5 leaked, bucket empty.
	cur = cur.Add(500 * time.Millisecond)
	if ok, _, _ := rl.TryAcquire(ctx, PriorityHigh); !ok {
		t.Fatal("request should be admitted after the bucket leaks")
	}
}

func TestRateLimiter_WaitTimesOutWhenSaturated(t *testing.T) {
	rl, _ := newTestLimiter(t, RateLimitConfig{
		Capacity: 1, RefillPerSec: 1, HighReserveFraction: 0.2, LowCeilingFraction: 0.6,
		MaxWait: 80 * time.Millisecond,
	})
	freezeClock(rl) // frozen → never leaks, so Wait must hit MaxWait
	ctx := context.Background()

	if ok, _, _ := rl.TryAcquire(ctx, PriorityHigh); !ok {
		t.Fatal("first request should fill the capacity-1 bucket")
	}
	start := time.Now()
	err := rl.Wait(ctx, PriorityHigh)
	if err == nil {
		t.Fatal("Wait should fail once MaxWait elapses on a saturated frozen bucket")
	}
	if time.Since(start) < 60*time.Millisecond {
		t.Fatalf("Wait returned too early: %v", time.Since(start))
	}
}

func TestRateLimiter_ObserveAdjustsRefill(t *testing.T) {
	rl := NewRateLimiter(nil, RateLimitConfig{
		Capacity: 100, RefillPerSec: 180, MinRefillPerSec: 10,
	})

	h := http.Header{}
	h.Set("X-RateLimit-Limit", "3600")
	h.Set("X-RateLimit-Remaining", "60")
	h.Set("X-RateLimit-Reset", "30") // 60 reqs over 30s → 2 req/s
	rl.Observe(h)
	if got := rl.refill(); got != 2 {
		t.Fatalf("refill after observe = %v, want 2", got)
	}
	m := rl.Metrics()
	if m.ObservedLimit != 3600 || m.ObservedRemain != 60 {
		t.Fatalf("observed headers not recorded: %+v", m)
	}

	// Remaining 0 floors at MinRefillPerSec.
	h.Set("X-RateLimit-Remaining", "0")
	h.Set("X-RateLimit-Reset", "0")
	rl.Observe(h)
	if got := rl.refill(); got != 10 {
		t.Fatalf("refill after exhaustion = %v, want MinRefillPerSec 10", got)
	}

	// A generous window is clamped to the configured steady-state rate.
	h.Set("X-RateLimit-Remaining", "100000")
	h.Set("X-RateLimit-Reset", "1")
	rl.Observe(h)
	if got := rl.refill(); got != 180 {
		t.Fatalf("refill should clamp to configured 180, got %v", got)
	}
}

func TestDefaultPriorityClassifier(t *testing.T) {
	post, _ := http.NewRequest(http.MethodPost, "https://horizon.example.org/transactions", nil)
	if got := DefaultPriorityClassifier(post); got != PriorityHigh {
		t.Fatalf("POST /transactions classified %v, want high", got)
	}
	get, _ := http.NewRequest(http.MethodGet, "https://horizon.example.org/accounts/GABC", nil)
	if got := DefaultPriorityClassifier(get); got != PriorityNormal {
		t.Fatalf("GET /accounts classified %v, want normal", got)
	}
}

func TestRateLimitTransport_ThrottlesAndObserves(t *testing.T) {
	rl, _ := newTestLimiter(t, RateLimitConfig{
		Capacity: 2, RefillPerSec: 1, HighReserveFraction: 0.2, LowCeilingFraction: 0.6,
		MaxWait: 50 * time.Millisecond,
	})
	freezeClock(rl)

	stub := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		h := http.Header{}
		h.Set("X-RateLimit-Limit", "100")
		h.Set("X-RateLimit-Remaining", "50")
		h.Set("X-RateLimit-Reset", "25")
		return &http.Response{StatusCode: 200, Header: h, Body: http.NoBody}, nil
	})
	rt := NewRateLimitTransport(stub, rl, nil)

	req, _ := http.NewRequest(http.MethodGet, "https://horizon.example.org/ledgers", nil)
	if _, err := rt.RoundTrip(req); err != nil {
		t.Fatalf("first request should pass: %v", err)
	}
	// Headers observed → refill retargeted to 50/25 = 2.
	if got := rl.refill(); got != 2 {
		t.Fatalf("refill after transport observe = %v, want 2", got)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
