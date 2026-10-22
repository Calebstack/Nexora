package stellar

import (
	"context"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// Priority ranks an outgoing Horizon request. Settlement broadcasts are
// submitted at PriorityHigh so they draw on the full rate budget, while
// best-effort reads run at PriorityLow and are throttled first when the bucket
// fills. The distinct ceilings per priority are what let urgent broadcasts
// through while non-urgent traffic backs off.
type Priority int

const (
	// PriorityLow is for best-effort, retryable reads (polling, backfills).
	PriorityLow Priority = iota
	// PriorityNormal is the default for interactive reads.
	PriorityNormal
	// PriorityHigh is for settlement broadcasts and other calls that must not
	// be starved; they may consume the entire rate budget.
	PriorityHigh
)

func (p Priority) String() string {
	switch p {
	case PriorityHigh:
		return "high"
	case PriorityLow:
		return "low"
	default:
		return "normal"
	}
}

// RateLimitConfig configures the distributed leaky bucket.
type RateLimitConfig struct {
	// Key namespaces the shared bucket in Redis. All API and worker instances
	// that share a Key share one global rate budget.
	Key string
	// Capacity is the bucket size: the maximum burst, in requests, that can be
	// admitted instantaneously.
	Capacity float64
	// RefillPerSec is the sustained leak rate in requests/second — the rate the
	// bucket drains, i.e. the steady-state request throughput allowed.
	RefillPerSec float64
	// HighReserveFraction is the top slice of capacity reserved for
	// PriorityHigh only. PriorityNormal is admitted up to
	// Capacity*(1-HighReserveFraction); PriorityHigh up to full Capacity.
	HighReserveFraction float64
	// LowCeilingFraction caps PriorityLow admission at Capacity*LowCeilingFraction,
	// so bulk reads yield headroom to interactive and settlement traffic.
	LowCeilingFraction float64
	// MaxWait bounds how long Wait blocks before giving up with a context
	// deadline; zero means wait until the caller's context is done.
	MaxWait time.Duration
	// MinRefillPerSec floors the dynamically adjusted leak rate so a pessimistic
	// Horizon header can never throttle traffic to a standstill.
	MinRefillPerSec float64
	// TTL expires the Redis bucket key after idleness so stale buckets are
	// reclaimed; it is refreshed on every access.
	TTL time.Duration
}

// DefaultRateLimitConfig returns a conservative global budget. Horizon's public
// limit is on the order of a few hundred requests/second per IP; defaults leave
// headroom and reserve 20% of burst for settlement broadcasts.
func DefaultRateLimitConfig() RateLimitConfig {
	return RateLimitConfig{
		Key:                 "stellar:horizon:ratelimit",
		Capacity:            200,
		RefillPerSec:        180,
		HighReserveFraction: 0.20,
		LowCeilingFraction:  0.60,
		MaxWait:             10 * time.Second,
		MinRefillPerSec:     10,
		TTL:                 2 * time.Minute,
	}
}

// leakyBucketScript implements a leaky bucket as a meter. It leaks the level by
// the elapsed time since the last access, then admits cost only if the
// resulting level stays at or below the per-priority ceiling. It always
// persists the leaked level and timestamp so concurrent callers across
// instances converge on a single shared level.
//
// KEYS[1] = bucket key
// ARGV: now_ms, leak_per_ms, cost, ceiling, ttl_ms
// returns {admitted(0|1), wait_ms, level_after}
var leakyBucketScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local leak = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local ceiling = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local state = redis.call('HMGET', KEYS[1], 'level', 'ts')
local level = tonumber(state[1]) or 0
local ts = tonumber(state[2]) or now

local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
level = level - (elapsed * leak)
if level < 0 then level = 0 end

if level + cost <= ceiling then
  level = level + cost
  redis.call('HSET', KEYS[1], 'level', level, 'ts', now)
  redis.call('PEXPIRE', KEYS[1], ttl)
  return {1, 0, tostring(level)}
end

local wait = 0
if leak > 0 then
  wait = math.ceil(((level + cost) - ceiling) / leak)
end
redis.call('HSET', KEYS[1], 'level', level, 'ts', now)
redis.call('PEXPIRE', KEYS[1], ttl)
return {0, wait, tostring(level)}
`)

// RateLimiter is a distributed leaky-bucket limiter backed by Redis. A single
// bucket is shared by every process that uses the same key, so the global
// Horizon request rate is bounded cluster-wide, not per instance. Horizon's
// X-RateLimit-* response headers feed Observe to tighten the rate dynamically.
type RateLimiter struct {
	rdb redis.UniversalClient
	cfg RateLimitConfig

	// effectiveRefill is the leak rate currently in force (requests/sec),
	// stored as float64 bits so Observe can adjust it lock-free.
	effectiveRefill atomic.Uint64
	// effectiveCapacity mirrors effectiveRefill for burst size.
	effectiveCapacity atomic.Uint64

	admitted  atomic.Int64
	throttled atomic.Int64
	waitedMs  atomic.Int64
	redisErr  atomic.Int64

	obsLimit     atomic.Int64
	obsRemaining atomic.Int64

	// now supplies the current time fed to the Lua script as the leak
	// reference. It is a field so tests can drive the bucket's leak
	// deterministically; production uses time.Now.
	now func() time.Time
}

// NewRateLimiter builds a limiter. A nil Redis client yields a limiter that
// always admits (useful in tests and single-node dev where throttling is off).
func NewRateLimiter(rdb redis.UniversalClient, cfg RateLimitConfig) *RateLimiter {
	d := DefaultRateLimitConfig()
	if strings.TrimSpace(cfg.Key) == "" {
		cfg.Key = d.Key
	}
	if cfg.Capacity <= 0 {
		cfg.Capacity = d.Capacity
	}
	if cfg.RefillPerSec <= 0 {
		cfg.RefillPerSec = d.RefillPerSec
	}
	if cfg.HighReserveFraction <= 0 || cfg.HighReserveFraction >= 1 {
		cfg.HighReserveFraction = d.HighReserveFraction
	}
	if cfg.LowCeilingFraction <= 0 || cfg.LowCeilingFraction > 1 {
		cfg.LowCeilingFraction = d.LowCeilingFraction
	}
	if cfg.MinRefillPerSec <= 0 {
		cfg.MinRefillPerSec = d.MinRefillPerSec
	}
	if cfg.TTL <= 0 {
		cfg.TTL = d.TTL
	}
	rl := &RateLimiter{rdb: rdb, cfg: cfg, now: time.Now}
	rl.setRefill(cfg.RefillPerSec)
	rl.setCapacity(cfg.Capacity)
	return rl
}

func (rl *RateLimiter) setRefill(v float64)  { rl.effectiveRefill.Store(math.Float64bits(v)) }
func (rl *RateLimiter) refill() float64       { return math.Float64frombits(rl.effectiveRefill.Load()) }
func (rl *RateLimiter) setCapacity(v float64) { rl.effectiveCapacity.Store(math.Float64bits(v)) }
func (rl *RateLimiter) capacity() float64 {
	return math.Float64frombits(rl.effectiveCapacity.Load())
}

// ceilingFor returns the maximum bucket level at which a request of the given
// priority is admitted.
func (rl *RateLimiter) ceilingFor(p Priority) float64 {
	capacity := rl.capacity()
	switch p {
	case PriorityHigh:
		return capacity
	case PriorityLow:
		return capacity * rl.cfg.LowCeilingFraction
	default:
		return capacity * (1 - rl.cfg.HighReserveFraction)
	}
}

// TryAcquire attempts to admit one request at the given priority without
// blocking. It returns admitted=true when the request may proceed, otherwise
// the suggested wait before retrying. On a Redis error it fails open (admits)
// so a Redis blip never halts settlement.
func (rl *RateLimiter) TryAcquire(ctx context.Context, p Priority) (admitted bool, wait time.Duration, err error) {
	if rl == nil || rl.rdb == nil {
		return true, 0, nil
	}
	leakPerMs := rl.refill() / 1000.0
	res, rerr := leakyBucketScript.Run(ctx, rl.rdb,
		[]string{rl.cfg.Key},
		rl.now().UnixMilli(),
		leakPerMs,
		1, // cost
		rl.ceilingFor(p),
		rl.cfg.TTL.Milliseconds(),
	).Slice()
	if rerr != nil {
		rl.redisErr.Add(1)
		log.Warn().Err(rerr).Msg("stellar ratelimit: redis unavailable, failing open")
		return true, 0, nil
	}
	ok, _ := res[0].(int64)
	waitMs, _ := res[1].(int64)
	if ok == 1 {
		rl.admitted.Add(1)
		return true, 0, nil
	}
	rl.throttled.Add(1)
	return false, time.Duration(waitMs) * time.Millisecond, nil
}

// Wait blocks until a request at the given priority is admitted, the caller's
// context is done, or MaxWait elapses. It is the primary entry point: the
// bucket shapes the outgoing call rate so Nexora stays under Horizon's
// threshold rather than reacting to 429s after the fact.
func (rl *RateLimiter) Wait(ctx context.Context, p Priority) error {
	if rl == nil || rl.rdb == nil {
		return nil
	}
	if rl.cfg.MaxWait > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, rl.cfg.MaxWait)
		defer cancel()
	}
	start := time.Now()
	for {
		admitted, wait, err := rl.TryAcquire(ctx, p)
		if err != nil {
			return err
		}
		if admitted {
			if waited := time.Since(start); waited > 0 {
				rl.waitedMs.Add(waited.Milliseconds())
			}
			return nil
		}
		// Cap a single sleep so a large wait estimate still rechecks shared
		// state (another instance may have drained the bucket) promptly.
		if wait <= 0 {
			wait = 5 * time.Millisecond
		}
		if wait > 250*time.Millisecond {
			wait = 250 * time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Observe reads Horizon's X-RateLimit-* headers from a response and adjusts the
// effective leak rate. Horizon reports X-RateLimit-Limit, -Remaining and -Reset
// (seconds until the window resets). The limiter re-targets its rate so the
// remaining budget is spent evenly across the reset window, flooring at
// MinRefillPerSec and never exceeding the configured steady-state rate.
func (rl *RateLimiter) Observe(h http.Header) {
	if rl == nil || h == nil {
		return
	}
	limit, hasLimit := parseIntHeader(h, "X-RateLimit-Limit")
	remaining, hasRem := parseIntHeader(h, "X-RateLimit-Remaining")
	reset, hasReset := parseIntHeader(h, "X-RateLimit-Reset")
	if hasLimit {
		rl.obsLimit.Store(limit)
	}
	if hasRem {
		rl.obsRemaining.Store(remaining)
	}
	if !hasRem || !hasReset {
		return
	}

	target := rl.cfg.RefillPerSec
	if reset > 0 {
		// Spend what's left smoothly over the remaining window.
		target = float64(remaining) / float64(reset)
	} else if remaining == 0 {
		target = rl.cfg.MinRefillPerSec
	}
	if target > rl.cfg.RefillPerSec {
		target = rl.cfg.RefillPerSec
	}
	if target < rl.cfg.MinRefillPerSec {
		target = rl.cfg.MinRefillPerSec
	}
	rl.setRefill(target)
}

// Metrics is a snapshot of limiter counters for observability.
type RateLimitMetrics struct {
	Admitted       int64
	Throttled      int64
	WaitedMs       int64
	RedisErrors    int64
	RefillPerSec   float64
	ObservedLimit  int64
	ObservedRemain int64
}

// Metrics returns a snapshot of the limiter's counters.
func (rl *RateLimiter) Metrics() RateLimitMetrics {
	return RateLimitMetrics{
		Admitted:       rl.admitted.Load(),
		Throttled:      rl.throttled.Load(),
		WaitedMs:       rl.waitedMs.Load(),
		RedisErrors:    rl.redisErr.Load(),
		RefillPerSec:   rl.refill(),
		ObservedLimit:  rl.obsLimit.Load(),
		ObservedRemain: rl.obsRemaining.Load(),
	}
}

func parseIntHeader(h http.Header, key string) (int64, bool) {
	v := strings.TrimSpace(h.Get(key))
	if v == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// PriorityClassifier decides the Priority of an outgoing Horizon request. The
// default classifies transaction submissions (POST .../transactions) as
// PriorityHigh and everything else as PriorityNormal.
type PriorityClassifier func(*http.Request) Priority

// DefaultPriorityClassifier prioritises settlement broadcasts.
func DefaultPriorityClassifier(r *http.Request) Priority {
	if r.Method == http.MethodPost && strings.HasSuffix(strings.TrimRight(r.URL.Path, "/"), "/transactions") {
		return PriorityHigh
	}
	return PriorityNormal
}

// rateLimitTransport throttles every Horizon HTTP request through the shared
// leaky bucket before it is sent, and feeds each response's rate-limit headers
// back into the limiter. Wrapping the transport means all Horizon traffic —
// every typed SDK call — is governed uniformly without changing call sites.
type rateLimitTransport struct {
	base     http.RoundTripper
	limiter  *RateLimiter
	classify PriorityClassifier
}

// NewRateLimitTransport wraps base so requests are throttled and responses are
// observed. A nil base uses http.DefaultTransport; a nil classify uses
// DefaultPriorityClassifier.
func NewRateLimitTransport(base http.RoundTripper, limiter *RateLimiter, classify PriorityClassifier) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	if classify == nil {
		classify = DefaultPriorityClassifier
	}
	return &rateLimitTransport{base: base, limiter: limiter, classify: classify}
}

func (t *rateLimitTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := t.limiter.Wait(req.Context(), t.classify(req)); err != nil {
		return nil, err
	}
	resp, err := t.base.RoundTrip(req)
	if resp != nil {
		t.limiter.Observe(resp.Header)
	}
	return resp, err
}
