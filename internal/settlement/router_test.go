package settlement

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/fx"
	"github.com/shopspring/decimal"
)

// --- fakes ---

type fakeLegExecutor struct {
	// failVenues causes Execute to fail for the named venues.
	failVenues map[fx.Venue]bool
	calls      atomic.Int64
}

func (f *fakeLegExecutor) Execute(_ context.Context, _, _ string, _ decimal.Decimal, venue fx.Venue) (string, error) {
	f.calls.Add(1)
	if f.failVenues[venue] {
		return "", errors.New("venue unavailable")
	}
	return "txhash_" + string(venue), nil
}

type fakeLegRollback struct {
	calls atomic.Int64
}

func (f *fakeLegRollback) Rollback(_ context.Context, _ string, _, _ string, _ decimal.Decimal, _ fx.Venue) error {
	f.calls.Add(1)
	return nil
}

func dec(s string) decimal.Decimal {
	v, _ := decimal.NewFromString(s)
	return v
}

func testDecision() *fx.RouteDecision {
	return &fx.RouteDecision{
		FromAsset:   "USDC",
		ToAsset:     "EURC",
		TotalAmount: dec("10000"),
		Legs: []fx.RouteLeg{
			{Venue: fx.VenueDEX, Fraction: dec("0.6"), Amount: dec("6000"), Rate: dec("1.06")},
			{Venue: fx.VenueAnchor, Fraction: dec("0.4"), Amount: dec("4000"), Rate: dec("1.04")},
		},
		BlendedRate:   dec("1.052"),
		BenchmarkRate: dec("1.04"),
		DecidedAt:     time.Now().UTC(),
	}
}

// --- tests ---

func TestSettlementRouter_AllLegsSucceed(t *testing.T) {
	exec := &fakeLegExecutor{}
	router := NewSettlementRouter(exec, nil, DefaultSettlementRouterConfig())
	decision := testDecision()

	res, err := router.Execute(context.Background(), decision)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Succeeded != 2 {
		t.Fatalf("succeeded = %d, want 2", res.Succeeded)
	}
	if res.Failed != 0 {
		t.Fatalf("failed = %d, want 0", res.Failed)
	}
	if !res.TotalSent.Equal(dec("10000")) {
		t.Fatalf("total sent = %s, want 10000", res.TotalSent)
	}
	for _, lr := range res.Legs {
		if lr.Status != LegConfirmed {
			t.Fatalf("leg %s status = %s, want confirmed", lr.Leg.Venue, lr.Status)
		}
		if lr.TxHash == "" {
			t.Fatal("confirmed leg has empty tx hash")
		}
	}

	m := router.Metrics()
	if m.Executed != 1 || m.Succeeded != 1 {
		t.Fatalf("metrics: executed=%d succeeded=%d", m.Executed, m.Succeeded)
	}
}

func TestSettlementRouter_AllLegsFail(t *testing.T) {
	exec := &fakeLegExecutor{failVenues: map[fx.Venue]bool{fx.VenueDEX: true, fx.VenueAnchor: true}}
	router := NewSettlementRouter(exec, nil, DefaultSettlementRouterConfig())

	res, err := router.Execute(context.Background(), testDecision())
	if !errors.Is(err, ErrAllLegsFailed) {
		t.Fatalf("expected ErrAllLegsFailed, got %v", err)
	}
	if res.Succeeded != 0 {
		t.Fatalf("succeeded = %d, want 0", res.Succeeded)
	}
	if res.Failed != 2 {
		t.Fatalf("failed = %d, want 2", res.Failed)
	}
	if router.Metrics().AllFailed != 1 {
		t.Fatal("all-failed counter not incremented")
	}
}

func TestSettlementRouter_PartialFill(t *testing.T) {
	exec := &fakeLegExecutor{failVenues: map[fx.Venue]bool{fx.VenueAnchor: true}}
	router := NewSettlementRouter(exec, nil, SettlementRouterConfig{
		MaxConcurrentLegs: 4,
		LegTimeout:        5 * time.Second,
		RollbackOnPartial: false,
	})

	res, err := router.Execute(context.Background(), testDecision())
	if !errors.Is(err, ErrPartialFill) {
		t.Fatalf("expected ErrPartialFill, got %v", err)
	}
	if res.Succeeded != 1 {
		t.Fatalf("succeeded = %d, want 1", res.Succeeded)
	}
	if res.Failed != 1 {
		t.Fatalf("failed = %d, want 1", res.Failed)
	}
	if !res.TotalSent.Equal(dec("6000")) {
		t.Fatalf("total sent = %s, want 6000", res.TotalSent)
	}
	if router.Metrics().PartialFills != 1 {
		t.Fatal("partial-fills counter not incremented")
	}
}

func TestSettlementRouter_PartialFillWithRollback(t *testing.T) {
	exec := &fakeLegExecutor{failVenues: map[fx.Venue]bool{fx.VenueAnchor: true}}
	rb := &fakeLegRollback{}
	router := NewSettlementRouter(exec, rb, SettlementRouterConfig{
		MaxConcurrentLegs: 4,
		LegTimeout:        5 * time.Second,
		RollbackOnPartial: true,
	})

	res, err := router.Execute(context.Background(), testDecision())
	if !errors.Is(err, ErrPartialFill) {
		t.Fatalf("expected ErrPartialFill, got %v", err)
	}

	// Rollback should have been attempted for the succeeded leg.
	if rb.calls.Load() != 1 {
		t.Fatalf("rollback calls = %d, want 1", rb.calls.Load())
	}

	// The succeeded leg should now be marked rolled back.
	var rolledBack int
	for _, lr := range res.Legs {
		if lr.Status == LegRolledBack {
			rolledBack++
		}
	}
	if rolledBack != 1 {
		t.Fatalf("rolled back legs = %d, want 1", rolledBack)
	}
	if router.Metrics().RollbacksRun != 1 {
		t.Fatal("rollbacks-run counter not incremented")
	}
}

func TestSettlementRouter_EmptyDecision(t *testing.T) {
	exec := &fakeLegExecutor{}
	router := NewSettlementRouter(exec, nil, DefaultSettlementRouterConfig())

	_, err := router.Execute(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for nil decision")
	}

	_, err = router.Execute(context.Background(), &fx.RouteDecision{})
	if err == nil {
		t.Fatal("expected error for empty legs")
	}
}

func TestSettlementRouter_ConcurrencyBound(t *testing.T) {
	// Create a decision with many legs and verify they execute concurrently
	// up to the configured bound.
	var maxConcurrent atomic.Int64
	var current atomic.Int64
	exec := &fakeLegExecutor{}
	origExec := exec

	// Replace with a tracking executor.
	tracker := &trackingExecutor{
		inner:         origExec,
		current:       &current,
		maxConcurrent: &maxConcurrent,
	}

	router := NewSettlementRouter(tracker, nil, SettlementRouterConfig{
		MaxConcurrentLegs: 2,
		LegTimeout:        5 * time.Second,
	})

	decision := &fx.RouteDecision{
		FromAsset:   "USDC",
		ToAsset:     "EURC",
		TotalAmount: dec("4000"),
		Legs: []fx.RouteLeg{
			{Venue: fx.VenueDEX, Amount: dec("1000"), Rate: dec("1.05")},
			{Venue: fx.VenueAnchor, Amount: dec("1000"), Rate: dec("1.04")},
			{Venue: "venue3", Amount: dec("1000"), Rate: dec("1.03")},
			{Venue: "venue4", Amount: dec("1000"), Rate: dec("1.02")},
		},
		DecidedAt: time.Now().UTC(),
	}

	res, err := router.Execute(context.Background(), decision)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Succeeded != 4 {
		t.Fatalf("succeeded = %d, want 4", res.Succeeded)
	}

	// maxConcurrent should not exceed the configured limit of 2.
	if maxConcurrent.Load() > 2 {
		t.Fatalf("max concurrent = %d, exceeds limit of 2", maxConcurrent.Load())
	}
}

type trackingExecutor struct {
	inner         LegExecutor
	current       *atomic.Int64
	maxConcurrent *atomic.Int64
}

func (t *trackingExecutor) Execute(ctx context.Context, from, to string, amount decimal.Decimal, venue fx.Venue) (string, error) {
	cur := t.current.Add(1)
	for {
		old := t.maxConcurrent.Load()
		if cur <= old || t.maxConcurrent.CompareAndSwap(old, cur) {
			break
		}
	}
	defer t.current.Add(-1)

	// Simulate brief work so concurrency is observable.
	time.Sleep(5 * time.Millisecond)
	return t.inner.Execute(ctx, from, to, amount, venue)
}

func TestSettlementRouter_ContextCancellation(t *testing.T) {
	exec := &slowExecutor{delay: 500 * time.Millisecond}
	router := NewSettlementRouter(exec, nil, SettlementRouterConfig{
		MaxConcurrentLegs: 4,
		LegTimeout:        2 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	res, err := router.Execute(ctx, testDecision())
	// Legs should have failed due to context cancellation.
	if err == nil {
		t.Fatal("expected error due to context cancellation")
	}
	if res.Succeeded != 0 {
		t.Fatalf("succeeded = %d, want 0 with cancelled context", res.Succeeded)
	}
}

type slowExecutor struct {
	delay time.Duration
}

func (s *slowExecutor) Execute(ctx context.Context, _, _ string, _ decimal.Decimal, _ fx.Venue) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(s.delay):
		return "txhash_slow", nil
	}
}

func TestLegStatus_String(t *testing.T) {
	tests := []struct {
		s    LegStatus
		want string
	}{
		{LegPending, "pending"},
		{LegSubmitted, "submitted"},
		{LegConfirmed, "confirmed"},
		{LegFailed, "failed"},
		{LegRolledBack, "rolled_back"},
	}
	for _, tc := range tests {
		if got := tc.s.String(); got != tc.want {
			t.Errorf("LegStatus(%d).String() = %q, want %q", tc.s, got, tc.want)
		}
	}
}
