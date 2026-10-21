package fx

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/shopspring/decimal"
)

type mockWebhookDispatcher struct {
	mu        sync.Mutex
	dispatched []RateAlertWebhookPayload
}

func (m *mockWebhookDispatcher) Dispatch(ctx context.Context, eventType string, payload interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p, ok := payload.(RateAlertWebhookPayload); ok {
		m.dispatched = append(m.dispatched, p)
	}
	return nil
}

func TestIntervalTree_InsertDeleteQuery(t *testing.T) {
	tree := NewIntervalTree()

	alerts := []*domain.RateAlert{
		{ID: "alert-1", TargetRate: decimal.RequireFromString("1.0500"), Direction: domain.AlertDirectionAtOrAbove},
		{ID: "alert-2", TargetRate: decimal.RequireFromString("1.1000"), Direction: domain.AlertDirectionAtOrAbove},
		{ID: "alert-3", TargetRate: decimal.RequireFromString("0.9500"), Direction: domain.AlertDirectionAtOrBelow},
		{ID: "alert-4", TargetRate: decimal.RequireFromString("1.2000"), Direction: domain.AlertDirectionAtOrAbove},
		{ID: "alert-5", TargetRate: decimal.RequireFromString("0.9000"), Direction: domain.AlertDirectionAtOrBelow},
	}

	for _, a := range alerts {
		tree.Insert(a)
	}

	if tree.Size() != 5 {
		t.Fatalf("tree size = %d, want 5", tree.Size())
	}

	// Query range [0.9200, 1.1500] should match alerts 1, 2, 3 (1.05, 1.10, 0.95)
	results := tree.QueryRange(decimal.RequireFromString("0.9200"), decimal.RequireFromString("1.1500"))
	if len(results) != 3 {
		t.Fatalf("query results count = %d, want 3", len(results))
	}

	// Delete alert-2
	deleted := tree.Delete("alert-2", decimal.RequireFromString("1.1000"))
	if !deleted {
		t.Fatalf("expected alert-2 to be deleted")
	}
	if tree.Size() != 4 {
		t.Fatalf("tree size after delete = %d, want 4", tree.Size())
	}

	// Re-query range [0.9200, 1.1500] should now match 2 alerts
	results = tree.QueryRange(decimal.RequireFromString("0.9200"), decimal.RequireFromString("1.1500"))
	if len(results) != 2 {
		t.Fatalf("query results count = %d, want 2", len(results))
	}
}

func TestAlertEvaluator_FiresExactlyOncePerCrossing(t *testing.T) {
	dispatcher := &mockWebhookDispatcher{}
	evaluator := NewAlertEvaluator(nil, nil, dispatcher)

	alert := &domain.RateAlert{
		ID:         "alert-crossing-1",
		TenantID:   "tenant-123",
		FromAsset:  "USDC",
		ToAsset:    "XLM",
		TargetRate: decimal.RequireFromString("1.1000"),
		Direction:  domain.AlertDirectionAtOrAbove,
		Active:     true,
	}

	ctx := context.Background()
	if err := evaluator.AddOrUpdateAlert(ctx, alert, false); err != nil {
		t.Fatal(err)
	}

	// 1. Initial price below target (1.0800): should not fire
	fired, err := evaluator.EvaluatePriceTick(ctx, "USDC:XLM", decimal.RequireFromString("1.0800"))
	if err != nil {
		t.Fatal(err)
	}
	if len(fired) != 0 {
		t.Fatalf("expected 0 fired alerts below threshold, got %d", len(fired))
	}

	// 2. Price crosses above target (1.1200): should fire once
	fired, err = evaluator.EvaluatePriceTick(ctx, "USDC:XLM", decimal.RequireFromString("1.1200"))
	if err != nil {
		t.Fatal(err)
	}
	if len(fired) != 1 {
		t.Fatalf("expected 1 fired alert on crossing, got %d", len(fired))
	}

	// 3. Price stays above target (1.1400): should NOT fire again
	fired, err = evaluator.EvaluatePriceTick(ctx, "USDC:XLM", decimal.RequireFromString("1.1400"))
	if err != nil {
		t.Fatal(err)
	}
	if len(fired) != 0 {
		t.Fatalf("expected 0 fired alerts when staying above target, got %d", len(fired))
	}

	// Verify dispatcher received exactly 1 webhook
	dispatcher.mu.Lock()
	count := len(dispatcher.dispatched)
	dispatcher.mu.Unlock()
	if count != 1 {
		t.Fatalf("dispatched webhook count = %d, want 1", count)
	}
}

func TestAlertEvaluator_AtomicCASRace(t *testing.T) {
	dispatcher := &mockWebhookDispatcher{}
	evaluator := NewAlertEvaluator(nil, nil, dispatcher)

	alert := &domain.RateAlert{
		ID:         "alert-race-1",
		TenantID:   "tenant-abc",
		FromAsset:  "EURC",
		ToAsset:    "USDC",
		TargetRate: decimal.RequireFromString("1.0500"),
		Direction:  domain.AlertDirectionAtOrAbove,
		Active:     true,
	}

	ctx := context.Background()
	_ = evaluator.AddOrUpdateAlert(ctx, alert, false)

	// Simulate concurrent evaluations of the same crossing tick across 10 goroutines
	var wg sync.WaitGroup
	var firedCount atomic.Int32

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fired, err := evaluator.EvaluatePriceTick(ctx, "EURC:USDC", decimal.RequireFromString("1.0600"))
			if err == nil && len(fired) > 0 {
				firedCount.Add(int32(len(fired)))
			}
		}()
	}
	wg.Wait()

	// Exactly-once firing invariant
	if firedCount.Load() != 1 {
		t.Fatalf("expected exactly 1 firing across concurrent racing workers, got %d", firedCount.Load())
	}
}

func BenchmarkAlertEvaluator_Evaluate50000Alerts(b *testing.B) {
	evaluator := NewAlertEvaluator(nil, nil, nil)
	ctx := context.Background()
	pair := "USDC:XLM"

	basePrice := 1.0000
	// Pre-populate 50,000 active concurrent rate alerts
	for i := 0; i < 50_000; i++ {
		// Distribute target rates between 0.5000 and 2.0000
		targetRateFloat := 0.5000 + (float64(i) * 0.00003)
		direction := domain.AlertDirectionAtOrAbove
		if i%2 == 0 {
			direction = domain.AlertDirectionAtOrBelow
		}

		alert := &domain.RateAlert{
			ID:         fmt.Sprintf("alert-%d", i),
			TenantID:   fmt.Sprintf("tenant-%d", i%100),
			FromAsset:  "USDC",
			ToAsset:    "XLM",
			TargetRate: decimal.NewFromFloat(targetRateFloat),
			Direction:  direction,
			Active:     true,
		}
		_ = evaluator.AddOrUpdateAlert(ctx, alert, false)
	}

	// Prime the previous price
	_, _ = evaluator.EvaluatePriceTick(ctx, pair, decimal.NewFromFloat(basePrice))

	tickPrice := decimal.RequireFromString("1.0005")

	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		_, err := evaluator.EvaluatePriceTick(ctx, pair, tickPrice)
		if err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	if b.N > 0 {
		perOp := elapsed / time.Duration(b.N)
		if perOp > time.Millisecond {
			b.Errorf("evaluation latency %v exceeds 1 millisecond limit for 50,000 alerts", perOp)
		}
	}
}
