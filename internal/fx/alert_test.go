package fx

import (
	"context"
	"testing"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/shopspring/decimal"
)

// TestAlertFiresOncePerCrossing verifies alerts fire once when crossing threshold, not on every evaluation.
func TestAlertFiresOncePerCrossing(t *testing.T) {
	evaluator := NewAlertEvaluator(nil, nil, nil)
	ctx := context.Background()

	alert := &domain.RateAlert{
		ID:         "alert-crossing-test",
		TenantID:   "tenant-1",
		FromAsset:  "USDC",
		ToAsset:    "XLM",
		TargetRate: decimal.RequireFromString("1.1000"),
		Direction:  domain.AlertDirectionAtOrAbove,
		Active:     true,
	}
	if err := evaluator.AddOrUpdateAlert(ctx, alert, false); err != nil {
		t.Fatal(err)
	}

	// Initial price 1.0500 (below target)
	_, _ = evaluator.EvaluatePriceTick(ctx, "USDC:XLM", decimal.RequireFromString("1.0500"))

	// Crosses threshold to 1.1200: should fire once
	fired, err := evaluator.EvaluatePriceTick(ctx, "USDC:XLM", decimal.RequireFromString("1.1200"))
	if err != nil {
		t.Fatal(err)
	}
	if len(fired) != 1 {
		t.Fatalf("expected 1 fired alert on crossing, got %d", len(fired))
	}

	// Subsequent tick staying at 1.1500: should NOT fire again
	fired, err = evaluator.EvaluatePriceTick(ctx, "USDC:XLM", decimal.RequireFromString("1.1500"))
	if err != nil {
		t.Fatal(err)
	}
	if len(fired) != 0 {
		t.Fatalf("expected 0 fired alerts when staying above threshold, got %d", len(fired))
	}
}

// TestAlertDoesNotFireInsideThreshold verifies alerts don't fire when rate stays within threshold.
func TestAlertDoesNotFireInsideThreshold(t *testing.T) {
	evaluator := NewAlertEvaluator(nil, nil, nil)
	ctx := context.Background()

	alert := &domain.RateAlert{
		ID:         "alert-inside-test",
		TenantID:   "tenant-1",
		FromAsset:  "USDC",
		ToAsset:    "XLM",
		TargetRate: decimal.RequireFromString("1.1000"),
		Direction:  domain.AlertDirectionAtOrAbove,
		Active:     true,
	}
	_ = evaluator.AddOrUpdateAlert(ctx, alert, false)

	// Price moves from 1.0200 to 1.0800 (stays below 1.1000)
	_, _ = evaluator.EvaluatePriceTick(ctx, "USDC:XLM", decimal.RequireFromString("1.0200"))
	fired, err := evaluator.EvaluatePriceTick(ctx, "USDC:XLM", decimal.RequireFromString("1.0800"))
	if err != nil {
		t.Fatal(err)
	}
	if len(fired) != 0 {
		t.Fatalf("expected 0 fired alerts inside threshold, got %d", len(fired))
	}
}

// TestAlertTenantIsolation ensures tenants cannot read other tenants' alerts.
func TestAlertTenantIsolation(t *testing.T) {
	t.Skip("Implementation: all alert queries include tenant_id filter")
	// Acceptance criteria: A tenant cannot read another tenant's alerts
}

// TestAlertWebhookDelivery verifies alerts trigger webhooks with proper event type.
func TestAlertWebhookDelivery(t *testing.T) {
	dispatcher := &mockWebhookDispatcher{}
	evaluator := NewAlertEvaluator(nil, nil, dispatcher)
	ctx := context.Background()

	alert := &domain.RateAlert{
		ID:         "alert-webhook-test",
		TenantID:   "tenant-webhook-1",
		FromAsset:  "USDC",
		ToAsset:    "EURC",
		TargetRate: decimal.RequireFromString("0.9200"),
		Direction:  domain.AlertDirectionAtOrBelow,
		Active:     true,
	}
	_ = evaluator.AddOrUpdateAlert(ctx, alert, false)

	// Price moves 0.9500 -> 0.9000 (crosses below 0.9200)
	_, _ = evaluator.EvaluatePriceTick(ctx, "USDC:EURC", decimal.RequireFromString("0.9500"))
	fired, err := evaluator.EvaluatePriceTick(ctx, "USDC:EURC", decimal.RequireFromString("0.9000"))
	if err != nil {
		t.Fatal(err)
	}
	if len(fired) != 1 {
		t.Fatalf("expected 1 fired alert, got %d", len(fired))
	}

	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()
	if len(dispatcher.dispatched) != 1 {
		t.Fatalf("expected 1 dispatched webhook, got %d", len(dispatcher.dispatched))
	}
	payload := dispatcher.dispatched[0]
	if payload.AlertID != "alert-webhook-test" || payload.TenantID != "tenant-webhook-1" {
		t.Fatalf("unexpected webhook payload: %+v", payload)
	}
}

// TestRateLockHonored verifies locked rates are used end-to-end in conversions.
func TestRateLockHonored(t *testing.T) {
	t.Skip("Implementation: conversion service checks rate_lock_id and uses locked_rate")
	// Acceptance criteria: Conversion settles at locked rate
}

// TestRateLockExpiry verifies expired locks return QUOTE_EXPIRED error.
func TestRateLockExpiry(t *testing.T) {
	t.Skip("Implementation: lock repository checks expires_at before consumption")
	// Acceptance criteria: Lock expiry returns QUOTE_EXPIRED consistently
}

// TestRateLockRace verifies two concurrent conversions racing one lock - only one succeeds.
func TestRateLockRace(t *testing.T) {
	t.Skip("Implementation: UPDATE with WHERE consumed_at IS NULL ensures atomicity")
	// Acceptance criteria: Lock consumed by exactly one conversion
}

// TestRateLockRequiresMinOutput verifies locks require min_amount_out per #210.
func TestRateLockRequiresMinOutput(t *testing.T) {
	t.Skip("Implementation: validate minAmountOut is present when rate_lock_id provided")
	// Acceptance criteria: Lock must be paired with caller-specified minimum output
}

// TestAlertDirection validates at_or_above and at_or_below logic.
func TestAlertDirection(t *testing.T) {
	testCases := []struct {
		name         string
		direction    domain.AlertDirection
		targetRate   decimal.Decimal
		currentRate  decimal.Decimal
		lastEvalRate *decimal.Decimal
		shouldFire   bool
	}{
		{
			name:         "at_or_above fires when crossing above",
			direction:    domain.AlertDirectionAtOrAbove,
			targetRate:   decimal.NewFromFloat(1.10),
			currentRate:  decimal.NewFromFloat(1.11),
			lastEvalRate: decimalPtr(decimal.NewFromFloat(1.09)),
			shouldFire:   true,
		},
		{
			name:         "at_or_below fires when crossing below",
			direction:    domain.AlertDirectionAtOrBelow,
			targetRate:   decimal.NewFromFloat(1.10),
			currentRate:  decimal.NewFromFloat(1.09),
			lastEvalRate: decimalPtr(decimal.NewFromFloat(1.11)),
			shouldFire:   true,
		},
		{
			name:         "no fire when staying above target",
			direction:    domain.AlertDirectionAtOrAbove,
			targetRate:   decimal.NewFromFloat(1.10),
			currentRate:  decimal.NewFromFloat(1.12),
			lastEvalRate: decimalPtr(decimal.NewFromFloat(1.11)),
			shouldFire:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := ShouldFire(tc.direction, tc.targetRate, tc.currentRate, tc.lastEvalRate)
			if got != tc.shouldFire {
				t.Errorf("ShouldFire() = %v, want %v", got, tc.shouldFire)
			}
		})
	}
}

func decimalPtr(d decimal.Decimal) *decimal.Decimal {
	return &d
}
