package fx

import (
	"errors"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/shopspring/decimal"
)

func TestValidateSlippageTolerance(t *testing.T) {
	tests := []struct {
		name    string
		bps     int
		pct     decimal.Decimal
		wantErr bool
	}{
		{"min bound 0.05% (5 bps)", 5, decimal.NewFromFloat(0.05), false},
		{"mid bound 1.0% (100 bps)", 100, decimal.NewFromFloat(1.0), false},
		{"max bound 5.0% (500 bps)", 500, decimal.NewFromFloat(5.0), false},
		{"below min bound 4 bps", 4, decimal.NewFromFloat(0.04), true},
		{"above max bound 501 bps", 501, decimal.NewFromFloat(5.01), true},
		{"zero bps", 0, decimal.Zero, true},
		{"negative bps", -10, decimal.NewFromInt(-1), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errBps := ValidateSlippageToleranceBps(tt.bps)
			if (errBps != nil) != tt.wantErr {
				t.Errorf("ValidateSlippageToleranceBps(%d) error = %v, wantErr %v", tt.bps, errBps, tt.wantErr)
			}
			errPct := ValidateSlippageTolerancePct(tt.pct)
			if (errPct != nil) != tt.wantErr {
				t.Errorf("ValidateSlippageTolerancePct(%s) error = %v, wantErr %v", tt.pct.String(), errPct, tt.wantErr)
			}
		})
	}
}

func sampleOrderbook() []OrderbookLevel {
	return []OrderbookLevel{
		{Price: decimal.RequireFromString("1.0000"), Amount: decimal.RequireFromString("1000")},
		{Price: decimal.RequireFromString("1.0050"), Amount: decimal.RequireFromString("2000")},
		{Price: decimal.RequireFromString("1.0100"), Amount: decimal.RequireFromString("3000")},
		{Price: decimal.RequireFromString("1.0200"), Amount: decimal.RequireFromString("4000")},
	}
}

func TestCalculatePriceImpact(t *testing.T) {
	levels := sampleOrderbook()

	// Fill level 0 only: 500 units @ 1.0000. Price impact should be 0.
	eff, top, impactPct, impactBps, err := CalculatePriceImpact(decimal.RequireFromString("500"), levels)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !eff.Equal(top) {
		t.Errorf("effective %s != top %s", eff, top)
	}
	if impactBps != 0 || !impactPct.IsZero() {
		t.Errorf("impact %d bps, want 0", impactBps)
	}

	// Fill 1000 @ 1.0000 and 1000 @ 1.0050 = total cost 1000 + 1005 = 2005.
	// Average price = 2005 / 2000 = 1.0025.
	// Price impact = (1.0025 - 1.0000) / 1.0000 = 0.0025 = 0.25% = 25 bps.
	eff, top, impactPct, impactBps, err = CalculatePriceImpact(decimal.RequireFromString("2000"), levels)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expectedEff := decimal.RequireFromString("1.0025")
	if !eff.Equal(expectedEff) {
		t.Errorf("effective %s != expected %s", eff, expectedEff)
	}
	if impactBps != 25 {
		t.Errorf("impact bps = %d, want 25", impactBps)
	}
	if !impactPct.Equal(decimal.RequireFromString("0.25")) {
		t.Errorf("impact pct = %s, want 0.25", impactPct)
	}
}

func TestSlippageGuard_RejectsExcessImpact(t *testing.T) {
	guard := NewSlippageGuard()
	levels := sampleOrderbook()

	// 2000 units yields 25 bps impact.
	// Tenant allows 10 bps (0.10%). Should reject.
	userTolerance := 10
	calc, err := guard.Check(decimal.RequireFromString("2000"), &userTolerance, levels)
	if err == nil {
		t.Fatalf("expected slippage rejection error, got nil")
	}

	var slipErr *SlippageExceededError
	if !errors.As(err, &slipErr) {
		t.Fatalf("expected *SlippageExceededError, got %T: %v", err, err)
	}
	if !errors.Is(err, domain.ErrSlippageExceeded) {
		t.Fatalf("expected errors.Is(err, domain.ErrSlippageExceeded) to be true")
	}

	if calc.Allowed {
		t.Errorf("calc.Allowed should be false")
	}
	if calc.PriceImpactBps != 25 {
		t.Errorf("calc.PriceImpactBps = %d, want 25", calc.PriceImpactBps)
	}
	if calc.ToleranceBps != 10 {
		t.Errorf("calc.ToleranceBps = %d, want 10", calc.ToleranceBps)
	}
	if calc.Tier != "Retail" {
		t.Errorf("calc.Tier = %s, want Retail", calc.Tier)
	}

	// Message should contain calculation details.
	msg := err.Error()
	if len(msg) == 0 {
		t.Errorf("error message should not be empty")
	}
}

func TestSlippageGuard_AllowsWithinTolerance(t *testing.T) {
	guard := NewSlippageGuard()
	levels := sampleOrderbook()

	// 2000 units yields 25 bps impact.
	// Tenant allows 30 bps (0.30%). Should allow.
	userTolerance := 30
	calc, err := guard.Check(decimal.RequireFromString("2000"), &userTolerance, levels)
	if err != nil {
		t.Fatalf("expected allowed, got error: %v", err)
	}
	if !calc.Allowed {
		t.Errorf("calc.Allowed should be true")
	}
}

func TestSlippageGuard_InsufficientLiquidity(t *testing.T) {
	guard := NewSlippageGuard()
	levels := sampleOrderbook() // total depth is 10,000 units

	_, err := guard.Check(decimal.RequireFromString("50000"), nil, levels)
	if !errors.Is(err, ErrInsufficientLiquidityDepth) {
		t.Fatalf("expected ErrInsufficientLiquidityDepth, got %v", err)
	}
}

func BenchmarkCalculatePriceImpact(b *testing.B) {
	// 50 realistic levels
	levels := make([]OrderbookLevel, 50)
	price := decimal.RequireFromString("1.0000")
	for i := range levels {
		levels[i] = OrderbookLevel{
			Price:  price.Add(decimal.NewFromFloat(float64(i) * 0.001)),
			Amount: decimal.NewFromInt(1000),
		}
	}
	tradeAmount := decimal.RequireFromString("15000")

	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		_, _, _, _, err := CalculatePriceImpact(tradeAmount, levels)
		if err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	if b.N > 0 {
		perOp := elapsed / time.Duration(b.N)
		if perOp > time.Millisecond {
			b.Errorf("calculation latency %v exceeds 1 millisecond limit", perOp)
		}
	}
}

func BenchmarkSlippageGuard_Check(b *testing.B) {
	guard := NewSlippageGuard()
	levels := make([]OrderbookLevel, 100)
	price := decimal.RequireFromString("1.0000")
	for i := range levels {
		levels[i] = OrderbookLevel{
			Price:  price.Add(decimal.NewFromFloat(float64(i) * 0.0005)),
			Amount: decimal.NewFromInt(500),
		}
	}
	tradeSize := decimal.RequireFromString("20000")
	tolerance := 100

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = guard.Check(tradeSize, &tolerance, levels)
	}
}
