package fx

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestVolatilityEngine_FlashCrashWidensRateLockSpread(t *testing.T) {
	engine := NewVolatilityEngine()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// A calm historical sequence should keep a normal spread tight.
	for i := 0; i < 120; i++ {
		price := 100.0
		if i%2 == 0 {
			price = 100.001
		}
		engine.Observe("BTC:USDC", price, start.Add(time.Duration(i)*time.Minute))
	}
	calm := engine.Observe("BTC:USDC", 100, start.Add(120*time.Minute))
	calmMultiplier := engine.SpreadMultiplier("BTC:USDC", calm)
	_, calmBps := priceWithVolatility(decimal.NewFromInt(100), 20, calmMultiplier)
	if calmBps != 20 {
		t.Fatalf("calm spread = %d bps, want 20", calmBps)
	}

	// Historical flash-crash sample: a 25% move in one minute. The short
	// horizon must react immediately and the lock spread must widen.
	crash := engine.Observe("BTC:USDC", 75, start.Add(121*time.Minute))
	crashMultiplier := engine.SpreadMultiplier("BTC:USDC", crash)
	_, crashBps := priceWithVolatility(decimal.NewFromInt(100), 20, crashMultiplier)
	if crash.OneHour <= calm.OneHour {
		t.Fatalf("1h volatility did not rise: calm=%f crash=%f", calm.OneHour, crash.OneHour)
	}
	if crashMultiplier <= 1 || crashBps <= calmBps {
		t.Fatalf("flash crash must widen spread: multiplier=%f bps=%d", crashMultiplier, crashBps)
	}
}

func TestVolatilityEngine_IgnoresOutOfOrderObservations(t *testing.T) {
	engine := NewVolatilityEngine()
	now := time.Now().UTC()
	engine.Observe("USDC:XLM", 2, now)
	engine.Observe("USDC:XLM", 1, now.Add(time.Minute))
	before := engine.states["USDC:XLM"]
	engine.Observe("USDC:XLM", 999, now)
	after := engine.states["USDC:XLM"]
	if !after.at.Equal(before.at) || after.price != before.price {
		t.Fatal("out-of-order observation changed the stored market state")
	}
}

func TestVolatilityEngine_AuditLogAndMultiHorizonFlashCrash(t *testing.T) {
	engine := NewVolatilityEngine()
	start := time.Date(2026, 3, 12, 0, 0, 0, 0, time.UTC)

	// Simulate 7 days of steady calm trading around $10,000
	tCurrent := start
	for day := 0; day < 7; day++ {
		for hour := 0; hour < 24; hour++ {
			price := 10000.0 + float64((hour%3)-1)*0.5
			engine.Observe("BTC:USD", price, tCurrent)
			tCurrent = tCurrent.Add(time.Hour)
		}
	}

	snapCalm := engine.Observe("BTC:USD", 10000.0, tCurrent)
	multCalm := engine.SpreadMultiplier("BTC:USD", snapCalm)
	if multCalm > 1.05 {
		t.Fatalf("expected tight calm multiplier close to 1.0, got %f", multCalm)
	}

	// Simulate market flash crash: price drops from $10,000 to $5,000 in 15 minutes
	crashSteps := []float64{9000, 8000, 6500, 5000}
	var snapCrash VolatilitySnapshot
	for _, crashPrice := range crashSteps {
		tCurrent = tCurrent.Add(3 * time.Minute)
		snapCrash = engine.Observe("BTC:USD", crashPrice, tCurrent)
	}

	multCrash := engine.SpreadMultiplier("BTC:USD", snapCrash)
	if multCrash <= multCalm {
		t.Fatalf("crash multiplier %f must exceed calm multiplier %f", multCrash, multCalm)
	}

	// Verify all 3 horizons are populated and short horizon reacts strongly
	if snapCrash.OneHour <= 0 || snapCrash.OneDay <= 0 || snapCrash.SevenDays <= 0 {
		t.Fatalf("expected all horizons to be positive: 1h=%f, 24h=%f, 7d=%f",
			snapCrash.OneHour, snapCrash.OneDay, snapCrash.SevenDays)
	}
	if snapCrash.OneHour < snapCrash.SevenDays {
		t.Fatalf("immediate flash crash must impact 1h (%f) more than 7d (%f)",
			snapCrash.OneHour, snapCrash.SevenDays)
	}

	// Verify audit logs record the adjustment for reporting
	auditLogs := engine.GetAuditLog("BTC:USD")
	if len(auditLogs) == 0 {
		t.Fatalf("expected audit log entries to be recorded")
	}
	lastEntry := auditLogs[len(auditLogs)-1]
	if lastEntry.Pair != "BTC:USD" || lastEntry.NewMultiplier != multCrash {
		t.Fatalf("audit entry mismatch: got %+v", lastEntry)
	}

	// Verify pricing calculation with widened spread
	pricing := CalculateDynamicRateLockSpread(decimal.NewFromInt(5000), 30, multCrash)
	if pricing.AdjustedSpreadBps <= 30 {
		t.Fatalf("expected adjusted spread > 30 bps, got %d", pricing.AdjustedSpreadBps)
	}
	if !pricing.FinalRate.GreaterThan(decimal.NewFromInt(5000)) {
		t.Fatalf("expected final rate to include spread protection, got %s", pricing.FinalRate)
	}
}
