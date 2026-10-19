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
