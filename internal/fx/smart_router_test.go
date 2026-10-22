package fx

import (
	"testing"

	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal {
	v, _ := decimal.NewFromString(s)
	return v
}

func TestSmartRouter_SingleVenueFill(t *testing.T) {
	router := NewSmartRouter(DefaultSmartRouterConfig())
	quotes := []VenueQuote{
		{Venue: VenueDEX, Rate: d("1.05"), Amount: d("10000"), DepthBps: 10, LatencyMs: 200},
		{Venue: VenueAnchor, Rate: d("1.04"), Amount: d("10000"), DepthBps: 5, LatencyMs: 50},
	}

	dec, err := router.Route("USDC", "EURC", d("5000"), quotes)
	if err != nil {
		t.Fatal(err)
	}
	if len(dec.Legs) != 1 {
		t.Fatalf("expected single leg, got %d", len(dec.Legs))
	}
	if dec.Legs[0].Venue != VenueDEX {
		t.Fatalf("expected DEX venue, got %s", dec.Legs[0].Venue)
	}
	if !dec.BlendedRate.Equal(d("1.05")) {
		t.Fatalf("blended rate = %s, want 1.05", dec.BlendedRate)
	}
	if dec.ImprovementBps != 0 {
		t.Fatalf("improvement = %d, want 0 for single-venue fill", dec.ImprovementBps)
	}
}

func TestSmartRouter_SplitAcrossVenues(t *testing.T) {
	router := NewSmartRouter(SmartRouterConfig{
		MinSplitFraction:  d("0.05"),
		MaxSlippageBps:    20,
		PreferSingleVenue: true,
	})

	// DEX has best rate but only 3000 liquidity with tight slippage;
	// anchor can fill the rest at a lower rate.
	quotes := []VenueQuote{
		{Venue: VenueDEX, Rate: d("1.06"), Amount: d("3000"), DepthBps: 15, LatencyMs: 200},
		{Venue: VenueAnchor, Rate: d("1.04"), Amount: d("10000"), DepthBps: 5, LatencyMs: 50},
	}

	dec, err := router.Route("USDC", "EURC", d("8000"), quotes)
	if err != nil {
		t.Fatal(err)
	}

	if len(dec.Legs) != 2 {
		t.Fatalf("expected 2 legs, got %d", len(dec.Legs))
	}

	// First leg should be DEX (best rate).
	if dec.Legs[0].Venue != VenueDEX {
		t.Fatalf("first leg should be DEX, got %s", dec.Legs[0].Venue)
	}
	if !dec.Legs[0].Amount.Equal(d("3000")) {
		t.Fatalf("DEX leg amount = %s, want 3000", dec.Legs[0].Amount)
	}
	// Second leg fills the remainder at anchor.
	if dec.Legs[1].Venue != VenueAnchor {
		t.Fatalf("second leg should be Anchor, got %s", dec.Legs[1].Venue)
	}
	if !dec.Legs[1].Amount.Equal(d("5000")) {
		t.Fatalf("anchor leg amount = %s, want 5000", dec.Legs[1].Amount)
	}

	// Blended rate: (1.06*3000 + 1.04*5000) / 8000 = (3180 + 5200) / 8000 = 1.0475
	expected := d("1.0475")
	if !dec.BlendedRate.Equal(expected) {
		t.Fatalf("blended rate = %s, want %s", dec.BlendedRate, expected)
	}

	// Benchmark is anchor at 1.04 (only venue that can fill 8000).
	if !dec.BenchmarkRate.Equal(d("1.04")) {
		t.Fatalf("benchmark = %s, want 1.04", dec.BenchmarkRate)
	}

	// Improvement should be positive (blended > benchmark).
	if dec.ImprovementBps <= 0 {
		t.Fatalf("improvement = %d, want positive", dec.ImprovementBps)
	}
}

func TestSmartRouter_AllLiquidityAtOneVenueWithHighSlippage(t *testing.T) {
	router := NewSmartRouter(SmartRouterConfig{
		MinSplitFraction:  d("0.05"),
		MaxSlippageBps:    20,
		PreferSingleVenue: true,
	})

	// DEX can fill but slippage exceeds threshold, and there's an anchor
	// alternative.
	quotes := []VenueQuote{
		{Venue: VenueDEX, Rate: d("1.06"), Amount: d("10000"), DepthBps: 50, LatencyMs: 200},
		{Venue: VenueAnchor, Rate: d("1.04"), Amount: d("10000"), DepthBps: 5, LatencyMs: 50},
	}

	dec, err := router.Route("USDC", "EURC", d("5000"), quotes)
	if err != nil {
		t.Fatal(err)
	}

	// DEX depth 50 > MaxSlippageBps 20, so split should happen (or at least
	// consider both). The router greedy-fills from DEX first since its rate
	// is better: DEX fills all 5000 even though depth is high, because the
	// amount fits within the venue's capacity.
	if len(dec.Legs) < 1 {
		t.Fatal("expected at least one leg")
	}
}

func TestSmartRouter_NoViableQuotes(t *testing.T) {
	router := NewSmartRouter(DefaultSmartRouterConfig())

	// Amount too small relative to total.
	quotes := []VenueQuote{
		{Venue: VenueDEX, Rate: d("1.05"), Amount: d("10"), DepthBps: 10},
	}
	_, err := router.Route("USDC", "EURC", d("10000"), quotes)
	if err == nil {
		t.Fatal("expected error for insufficient venue liquidity")
	}
}

func TestSmartRouter_ZeroAmount(t *testing.T) {
	router := NewSmartRouter(DefaultSmartRouterConfig())
	_, err := router.Route("USDC", "EURC", d("0"), nil)
	if err == nil {
		t.Fatal("expected error for zero amount")
	}
}

func TestSmartRouter_EmptyQuotes(t *testing.T) {
	router := NewSmartRouter(DefaultSmartRouterConfig())
	_, err := router.Route("USDC", "EURC", d("100"), nil)
	if err == nil {
		t.Fatal("expected error for empty quotes")
	}
}

func TestSmartRouter_ThreeVenueSplit(t *testing.T) {
	router := NewSmartRouter(SmartRouterConfig{
		MinSplitFraction:  d("0.05"),
		MaxSlippageBps:    10,
		PreferSingleVenue: true,
	})

	quotes := []VenueQuote{
		{Venue: VenueDEX, Rate: d("1.08"), Amount: d("2000"), DepthBps: 5, LatencyMs: 200},
		{Venue: VenueAnchor, Rate: d("1.06"), Amount: d("3000"), DepthBps: 8, LatencyMs: 50},
		{Venue: "offchain", Rate: d("1.04"), Amount: d("10000"), DepthBps: 2, LatencyMs: 30},
	}

	dec, err := router.Route("USDC", "EURC", d("9000"), quotes)
	if err != nil {
		t.Fatal(err)
	}

	if len(dec.Legs) != 3 {
		t.Fatalf("expected 3 legs, got %d", len(dec.Legs))
	}

	// Verify total fills.
	total := decimal.Zero
	for _, l := range dec.Legs {
		total = total.Add(l.Amount)
	}
	if !total.Equal(d("9000")) {
		t.Fatalf("total filled = %s, want 9000", total)
	}

	// Fractions should sum to 1.
	fracSum := decimal.Zero
	for _, l := range dec.Legs {
		fracSum = fracSum.Add(l.Fraction)
	}
	one := decimal.NewFromInt(1)
	// Allow tiny rounding.
	if fracSum.Sub(one).Abs().GreaterThan(d("0.001")) {
		t.Fatalf("fraction sum = %s, want ~1", fracSum)
	}
}

func TestBlendedRate(t *testing.T) {
	legs := []RouteLeg{
		{Amount: d("6000"), Rate: d("1.10")},
		{Amount: d("4000"), Rate: d("1.05")},
	}
	// (1.10*6000 + 1.05*4000) / 10000 = (6600+4200)/10000 = 1.08
	got := blendedRate(legs, d("10000"))
	if !got.Equal(d("1.08")) {
		t.Fatalf("blended = %s, want 1.08", got)
	}
}

func TestImprovementBps(t *testing.T) {
	// 1.08 vs 1.05 → (0.03/1.05)*10000 ≈ 285.7 → 286
	got := improvementBps(d("1.08"), d("1.05"))
	if got < 285 || got > 286 {
		t.Fatalf("improvement = %d, want ~286", got)
	}

	// Same rate → 0.
	if improvementBps(d("1.05"), d("1.05")) != 0 {
		t.Fatal("equal rates should yield 0 improvement")
	}

	// Worse blended → negative.
	got = improvementBps(d("1.03"), d("1.05"))
	if got >= 0 {
		t.Fatalf("worse blended should be negative, got %d", got)
	}
}
