package fx

import (
	"errors"
	"testing"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/shopspring/decimal"
)

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func soundQuote() QuoteSnapshot {
	return QuoteSnapshot{
		FromAsset: "XLM",
		ToAsset:   "USDC",
		Bid:       dec("0.0990"),
		Ask:       dec("0.1010"),
		Mid:       dec("0.1000"),
		SpreadBps: 100,
		Provider:  "fx.HorizonProvider",
	}
}

func TestSanityChecker_AcceptsSoundQuote(t *testing.T) {
	c := NewSanityChecker(DefaultHistoricalCorridors(), nil)
	if err := c.Check(soundQuote()); err != nil {
		t.Fatalf("sound quote rejected: %v", err)
	}
	if c.IsSuspended("XLM", "USDC") {
		t.Fatal("pair suspended after a sound quote")
	}
	if len(c.GetRecentAlerts()) != 0 {
		t.Fatalf("expected no alerts, got %d", len(c.GetRecentAlerts()))
	}
}

func TestSanityChecker_DetectsNegativeSpread(t *testing.T) {
	c := NewSanityChecker(nil, nil)
	q := soundQuote()
	q.Bid = dec("0.1010") // equal to ask → zero spread, ask not strictly greater
	err := c.Check(q)
	if !errors.Is(err, domain.ErrNegativeSpread) {
		t.Fatalf("want ErrNegativeSpread, got %v", err)
	}
	if !c.IsSuspended("XLM", "USDC") {
		t.Fatal("pair not suspended after negative spread")
	}
}

func TestSanityChecker_DetectsInvertedRate(t *testing.T) {
	cases := map[string]QuoteSnapshot{
		"bid above ask": func() QuoteSnapshot { q := soundQuote(); q.Bid = dec("0.2"); q.Ask = dec("0.1"); return q }(),
		"zero ask":      func() QuoteSnapshot { q := soundQuote(); q.Ask = decimal.Zero; return q }(),
		"negative bid":  func() QuoteSnapshot { q := soundQuote(); q.Bid = dec("-0.1"); return q }(),
	}
	for name, q := range cases {
		t.Run(name, func(t *testing.T) {
			c := NewSanityChecker(nil, nil)
			if err := c.Check(q); !errors.Is(err, domain.ErrInvertedRate) {
				t.Fatalf("want ErrInvertedRate, got %v", err)
			}
			if !c.IsSuspended(q.FromAsset, q.ToAsset) {
				t.Fatal("pair not suspended after inverted rate")
			}
		})
	}
}

func TestSanityChecker_DetectsOutOfCorridor(t *testing.T) {
	c := NewSanityChecker(map[string]Corridor{
		"XLM-USDC": {Min: dec("0.01"), Max: dec("1")},
	}, nil)

	high := soundQuote()
	high.Mid = dec("50") // decimal-shift style feed error
	high.Bid = dec("49")
	high.Ask = dec("51")
	if err := c.Check(high); !errors.Is(err, domain.ErrRateOutOfCorridor) {
		t.Fatalf("want ErrRateOutOfCorridor for high mid, got %v", err)
	}

	// A fresh checker: the low-bound case must also trip.
	c2 := NewSanityChecker(map[string]Corridor{"XLM-USDC": {Min: dec("0.01"), Max: dec("1")}}, nil)
	low := soundQuote()
	low.Mid = dec("0.0001")
	low.Bid = dec("0.00009")
	low.Ask = dec("0.00011")
	if err := c2.Check(low); !errors.Is(err, domain.ErrRateOutOfCorridor) {
		t.Fatalf("want ErrRateOutOfCorridor for low mid, got %v", err)
	}
}

func TestSanityChecker_CorridorOpenEnded(t *testing.T) {
	// Min only: no upper bound.
	c := NewSanityChecker(map[string]Corridor{"XLM-USDC": {Min: dec("0.01")}}, nil)
	q := soundQuote()
	q.Mid = dec("1000000")
	q.Bid = dec("999999")
	q.Ask = dec("1000001")
	if err := c.Check(q); err != nil {
		t.Fatalf("open-ended max should accept a large mid: %v", err)
	}
}

func TestSanityChecker_SuspensionGatesAndAlerts(t *testing.T) {
	var fired []SanityAlert
	c := NewSanityChecker(nil, func(a SanityAlert) { fired = append(fired, a) })

	bad := soundQuote()
	bad.Ask = dec("0.0990") // inverted (ask < bid)
	bad.Bid = dec("0.1010")

	if err := c.Check(bad); !errors.Is(err, domain.ErrInvertedRate) {
		t.Fatalf("first check want ErrInvertedRate, got %v", err)
	}

	// A subsequent check on the same pair — even a sound one — is gated by the
	// suspension and must not re-alert.
	if err := c.Check(soundQuote()); !errors.Is(err, domain.ErrQuotingSuspended) {
		t.Fatalf("second check want ErrQuotingSuspended, got %v", err)
	}
	if len(fired) != 1 {
		t.Fatalf("expected exactly one alert, got %d", len(fired))
	}
	if fired[0].Violation != ViolationInvertedRate {
		t.Fatalf("alert violation = %q, want inverted_rate", fired[0].Violation)
	}
	if fired[0].Pair != "XLM-USDC" || fired[0].Provider != "fx.HorizonProvider" {
		t.Fatalf("alert payload missing detail: %+v", fired[0])
	}
}

func TestSanityChecker_Resume(t *testing.T) {
	c := NewSanityChecker(nil, nil)
	bad := soundQuote()
	bad.Bid = bad.Ask // zero spread
	_ = c.Check(bad)

	if !c.Resume("XLM", "USDC") {
		t.Fatal("Resume should report the pair was suspended")
	}
	if c.IsSuspended("XLM", "USDC") {
		t.Fatal("pair still suspended after Resume")
	}
	if c.Resume("XLM", "USDC") {
		t.Fatal("Resume on a non-suspended pair should report false")
	}
	// After resume a sound quote flows again.
	if err := c.Check(soundQuote()); err != nil {
		t.Fatalf("sound quote rejected after resume: %v", err)
	}
}

func TestSnapshotFromMid(t *testing.T) {
	snap := SnapshotFromMid("XLM", "USDC", dec("0.10"), 100, "p")
	if !snap.Ask.GreaterThan(snap.Bid) {
		t.Fatalf("ask %s not greater than bid %s", snap.Ask, snap.Bid)
	}
	if !snap.Mid.Equal(dec("0.10")) {
		t.Fatalf("mid = %s, want 0.10", snap.Mid)
	}
	// 100 bps either side of 0.10 → bid 0.099, ask 0.101.
	if !snap.Bid.Equal(dec("0.099")) || !snap.Ask.Equal(dec("0.101")) {
		t.Fatalf("unexpected bid/ask: %s / %s", snap.Bid, snap.Ask)
	}
	if err := NewSanityChecker(DefaultHistoricalCorridors(), nil).Check(snap); err != nil {
		t.Fatalf("derived snapshot rejected: %v", err)
	}
}
