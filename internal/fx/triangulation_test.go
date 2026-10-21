package fx

import (
	"errors"
	"math"
	"testing"
)

func TestTriangulationNormalSpreadDoesNotHalt(t *testing.T) {
	result, err := CheckTriangulation(PairRate{"USD", "XLM", 10.1}, PairRate{"USD", "EUR", 1}, PairRate{"EUR", "XLM", 10}, .012)
	if err != nil || result.Halt {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestTriangulationDetector_NormalMarketSpreadsNoFalsePositives(t *testing.T) {
	var alertCount int
	detector := NewTriangulationDetector(0.012, func(a TriangulationAlert) {
		alertCount++
	})

	// Normal market spread: USD/EUR = 0.92, EUR/XLM = 5.43 -> Implied USD/XLM = 0.92 * 5.43 = 4.9956
	// Direct USD/XLM = 5.01 (discrepancy ~0.288%, well below 1.2% threshold)
	rates := map[string]float64{
		"USD-EUR": 0.92,
		"EUR-XLM": 5.43,
	}

	result, err := detector.ValidateQuote("USD", "XLM", 5.01, rates)
	if err != nil {
		t.Fatalf("unexpected error under normal market spread: %v", err)
	}
	if result.Halt {
		t.Fatalf("circuit breaker tripped unexpectedly under normal market spread: discrepancy=%.4f", result.Discrepancy)
	}
	if alertCount > 0 {
		t.Fatalf("expected no alerts under normal spread, got %d", alertCount)
	}
}

func TestTriangulationDetector_HaltsQuoteIssuanceOnDiscrepancyAboveThreshold(t *testing.T) {
	var capturedAlert *TriangulationAlert
	detector := NewTriangulationDetector(0.012, func(a TriangulationAlert) {
		capturedAlert = &a
	})

	// USD/EUR = 0.92, EUR/XLM = 5.00 -> Implied USD/XLM = 4.60
	// Direct USD/XLM = 4.75 -> discrepancy = |4.75 - 4.60| / 4.60 = 0.0326 (3.26% > 1.2%)
	rates := map[string]float64{
		"USD-EUR": 0.92,
		"EUR-XLM": 5.00,
	}

	result, err := detector.ValidateQuote("USD", "XLM", 4.75, rates)
	if err == nil {
		t.Fatal("expected quote issuance to halt with error due to 3.26% discrepancy")
	}
	if !errors.Is(err, ErrTriangulationDiscrepancy) {
		t.Fatalf("expected ErrTriangulationDiscrepancy, got: %v", err)
	}
	if !result.Halt {
		t.Fatal("expected result.Halt = true")
	}

	// Verify alert was emitted
	if capturedAlert == nil {
		t.Fatal("expected alert to be emitted to handler")
	}
	if capturedAlert.DirectPair != "USD-XLM" {
		t.Fatalf("alert DirectPair = %s, want USD-XLM", capturedAlert.DirectPair)
	}
	if capturedAlert.Intermediate != "EUR" {
		t.Fatalf("alert Intermediate = %s, want EUR", capturedAlert.Intermediate)
	}
	if capturedAlert.DiscrepancyPct < 3.0 {
		t.Fatalf("alert DiscrepancyPct = %f, want > 3.0", capturedAlert.DiscrepancyPct)
	}
}

func TestTriangulationDetector_ComputeAllSyntheticRates(t *testing.T) {
	detector := NewTriangulationDetector(0.012, nil)

	rates := map[string]float64{
		"USD-EUR": 0.92,
		"EUR-XLM": 5.00,
		"USD-USDC": 1.00,
		"USDC-XLM": 4.60,
	}

	supportedPairs := []string{"USD-XLM", "XLM-USD"}
	synthetics := detector.ComputeAllSyntheticRates(supportedPairs, rates)

	usdxlm, ok := synthetics["USD-XLM"]
	if !ok {
		t.Fatal("expected USD-XLM in synthetic rates")
	}

	eurImplied, ok := usdxlm["EUR"]
	if !ok || math.Abs(eurImplied-4.60) > 0.0001 {
		t.Fatalf("implied via EUR = %f, want 4.60", eurImplied)
	}

	usdcImplied, ok := usdxlm["USDC"]
	if !ok || math.Abs(usdcImplied-4.60) > 0.0001 {
		t.Fatalf("implied via USDC = %f, want 4.60", usdcImplied)
	}
}

