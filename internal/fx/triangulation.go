package fx

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

// ErrTriangulationDiscrepancy is returned when a direct quote diverges from synthetic cross-rates.
var ErrTriangulationDiscrepancy = errors.New("triangular pricing anomaly: discrepancy exceeds circuit breaker threshold")

// DefaultMaxTriangulationDiscrepancy is 1.2% (0.012).
const DefaultMaxTriangulationDiscrepancy = 0.012

type PairRate struct {
	From, To string
	Rate     float64
}

type TriangulationResult struct {
	Direct, Implied, Discrepancy float64
	Halt                         bool
	Intermediate                 string
}

// TriangulationAlert is emitted when potential oracle divergence or stale market data is detected.
type TriangulationAlert struct {
	DirectPair     string    `json:"direct_pair"`
	Intermediate   string    `json:"intermediate"`
	DirectRate     float64   `json:"direct_rate"`
	ImpliedRate    float64   `json:"implied_rate"`
	DiscrepancyPct float64   `json:"discrepancy_pct"`
	ThresholdPct   float64   `json:"threshold_pct"`
	Message        string    `json:"message"`
	Timestamp      time.Time `json:"timestamp"`
}

// CheckTriangulation performs a single triangular path consistency check.
func CheckTriangulation(direct PairRate, first, second PairRate, maxDiscrepancy float64) (TriangulationResult, error) {
	if direct.Rate <= 0 || first.Rate <= 0 || second.Rate <= 0 {
		return TriangulationResult{}, fmt.Errorf("rates must be positive")
	}
	implied := first.Rate * second.Rate
	discrepancy := math.Abs(direct.Rate-implied) / implied
	return TriangulationResult{
		Direct:       direct.Rate,
		Implied:      implied,
		Discrepancy:  discrepancy,
		Halt:         discrepancy > maxDiscrepancy,
		Intermediate: first.To,
	}, nil
}

// TriangulationDetector monitors cross-rates and halts quote issuance on anomalies.
type TriangulationDetector struct {
	mu             sync.RWMutex
	maxDiscrepancy float64
	intermediates  []string
	alertHandler   func(alert TriangulationAlert)
	alerts         []TriangulationAlert
}

// NewTriangulationDetector creates an arbitrage and cross-pair pricing fairness validator.
func NewTriangulationDetector(maxDiscrepancy float64, alertHandler func(TriangulationAlert)) *TriangulationDetector {
	if maxDiscrepancy <= 0 {
		maxDiscrepancy = DefaultMaxTriangulationDiscrepancy
	}
	return &TriangulationDetector{
		maxDiscrepancy: maxDiscrepancy,
		intermediates:  []string{"USD", "EUR", "XLM", "USDC", "EURC"},
		alertHandler:   alertHandler,
	}
}

// SetIntermediates configures bridge currencies for triangular synthetic calculation.
func (d *TriangulationDetector) SetIntermediates(assets []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.intermediates = append([]string(nil), assets...)
}

// GetRecentAlerts returns historical alerts emitted by the detector.
func (d *TriangulationDetector) GetRecentAlerts() []TriangulationAlert {
	d.mu.RLock()
	defer d.mu.RUnlock()
	res := make([]TriangulationAlert, len(d.alerts))
	copy(res, d.alerts)
	return res
}

func pairKey(from, to string) string {
	return from + "-" + to
}

// getDirectOrInvertedRate resolves rate for from->to either directly or by inverting to->from.
func getDirectOrInvertedRate(from, to string, rates map[string]float64) (float64, bool) {
	if from == to {
		return 1.0, true
	}
	if r, ok := rates[pairKey(from, to)]; ok && r > 0 {
		return r, true
	}
	if r, ok := rates[from+":"+to]; ok && r > 0 {
		return r, true
	}
	// Try inverse
	if r, ok := rates[pairKey(to, from)]; ok && r > 0 {
		return 1.0 / r, true
	}
	if r, ok := rates[to+":"+from]; ok && r > 0 {
		return 1.0 / r, true
	}
	return 0, false
}

// ComputeImpliedRate calculates the synthetic exchange rate from -> intermediate -> to.
func (d *TriangulationDetector) ComputeImpliedRate(from, intermediate, to string, rates map[string]float64) (float64, bool) {
	if from == intermediate || intermediate == to || from == to {
		return 0, false
	}
	r1, ok1 := getDirectOrInvertedRate(from, intermediate, rates)
	if !ok1 {
		return 0, false
	}
	r2, ok2 := getDirectOrInvertedRate(intermediate, to, rates)
	if !ok2 {
		return 0, false
	}
	return r1 * r2, true
}

// ComputeAllSyntheticRates computes implied synthetic exchange rates across all supported pairs.
func (d *TriangulationDetector) ComputeAllSyntheticRates(supportedPairs []string, rates map[string]float64) map[string]map[string]float64 {
	d.mu.RLock()
	intermediates := append([]string(nil), d.intermediates...)
	d.mu.RUnlock()

	result := make(map[string]map[string]float64)

	for _, pair := range supportedPairs {
		parts := strings.Split(pair, "-")
		if len(parts) != 2 {
			continue
		}
		from, to := parts[0], parts[1]
		result[pair] = make(map[string]float64)
		for _, inter := range intermediates {
			if implied, ok := d.ComputeImpliedRate(from, inter, to, rates); ok {
				result[pair][inter] = implied
			}
		}
	}

	return result
}

// ValidateQuote checks direct quote fairness against triangular paths.
// Halts quote issuance if triangular discrepancy exceeds maximum threshold.
func (d *TriangulationDetector) ValidateQuote(from, to string, directRate float64, rates map[string]float64) (TriangulationResult, error) {
	if directRate <= 0 {
		return TriangulationResult{}, fmt.Errorf("direct rate must be positive: %f", directRate)
	}

	d.mu.RLock()
	maxDiscrepancy := d.maxDiscrepancy
	intermediates := append([]string(nil), d.intermediates...)
	handler := d.alertHandler
	d.mu.RUnlock()

	var worstResult TriangulationResult
	foundPath := false

	for _, inter := range intermediates {
		if inter == from || inter == to {
			continue
		}
		implied, ok := d.ComputeImpliedRate(from, inter, to, rates)
		if !ok || implied <= 0 {
			continue
		}

		discrepancy := math.Abs(directRate-implied) / implied
		res := TriangulationResult{
			Direct:       directRate,
			Implied:      implied,
			Discrepancy:  discrepancy,
			Halt:         discrepancy > maxDiscrepancy,
			Intermediate: inter,
		}

		if !foundPath || discrepancy > worstResult.Discrepancy {
			worstResult = res
			foundPath = true
		}
	}

	if !foundPath {
		// No triangular path available to check; allow quote
		return TriangulationResult{Direct: directRate, Implied: directRate, Discrepancy: 0, Halt: false}, nil
	}

	if worstResult.Halt {
		alert := TriangulationAlert{
			DirectPair:     pairKey(from, to),
			Intermediate:   worstResult.Intermediate,
			DirectRate:     directRate,
			ImpliedRate:    worstResult.Implied,
			DiscrepancyPct: worstResult.Discrepancy * 100,
			ThresholdPct:   maxDiscrepancy * 100,
			Message:        fmt.Sprintf("potential oracle divergence or stale market data: direct quote %s (%.6f) diverges by %.2f%% from triangular path via %s (implied %.6f)", pairKey(from, to), directRate, worstResult.Discrepancy*100, worstResult.Intermediate, worstResult.Implied),
			Timestamp:      time.Now().UTC(),
		}

		d.mu.Lock()
		d.alerts = append(d.alerts, alert)
		d.mu.Unlock()

		if handler != nil {
			handler(alert)
		}

		return worstResult, fmt.Errorf("%w: %s", ErrTriangulationDiscrepancy, alert.Message)
	}

	return worstResult, nil
}

