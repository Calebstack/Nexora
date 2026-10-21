package fx

import (
	"math"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const secondsPerYear = 365.25 * 24 * 60 * 60

// VolatilitySnapshot contains annualized EWMA volatility for the horizons
// used when pricing a rate lock. Values are fractions: 0.25 is 25%.
type VolatilitySnapshot struct {
	OneHour   float64 `json:"one_hour"`
	OneDay    float64 `json:"one_day"`
	SevenDays float64 `json:"seven_days"`
}

func (s VolatilitySnapshot) maximum() float64 {
	return math.Max(s.OneHour, math.Max(s.OneDay, s.SevenDays))
}

type volatilityState struct {
	price     float64
	at        time.Time
	variances [3]float64 // EWMA variance per second, shortest window first.
}

// VolatilityAuditEntry records an adjustment to the spread multiplier for financial
// reporting and regulatory audit transparency.
type VolatilityAuditEntry struct {
	Pair                 string             `json:"pair"`
	Timestamp            time.Time          `json:"timestamp"`
	AnnualizedVolatility float64            `json:"annualized_volatility"`
	PreviousMultiplier   float64            `json:"previous_multiplier"`
	NewMultiplier        float64            `json:"new_multiplier"`
	Snapshot             VolatilitySnapshot `json:"snapshot"`
}

// VolatilityEngine estimates volatility from log returns. It keeps only a
// constant amount of state per pair, so it is suitable for long-running quote
// streams as well as on-demand pricing.
type VolatilityEngine struct {
	mu          sync.Mutex
	states      map[string]volatilityState
	multipliers map[string]float64
	auditLogs   map[string][]VolatilityAuditEntry
}

func NewVolatilityEngine() *VolatilityEngine {
	return &VolatilityEngine{
		states:      make(map[string]volatilityState),
		multipliers: make(map[string]float64),
		auditLogs:   make(map[string][]VolatilityAuditEntry),
	}
}

var volatilityWindows = [...]time.Duration{time.Hour, 24 * time.Hour, 7 * 24 * time.Hour}

// Observe records a mid-market price and returns the updated annualized EWMA
// estimate. Out-of-order or non-positive observations are safely ignored.
func (e *VolatilityEngine) Observe(pair string, price float64, at time.Time) VolatilitySnapshot {
	if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
		return VolatilitySnapshot{}
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	previous, ok := e.states[pair]
	if !ok {
		e.states[pair] = volatilityState{price: price, at: at}
		return VolatilitySnapshot{}
	}
	if !at.After(previous.at) {
		return snapshot(previous.variances)
	}

	dt := at.Sub(previous.at).Seconds()
	if previous.price > 0 && dt > 0 {
		returnPerSecond := math.Pow(math.Log(price/previous.price), 2) / dt
		for i, window := range volatilityWindows {
			alpha := math.Exp(-dt / window.Seconds())
			previous.variances[i] = alpha*previous.variances[i] + (1-alpha)*returnPerSecond
		}
	}
	previous.price, previous.at = price, at
	e.states[pair] = previous
	return snapshot(previous.variances)
}

func snapshot(variances [3]float64) VolatilitySnapshot {
	annualized := func(variance float64) float64 {
		if variance <= 0 {
			return 0
		}
		return math.Sqrt(variance * secondsPerYear)
	}
	return VolatilitySnapshot{
		OneHour:   annualized(variances[0]),
		OneDay:    annualized(variances[1]),
		SevenDays: annualized(variances[2]),
	}
}

// SpreadMultiplier translates turbulence into a bounded liquidity-protection
// factor. Markets at or below 15% annualized volatility retain the base
// spread; above that threshold the factor grows linearly to a fivefold cap.
func (e *VolatilityEngine) SpreadMultiplier(pair string, volatility VolatilitySnapshot) float64 {
	const calmAnnualized = 0.15
	const maximum = 5.0

	multiplier := 1.0 + math.Max(0, (volatility.maximum()-calmAnnualized)/calmAnnualized)
	if multiplier > maximum {
		multiplier = maximum
	}

	e.mu.Lock()
	previous := e.multipliers[pair]
	e.multipliers[pair] = multiplier

	// Record audit entry for financial reporting whenever factor is established or adjusted.
	if previous == 0 || math.Abs(previous-multiplier) > 0.01 {
		entry := VolatilityAuditEntry{
			Pair:                 pair,
			Timestamp:            time.Now().UTC(),
			AnnualizedVolatility: volatility.maximum(),
			PreviousMultiplier:   previous,
			NewMultiplier:        multiplier,
			Snapshot:             volatility,
		}
		e.auditLogs[pair] = append(e.auditLogs[pair], entry)
	}
	e.mu.Unlock()

	if previous != 0 && math.Abs(previous-multiplier) > 0.01 {
		log.Info().Str("pair", pair).
			Float64("previous_multiplier", previous).
			Float64("spread_multiplier", multiplier).
			Float64("annualized_volatility", volatility.maximum()).
			Msg("fx rate-lock volatility factor adjusted")
	}
	return multiplier
}

// GetAuditLog retrieves the recorded multiplier adjustments for audit and financial reporting.
func (e *VolatilityEngine) GetAuditLog(pair string) []VolatilityAuditEntry {
	e.mu.Lock()
	defer e.mu.Unlock()
	logs := e.auditLogs[pair]
	result := make([]VolatilityAuditEntry, len(logs))
	copy(result, logs)
	return result
}
