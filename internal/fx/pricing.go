package fx

import (
	"math"

	"github.com/shopspring/decimal"
)

// priceWithVolatility applies the base rate-lock spread after the volatility
// engine has widened it. Rounding up prevents a fractional basis point from
// reducing the intended liquidity protection.
func priceWithVolatility(mid decimal.Decimal, baseSpreadBps int, multiplier float64) (decimal.Decimal, int) {
	if baseSpreadBps <= 0 {
		return mid, 0
	}
	adjustedBps := int(math.Ceil(float64(baseSpreadBps) * multiplier))
	factor := decimal.NewFromInt(int64(adjustedBps)).Div(decimal.NewFromInt(10000))
	return mid.Mul(decimal.NewFromInt(1).Add(factor)), adjustedBps
}

// RateLockPricing represents the spread breakdown and final rate for a rate lock.
type RateLockPricing struct {
	BaseRate          decimal.Decimal `json:"base_rate"`
	BaseSpreadBps     int             `json:"base_spread_bps"`
	Multiplier        float64         `json:"multiplier"`
	AdjustedSpreadBps int             `json:"adjusted_spread_bps"`
	FinalRate         decimal.Decimal `json:"final_rate"`
}

// CalculateDynamicRateLockSpread computes the dynamic spread adjustment for rate locks
// based on the volatility multiplier.
// Time complexity: O(1). Space complexity: O(1).
func CalculateDynamicRateLockSpread(baseRate decimal.Decimal, baseSpreadBps int, multiplier float64) RateLockPricing {
	finalRate, adjustedBps := priceWithVolatility(baseRate, baseSpreadBps, multiplier)
	return RateLockPricing{
		BaseRate:          baseRate,
		BaseSpreadBps:     baseSpreadBps,
		Multiplier:        multiplier,
		AdjustedSpreadBps: adjustedBps,
		FinalRate:         finalRate,
	}
}
