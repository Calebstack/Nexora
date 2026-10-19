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
