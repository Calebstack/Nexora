package fx

import "github.com/shopspring/decimal"

// OrderbookLevel is a single bid/ask price level (issue #79).
type OrderbookLevel struct {
	Price  decimal.Decimal
	Amount decimal.Decimal
}

// DepthAt sums the cumulative amount available at or better than
// maxDepthBps away from the top-of-book price. Horizon polling, Redis
// caching and illiquidity alerts are not covered here - this is just the
// depth-curve calculation.
func DepthAt(levels []OrderbookLevel, topPrice decimal.Decimal, maxDepthBps int) decimal.Decimal {
	limit := topPrice.Mul(decimal.NewFromInt(int64(10000 - maxDepthBps))).Div(decimal.NewFromInt(10000))
	total := decimal.Zero
	for _, lvl := range levels {
		if lvl.Price.LessThan(limit) {
			break
		}
		total = total.Add(lvl.Amount)
	}
	return total
}
