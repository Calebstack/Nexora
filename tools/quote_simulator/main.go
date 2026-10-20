// Package main is a historical quote replay audit utility (issue #80).
// Full tick ingestion, variance reports and quarter-wide validation are not
// covered here - this is just the single-execution fairness check.
package main

import "github.com/shopspring/decimal"

// withinFairSpread reports whether an executed rate fell within bps of the
// historical fair-market mid rate at execution time.
func withinFairSpread(executedRate, fairMidRate decimal.Decimal, toleranceBps int) bool {
	diff := executedRate.Sub(fairMidRate).Abs()
	maxDiff := fairMidRate.Mul(decimal.NewFromInt(int64(toleranceBps))).Div(decimal.NewFromInt(10000))
	return diff.LessThanOrEqual(maxDiff)
}

func main() {}
