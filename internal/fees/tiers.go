package fees

import "github.com/shopspring/decimal"

// Tier is a 30-day-rolling-volume pricing bracket (issue #77).
type Tier struct {
	Name         string
	MinVolumeUSD decimal.Decimal
	DiscountBps  int
}

var volumeTiers = []Tier{
	{"Platinum", decimal.NewFromInt(1_000_000), 50},
	{"Gold", decimal.NewFromInt(250_000), 30},
	{"Silver", decimal.NewFromInt(50_000), 15},
	{"Standard", decimal.Zero, 0},
}

// TierFor returns the highest bracket the given rolling 30-day USD
// conversion volume qualifies for.
func TierFor(rolling30dVolumeUSD decimal.Decimal) Tier {
	for _, t := range volumeTiers {
		if rolling30dVolumeUSD.GreaterThanOrEqual(t.MinVolumeUSD) {
			return t
		}
	}
	return volumeTiers[len(volumeTiers)-1]
}
