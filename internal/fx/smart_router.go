package fx

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/shopspring/decimal"
)

// Venue identifies a liquidity source the smart router can route to.
type Venue string

const (
	// VenueDEX is the Stellar decentralised exchange (order book).
	VenueDEX Venue = "dex"
	// VenueAnchor is a centralized anchor / off-chain liquidity provider.
	VenueAnchor Venue = "anchor"
)

// VenueQuote is a price-depth snapshot from a single venue for one side of a
// trade.
type VenueQuote struct {
	Venue     Venue
	// Rate is the effective rate (units of ToAsset per unit of FromAsset) the
	// venue offers for Amount.
	Rate      decimal.Decimal
	// Amount is the maximum notional the venue can fill at Rate before
	// slippage exceeds tolerable bounds.
	Amount    decimal.Decimal
	// DepthBps is how many basis points deep into the book this quote reaches.
	DepthBps  int
	// LatencyMs is the expected execution latency for this venue.
	LatencyMs int
}

// RouteLeg describes one piece of a split execution: a fraction of the total
// order routed to a specific venue.
type RouteLeg struct {
	Venue      Venue
	Fraction   decimal.Decimal // 0..1
	Amount     decimal.Decimal
	Rate       decimal.Decimal
	DepthBps   int
	LatencyMs  int
}

// RouteDecision is the smart router's output: an optimal split of a trade
// across venues plus a blended benchmark comparison.
type RouteDecision struct {
	FromAsset     string
	ToAsset       string
	TotalAmount   decimal.Decimal
	Legs          []RouteLeg
	BlendedRate   decimal.Decimal
	BenchmarkRate decimal.Decimal // best single-venue rate for the full amount
	ImprovementBps int            // blended vs benchmark, positive = better
	DecidedAt     time.Time
}

// SmartRouterConfig configures the routing heuristic.
type SmartRouterConfig struct {
	// MinSplitFraction is the smallest fraction the router will assign to a
	// leg. Below this the leg is not worth the overhead of a separate
	// transaction envelope.
	MinSplitFraction decimal.Decimal
	// MaxSlippageBps is the maximum tolerable slippage for a single venue
	// before the router starts splitting.
	MaxSlippageBps int
	// PreferSingleVenue is true when the total amount can be filled at one
	// venue without exceeding MaxSlippageBps — the router skips splitting.
	PreferSingleVenue bool
}

// DefaultSmartRouterConfig returns conservative defaults.
func DefaultSmartRouterConfig() SmartRouterConfig {
	return SmartRouterConfig{
		MinSplitFraction:  decimal.NewFromFloat(0.05), // 5%
		MaxSlippageBps:    50,
		PreferSingleVenue: true,
	}
}

// SmartRouter analyses liquidity across the Stellar DEX and centralized anchor
// quotes, then decides the optimal split ratio (issue #84). It is a pure
// function: it receives venue quotes and returns a routing decision without
// side effects.
type SmartRouter struct {
	cfg SmartRouterConfig
}

// NewSmartRouter builds a router.
func NewSmartRouter(cfg SmartRouterConfig) *SmartRouter {
	if cfg.MinSplitFraction.IsZero() {
		cfg = DefaultSmartRouterConfig()
	}
	return &SmartRouter{cfg: cfg}
}

// Route decides how to split totalAmount across the provided venue quotes. It
// returns a RouteDecision with the legs, blended rate and improvement over the
// best single-venue fill.
//
// Algorithm:
//  1. Filter venues that can fill at least MinSplitFraction of the total.
//  2. Sort by rate (best first).
//  3. Greedily fill from the best venue up to its depth, then overflow to the
//     next. Each leg's fraction is Amount/TotalAmount.
//  4. Compute the volume-weighted blended rate.
//  5. Compare to the benchmark (best single-venue rate for the full amount,
//     which may have worse slippage).
func (r *SmartRouter) Route(fromAsset, toAsset string, totalAmount decimal.Decimal, quotes []VenueQuote) (*RouteDecision, error) {
	if totalAmount.Sign() <= 0 {
		return nil, fmt.Errorf("smart router: total amount must be positive")
	}
	if len(quotes) == 0 {
		return nil, fmt.Errorf("smart router: no venue quotes provided")
	}

	minLeg := totalAmount.Mul(r.cfg.MinSplitFraction)

	// Filter viable venues.
	viable := make([]VenueQuote, 0, len(quotes))
	for _, q := range quotes {
		if q.Amount.GreaterThanOrEqual(minLeg) && q.Rate.Sign() > 0 {
			viable = append(viable, q)
		}
	}
	if len(viable) == 0 {
		return nil, fmt.Errorf("smart router: no venue meets minimum fill for %s-%s", fromAsset, toAsset)
	}

	// Sort by rate descending (best rate first for a buy).
	sort.Slice(viable, func(i, j int) bool {
		return viable[i].Rate.GreaterThan(viable[j].Rate)
	})

	// If the best venue can fill everything within slippage, prefer single.
	if r.cfg.PreferSingleVenue && viable[0].Amount.GreaterThanOrEqual(totalAmount) && viable[0].DepthBps <= r.cfg.MaxSlippageBps {
		legs := []RouteLeg{{
			Venue:     viable[0].Venue,
			Fraction:  decimal.NewFromInt(1),
			Amount:    totalAmount,
			Rate:      viable[0].Rate,
			DepthBps:  viable[0].DepthBps,
			LatencyMs: viable[0].LatencyMs,
		}}
		return &RouteDecision{
			FromAsset:      fromAsset,
			ToAsset:        toAsset,
			TotalAmount:    totalAmount,
			Legs:           legs,
			BlendedRate:    viable[0].Rate,
			BenchmarkRate:  viable[0].Rate,
			ImprovementBps: 0,
			DecidedAt:      time.Now().UTC(),
		}, nil
	}

	// Greedy fill.
	remaining := totalAmount
	var legs []RouteLeg
	for _, q := range viable {
		if remaining.Sign() <= 0 {
			break
		}
		fill := q.Amount
		if fill.GreaterThan(remaining) {
			fill = remaining
		}
		// Skip legs too small.
		if fill.LessThan(minLeg) {
			continue
		}
		frac := fill.Div(totalAmount)
		legs = append(legs, RouteLeg{
			Venue:     q.Venue,
			Fraction:  frac,
			Amount:    fill,
			Rate:      q.Rate,
			DepthBps:  q.DepthBps,
			LatencyMs: q.LatencyMs,
		})
		remaining = remaining.Sub(fill)
	}

	if len(legs) == 0 {
		return nil, fmt.Errorf("smart router: could not fill any amount for %s-%s", fromAsset, toAsset)
	}

	// If there's remaining unfilled, allocate to the last leg (best effort).
	if remaining.Sign() > 0 {
		last := &legs[len(legs)-1]
		last.Amount = last.Amount.Add(remaining)
		last.Fraction = last.Amount.Div(totalAmount)
	}

	blended := blendedRate(legs, totalAmount)
	benchmark := benchmarkRate(viable, totalAmount)
	improvement := improvementBps(blended, benchmark)

	return &RouteDecision{
		FromAsset:      fromAsset,
		ToAsset:        toAsset,
		TotalAmount:    totalAmount,
		Legs:           legs,
		BlendedRate:    blended,
		BenchmarkRate:  benchmark,
		ImprovementBps: improvement,
		DecidedAt:      time.Now().UTC(),
	}, nil
}

// blendedRate computes the volume-weighted average rate across legs.
func blendedRate(legs []RouteLeg, total decimal.Decimal) decimal.Decimal {
	if total.Sign() == 0 {
		return decimal.Zero
	}
	weightedSum := decimal.Zero
	for _, l := range legs {
		weightedSum = weightedSum.Add(l.Rate.Mul(l.Amount))
	}
	return weightedSum.Div(total)
}

// benchmarkRate is the best single-venue effective rate, ignoring depth. This
// is the rate you would get if you routed 100% to the best venue even if
// slippage is high.
func benchmarkRate(quotes []VenueQuote, total decimal.Decimal) decimal.Decimal {
	best := decimal.Zero
	for _, q := range quotes {
		if q.Amount.GreaterThanOrEqual(total) && q.Rate.GreaterThan(best) {
			best = q.Rate
		}
	}
	// If no single venue can fill, use the top rate regardless.
	if best.IsZero() && len(quotes) > 0 {
		best = quotes[0].Rate
	}
	return best
}

// improvementBps returns how many basis points blended improves over benchmark.
// Positive means blended is better.
func improvementBps(blended, benchmark decimal.Decimal) int {
	if benchmark.Sign() == 0 {
		return 0
	}
	diff, _ := blended.Sub(benchmark).Div(benchmark).Mul(decimal.NewFromInt(10000)).Float64()
	return int(math.Round(diff))
}
