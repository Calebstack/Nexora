package fx

import (
	"errors"
	"fmt"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/shopspring/decimal"
)

// Bounds for tenant-configurable slippage tolerance: 0.05% (5 bps) to 5.0% (500 bps).
const (
	MinSlippageBps = 5
	MaxSlippageBps = 500
)

var (
	MinSlippagePct = decimal.NewFromFloat(0.05)
	MaxSlippagePct = decimal.NewFromFloat(5.0)

	ErrSlippageToleranceOutOfBounds = fmt.Errorf("slippage tolerance must be between 0.05%% and 5.0%% (5 to 500 bps)")
	ErrInsufficientLiquidityDepth   = fmt.Errorf("insufficient orderbook liquidity depth for trade size")
	ErrInvalidTradeAmount           = fmt.Errorf("trade amount must be positive")
	ErrEmptyOrderbook               = fmt.Errorf("orderbook contains no liquidity levels")
)

// SlippageTier defines trade-size based execution thresholds.
type SlippageTier struct {
	Name          string
	MinAmount     decimal.Decimal
	MaxAmount     decimal.Decimal
	DefaultMaxBps int
	CeilingBps    int
}

var defaultTiers = []SlippageTier{
	{
		Name:          "Retail",
		MinAmount:     decimal.Zero,
		MaxAmount:     decimal.NewFromInt(10_000),
		DefaultMaxBps: 50,  // 0.50%
		CeilingBps:    100, // 1.00%
	},
	{
		Name:          "Commercial",
		MinAmount:     decimal.NewFromInt(10_000),
		MaxAmount:     decimal.NewFromInt(100_000),
		DefaultMaxBps: 100, // 1.00%
		CeilingBps:    250, // 2.50%
	},
	{
		Name:          "Institutional",
		MinAmount:     decimal.NewFromInt(100_000),
		MaxAmount:     decimal.NewFromInt(1_000_000_000),
		DefaultMaxBps: 200, // 2.00%
		CeilingBps:    500, // 5.00%
	},
}

// SlippageCalculation contains comprehensive metrics on projected price impact.
type SlippageCalculation struct {
	TradeSize      decimal.Decimal `json:"trade_size"`
	TopPrice       decimal.Decimal `json:"top_price"`
	EffectivePrice decimal.Decimal `json:"effective_price"`
	PriceImpactPct decimal.Decimal `json:"price_impact_pct"`
	PriceImpactBps int             `json:"price_impact_bps"`
	TolerancePct   decimal.Decimal `json:"tolerance_pct"`
	ToleranceBps   int             `json:"tolerance_bps"`
	Tier           string          `json:"tier"`
	Allowed        bool            `json:"allowed"`
}

// SlippageExceededError represents a rejection when projected impact exceeds tolerance.
type SlippageExceededError struct {
	Calculation SlippageCalculation
}

func (e *SlippageExceededError) Error() string {
	return fmt.Sprintf(
		"slippage tolerance exceeded: projected price impact %s%% (%d bps) exceeds tolerance %s%% (%d bps) [tier=%s top_price=%s effective_price=%s trade_size=%s]",
		e.Calculation.PriceImpactPct.StringFixed(4),
		e.Calculation.PriceImpactBps,
		e.Calculation.TolerancePct.StringFixed(4),
		e.Calculation.ToleranceBps,
		e.Calculation.Tier,
		e.Calculation.TopPrice.String(),
		e.Calculation.EffectivePrice.String(),
		e.Calculation.TradeSize.String(),
	)
}

func (e *SlippageExceededError) Is(target error) bool {
	return target == domain.ErrSlippageExceeded || errors.Is(target, domain.ErrSlippageExceeded)
}

// ValidateSlippageTolerance checks whether a tenant-specified tolerance percentage
// or basis points falls within the allowed 0.05% to 5.0% range.
func ValidateSlippageTolerancePct(pct decimal.Decimal) error {
	if pct.LessThan(MinSlippagePct) || pct.GreaterThan(MaxSlippagePct) {
		return ErrSlippageToleranceOutOfBounds
	}
	return nil
}

// ValidateSlippageToleranceBps validates basis points in [5, 500].
func ValidateSlippageToleranceBps(bps int) error {
	if bps < MinSlippageBps || bps > MaxSlippageBps {
		return ErrSlippageToleranceOutOfBounds
	}
	return nil
}

// SlippageGuard coordinates dynamic slippage validation against orderbook depth.
type SlippageGuard struct {
	tiers []SlippageTier
}

// NewSlippageGuard constructs a guard initialized with multi-tier tolerance profiles.
func NewSlippageGuard() *SlippageGuard {
	return &SlippageGuard{
		tiers: defaultTiers,
	}
}

// ResolveTier determines the execution tier for a given trade size.
// Time complexity: O(T) where T is tier count (constant, T=3).
func (g *SlippageGuard) ResolveTier(tradeSize decimal.Decimal) SlippageTier {
	for _, tier := range g.tiers {
		if tradeSize.GreaterThan(tier.MinAmount) && tradeSize.LessThanOrEqual(tier.MaxAmount) {
			return tier
		}
	}
	return g.tiers[len(g.tiers)-1]
}

// CalculatePriceImpact computes volume-weighted average price across orderbook depth
// and calculates resulting price impact relative to top-of-book.
// Time complexity: O(L) where L is the number of levels filled.
// Space complexity: O(1) auxiliary memory.
func CalculatePriceImpact(tradeAmount decimal.Decimal, levels []OrderbookLevel) (effectivePrice, topPrice, impactPct decimal.Decimal, impactBps int, err error) {
	if tradeAmount.Sign() <= 0 {
		return decimal.Zero, decimal.Zero, decimal.Zero, 0, ErrInvalidTradeAmount
	}
	if len(levels) == 0 {
		return decimal.Zero, decimal.Zero, decimal.Zero, 0, ErrEmptyOrderbook
	}

	topPrice = levels[0].Price
	if topPrice.Sign() <= 0 {
		return decimal.Zero, decimal.Zero, decimal.Zero, 0, fmt.Errorf("invalid non-positive top-of-book price")
	}

	remaining := tradeAmount
	totalCost := decimal.Zero

	for _, lvl := range levels {
		if lvl.Amount.Sign() <= 0 || lvl.Price.Sign() <= 0 {
			continue
		}
		fill := decimal.Min(remaining, lvl.Amount)
		totalCost = totalCost.Add(fill.Mul(lvl.Price))
		remaining = remaining.Sub(fill)
		if remaining.IsZero() {
			break
		}
	}

	if remaining.Sign() > 0 {
		return decimal.Zero, topPrice, decimal.Zero, 0, ErrInsufficientLiquidityDepth
	}

	effectivePrice = totalCost.Div(tradeAmount)
	diff := effectivePrice.Sub(topPrice).Abs()
	impactFraction := diff.Div(topPrice)
	impactPct = impactFraction.Mul(decimal.NewFromInt(100))
	impactBps = int(impactFraction.Mul(decimal.NewFromInt(10000)).IntPart())

	return effectivePrice, topPrice, impactPct, impactBps, nil
}

// Check evaluates whether a proposed trade complies with tenant-configured tolerance
// and orderbook liquidity depth.
func (g *SlippageGuard) Check(tradeSize decimal.Decimal, userToleranceBps *int, levels []OrderbookLevel) (*SlippageCalculation, error) {
	tier := g.ResolveTier(tradeSize)

	toleranceBps := tier.DefaultMaxBps
	if userToleranceBps != nil {
		if err := ValidateSlippageToleranceBps(*userToleranceBps); err != nil {
			return nil, err
		}
		toleranceBps = *userToleranceBps
	}

	tolerancePct := decimal.NewFromInt(int64(toleranceBps)).Div(decimal.NewFromInt(100))

	effPrice, topPrice, impactPct, impactBps, err := CalculatePriceImpact(tradeSize, levels)
	if err != nil {
		return nil, err
	}

	calc := SlippageCalculation{
		TradeSize:      tradeSize,
		TopPrice:       topPrice,
		EffectivePrice: effPrice,
		PriceImpactPct: impactPct,
		PriceImpactBps: impactBps,
		TolerancePct:   tolerancePct,
		ToleranceBps:   toleranceBps,
		Tier:           tier.Name,
		Allowed:        impactBps <= toleranceBps,
	}

	if !calc.Allowed {
		return &calc, &SlippageExceededError{Calculation: calc}
	}

	return &calc, nil
}
