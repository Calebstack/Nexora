package fees

import (
	"errors"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

// SpeedTier selects how aggressively a transfer is priced for settlement
// latency. Faster tiers bid a higher Stellar inclusion fee percentile and
// carry their own corridor processing charge.
type SpeedTier string

const (
	SpeedStandard SpeedTier = "standard"
	SpeedExpress  SpeedTier = "express"
	SpeedInstant  SpeedTier = "instant"
)

// Valid reports whether t is a known speed tier.
func (t SpeedTier) Valid() bool {
	switch t {
	case SpeedStandard, SpeedExpress, SpeedInstant:
		return true
	}
	return false
}

// StroopsPerXLM is the number of stroops in one lumen (1 stroop = 0.0000001 XLM).
const StroopsPerXLM = 10_000_000

// MinBaseFeeStroops is the network minimum inclusion fee per operation.
const MinBaseFeeStroops int64 = 100

// minutesPerDay bounds SurgeWindow offsets.
const minutesPerDay = 24 * 60

var (
	ErrNonPositiveAmount = errors.New("amount must be greater than zero")
	ErrInvalidCharge     = errors.New("invalid fee charge")
	ErrInvalidSurge      = errors.New("invalid surge window")
	ErrInvalidRate       = errors.New("conversion rate must be greater than zero")
)

// Charge is a flat amount plus a proportional component expressed in basis
// points, clamped to [Min, Max]. Bps is a decimal so fractional basis points
// (e.g. 12.5) are represented exactly.
type Charge struct {
	Flat decimal.Decimal
	Bps  decimal.Decimal
	Min  decimal.Decimal
	Max  *decimal.Decimal
}

// Validate rejects negative components and an inverted [Min, Max] range.
func (c Charge) Validate() error {
	if c.Flat.IsNegative() || c.Bps.IsNegative() || c.Min.IsNegative() {
		return fmt.Errorf("%w: flat, bps and min must not be negative", ErrInvalidCharge)
	}
	if c.Max != nil && c.Max.LessThan(c.Min) {
		return fmt.Errorf("%w: max %s is below min %s", ErrInvalidCharge, c.Max, c.Min)
	}
	return nil
}

// apply returns (Flat + amount*Bps/10000) * multiplier, clamped to [Min, Max].
// The clamp runs after the multiplier so contractual caps always hold.
func (c Charge) apply(amount, multiplier decimal.Decimal) decimal.Decimal {
	fee := c.Flat.Add(amount.Mul(c.Bps).Div(bpsDivisor)).Mul(multiplier)
	if fee.LessThan(c.Min) {
		fee = c.Min
	}
	if c.Max != nil && fee.GreaterThan(*c.Max) {
		fee = *c.Max
	}
	return fee
}

// SurgeWindow raises the banking rail surcharge during a congested banking
// window. StartMinute and EndMinute are minutes after local midnight in the
// corridor's time zone; the window is [Start, End) and wraps midnight when
// End <= Start. An empty Weekdays slice matches every day; for a wrapping
// window the weekday is the one on which the window opened.
type SurgeWindow struct {
	Weekdays    []time.Weekday
	StartMinute int
	EndMinute   int
	Multiplier  decimal.Decimal
}

// Validate rejects out-of-range offsets and multipliers below 1, which would
// turn a surge into a discount.
func (w SurgeWindow) Validate() error {
	if w.StartMinute < 0 || w.StartMinute >= minutesPerDay || w.EndMinute < 0 || w.EndMinute > minutesPerDay {
		return fmt.Errorf("%w: minutes must be within [0, %d]", ErrInvalidSurge, minutesPerDay)
	}
	if w.StartMinute == w.EndMinute {
		return fmt.Errorf("%w: window is empty", ErrInvalidSurge)
	}
	if w.Multiplier.LessThan(decimal.NewFromInt(1)) {
		return fmt.Errorf("%w: multiplier must be at least 1", ErrInvalidSurge)
	}
	return nil
}

func (w SurgeWindow) matches(local time.Time) bool {
	minute := local.Hour()*60 + local.Minute()
	day := local.Weekday()
	if w.EndMinute > w.StartMinute {
		return minute >= w.StartMinute && minute < w.EndMinute && w.onDay(day)
	}
	// Wrapping window: the late part opened today, the early part opened yesterday.
	if minute >= w.StartMinute {
		return w.onDay(day)
	}
	return minute < w.EndMinute && w.onDay((day+6)%7)
}

func (w SurgeWindow) onDay(d time.Weekday) bool {
	if len(w.Weekdays) == 0 {
		return true
	}
	for _, wd := range w.Weekdays {
		if wd == d {
			return true
		}
	}
	return false
}

// SurgeMultiplier returns the highest multiplier among the windows active at
// `at` in loc, or 1 when none is active. O(w) in the number of windows.
func SurgeMultiplier(windows []SurgeWindow, at time.Time, loc *time.Location) decimal.Decimal {
	if loc == nil {
		loc = time.UTC
	}
	local := at.In(loc)
	m := decimal.NewFromInt(1)
	for _, w := range windows {
		if w.matches(local) && w.Multiplier.GreaterThan(m) {
			m = w.Multiplier
		}
	}
	return m
}

// CorridorSchedule is the effective fee schedule for one transfer corridor
// and speed tier.
type CorridorSchedule struct {
	Corridor      string
	SpeedTier     SpeedTier
	Rail          string
	Processing    Charge
	RailSurcharge Charge
	Location      *time.Location
	SurgeWindows  []SurgeWindow
	// Operations is the number of Stellar operations a transfer in this
	// corridor submits; the inclusion fee is charged per operation.
	Operations int
}

// Validate checks every component of the schedule.
func (s CorridorSchedule) Validate() error {
	if !s.SpeedTier.Valid() {
		return fmt.Errorf("%w: unknown speed tier %q", ErrInvalidCharge, s.SpeedTier)
	}
	if s.Operations < 1 {
		return fmt.Errorf("%w: operations must be at least 1", ErrInvalidCharge)
	}
	if err := s.Processing.Validate(); err != nil {
		return fmt.Errorf("processing: %w", err)
	}
	if err := s.RailSurcharge.Validate(); err != nil {
		return fmt.Errorf("rail surcharge: %w", err)
	}
	for i, w := range s.SurgeWindows {
		if err := w.Validate(); err != nil {
			return fmt.Errorf("surge window %d: %w", i, err)
		}
	}
	return nil
}

// NetworkFeeStroops is the Stellar transaction fee: operations * base fee
// (the inclusion fee) plus the Soroban resource fee, which is zero for
// classic transactions. The base fee never drops below the network minimum.
func NetworkFeeStroops(operations int, baseFeePerOp, resourceFee int64) int64 {
	if baseFeePerOp < MinBaseFeeStroops {
		baseFeePerOp = MinBaseFeeStroops
	}
	return int64(operations)*baseFeePerOp + resourceFee
}

// StroopsToXLM converts stroops to lumens exactly.
func StroopsToXLM(stroops int64) decimal.Decimal {
	return decimal.New(stroops, -7)
}

// FeeLineKind identifies a component of an itemized fee.
type FeeLineKind string

const (
	FeeLineNetwork    FeeLineKind = "network"
	FeeLineProcessing FeeLineKind = "processing"
	FeeLineRail       FeeLineKind = "rail_surcharge"
)

// FeeLine is one itemized fee component, already rounded to the fee asset's
// precision.
type FeeLine struct {
	Kind   FeeLineKind
	Amount decimal.Decimal
}

// FeeInput carries everything needed to itemize a fee. Amount and every
// returned line are denominated in the fee asset.
type FeeInput struct {
	Amount   decimal.Decimal
	Schedule CorridorSchedule
	// NetworkFeeStroops is the Stellar fee for the transfer, in stroops.
	NetworkFeeStroops int64
	// XLMRate is the price of one XLM in the fee asset.
	XLMRate decimal.Decimal
	At      time.Time
	// Precision is the number of decimal places of the fee asset (7 for
	// Stellar assets).
	Precision int32
}

// FeeBreakdown is an itemized fee whose Total equals the exact sum of its
// lines, as SEP-6/24/31/38 fee disclosures require.
type FeeBreakdown struct {
	Lines           []FeeLine
	Total           decimal.Decimal
	SurgeMultiplier decimal.Decimal
}

// Itemize computes the network, processing and rail surcharge components.
// Each component is rounded up to the asset precision before summing, so the
// total never under-collects and always equals sum(lines) exactly. O(w) in
// the number of surge windows.
func Itemize(in FeeInput) (FeeBreakdown, error) {
	if !in.Amount.IsPositive() {
		return FeeBreakdown{}, ErrNonPositiveAmount
	}
	if in.NetworkFeeStroops > 0 && !in.XLMRate.IsPositive() {
		return FeeBreakdown{}, ErrInvalidRate
	}
	if err := in.Schedule.Validate(); err != nil {
		return FeeBreakdown{}, err
	}

	one := decimal.NewFromInt(1)
	surge := SurgeMultiplier(in.Schedule.SurgeWindows, in.At, in.Schedule.Location)

	lines := []FeeLine{
		{Kind: FeeLineNetwork, Amount: StroopsToXLM(in.NetworkFeeStroops).Mul(in.XLMRate).RoundUp(in.Precision)},
		{Kind: FeeLineProcessing, Amount: in.Schedule.Processing.apply(in.Amount, one).RoundUp(in.Precision)},
		{Kind: FeeLineRail, Amount: in.Schedule.RailSurcharge.apply(in.Amount, surge).RoundUp(in.Precision)},
	}
	total := decimal.Zero
	for _, l := range lines {
		total = total.Add(l.Amount)
	}
	return FeeBreakdown{Lines: lines, Total: total, SurgeMultiplier: surge}, nil
}
