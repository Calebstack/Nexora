package anchor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/api"
	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/fees"
	"github.com/Lumen-Nexora/Nexora/internal/tenant"
	"github.com/go-chi/chi/v5"
	"github.com/shopspring/decimal"
	hProtocol "github.com/stellar/go/protocols/horizon"
)

// stellarAssetPrecision is the number of decimal places of every Stellar asset.
const stellarAssetPrecision int32 = 7

// ErrAmountBelowFees mirrors the SEP `too_small` outcome: the fees would
// consume the whole amount.
var ErrAmountBelowFees = errors.New("amount does not cover fees")

// FeeDetails is the itemized fee object shared by SEP-6/24/31 transaction
// records (`fee_details`) and SEP-38 prices (`fee`). Asset uses the SEP-38
// Asset Identification Format and sum(details.amount) equals total.
type FeeDetails struct {
	Total   string      `json:"total"`
	Asset   string      `json:"asset"`
	Details []FeeDetail `json:"details,omitempty"`
}

// FeeDetail is one named component of FeeDetails.
type FeeDetail struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Amount      string `json:"amount"`
}

// FeeScheduleStore resolves the effective corridor schedule for a tenant:
// the tenant's override when one exists, otherwise the platform default
// (tenant_id NULL). It returns domain.ErrFeeScheduleNotFound when neither
// exists.
type FeeScheduleStore interface {
	EffectiveCorridorSchedule(ctx context.Context, tenantID, corridor string, tier fees.SpeedTier) (*fees.CorridorSchedule, error)
}

// NetworkFeeStats is satisfied by *horizonclient.Client.
type NetworkFeeStats interface {
	FeeStats() (hProtocol.FeeStats, error)
}

// XLMRateSource is satisfied by fx.Provider: it returns units of `to` per
// one unit of `from`.
type XLMRateSource interface {
	GetRate(ctx context.Context, from, to, amount string) (decimal.Decimal, error)
}

// FeeEstimatorConfig tunes caching. Zero values fall back to defaults.
type FeeEstimatorConfig struct {
	// ScheduleTTL bounds how long a resolved schedule is served from memory.
	ScheduleTTL time.Duration
	// FeeStatsTTL bounds how long Horizon fee stats are reused. Horizon
	// aggregates them over the last 5 ledgers, so a few seconds is enough.
	FeeStatsTTL time.Duration
	Now         func() time.Time
}

// FeeEstimator prices a transfer for disclosure: Stellar network fee,
// anchor processing fee and the local banking rail surcharge, with
// time-of-day surge pricing on the rail component.
type FeeEstimator struct {
	store    FeeScheduleStore
	feeStats NetworkFeeStats
	rates    XLMRateSource
	cfg      FeeEstimatorConfig

	mu        sync.RWMutex
	schedules map[scheduleKey]scheduleEntry

	statsMu      sync.Mutex
	stats        hProtocol.FeeStats
	statsExpires time.Time
}

type scheduleKey struct {
	tenantID string
	corridor string
	tier     fees.SpeedTier
}

type scheduleEntry struct {
	schedule *fees.CorridorSchedule
	expires  time.Time
}

func NewFeeEstimator(store FeeScheduleStore, feeStats NetworkFeeStats, rates XLMRateSource, cfg FeeEstimatorConfig) *FeeEstimator {
	if cfg.ScheduleTTL <= 0 {
		cfg.ScheduleTTL = 5 * time.Minute
	}
	if cfg.FeeStatsTTL <= 0 {
		cfg.FeeStatsTTL = 5 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &FeeEstimator{
		store:     store,
		feeStats:  feeStats,
		rates:     rates,
		cfg:       cfg,
		schedules: make(map[scheduleKey]scheduleEntry),
	}
}

// FeeEstimateRequest identifies the transfer being priced.
type FeeEstimateRequest struct {
	TenantID            string
	Asset               SEP38Asset
	DestinationCurrency string
	SpeedTier           fees.SpeedTier
	Amount              decimal.Decimal
}

// FeeEstimate is the itemized result.
type FeeEstimate struct {
	Corridor          string
	Rail              string
	SpeedTier         fees.SpeedTier
	Amount            decimal.Decimal
	AmountAfterFees   decimal.Decimal
	SurgeMultiplier   decimal.Decimal
	NetworkFeeStroops int64
	FeeDetails        FeeDetails
}

// CorridorKey is the schedule key for a Stellar asset paid out in a fiat
// currency, e.g. "USDC:NGN".
func CorridorKey(assetCode, destinationCurrency string) string {
	return strings.ToUpper(assetCode) + ":" + strings.ToUpper(destinationCurrency)
}

// Estimate itemizes the fee for req. Schedule lookup is O(1) on a cache hit.
func (e *FeeEstimator) Estimate(ctx context.Context, req FeeEstimateRequest) (*FeeEstimate, error) {
	if !req.SpeedTier.Valid() {
		return nil, fmt.Errorf("%w: unknown speed tier %q", fees.ErrInvalidCharge, req.SpeedTier)
	}
	corridor := CorridorKey(req.Asset.CleanCode(), req.DestinationCurrency)
	schedule, err := e.schedule(ctx, req.TenantID, corridor, req.SpeedTier)
	if err != nil {
		return nil, err
	}

	stats, err := e.networkFeeStats()
	if err != nil {
		return nil, fmt.Errorf("fetch network fee stats: %w", err)
	}
	networkStroops := fees.NetworkFeeStroops(schedule.Operations, inclusionBid(stats, req.SpeedTier), 0)

	xlmRate := decimal.NewFromInt(1)
	if req.Asset.CleanCode() != "XLM" {
		xlmAmount := fees.StroopsToXLM(networkStroops).String()
		xlmRate, err = e.rates.GetRate(ctx, "XLM", req.Asset.CleanCode(), xlmAmount)
		if err != nil {
			return nil, fmt.Errorf("fetch XLM/%s rate: %w", req.Asset.CleanCode(), err)
		}
	}

	breakdown, err := fees.Itemize(fees.FeeInput{
		Amount:            req.Amount,
		Schedule:          *schedule,
		NetworkFeeStroops: networkStroops,
		XLMRate:           xlmRate,
		At:                e.cfg.Now(),
		Precision:         stellarAssetPrecision,
	})
	if err != nil {
		return nil, err
	}
	if !req.Amount.GreaterThan(breakdown.Total) {
		return nil, fmt.Errorf("%w: fees %s consume the full amount %s", ErrAmountBelowFees, breakdown.Total, req.Amount)
	}

	return &FeeEstimate{
		Corridor:          corridor,
		Rail:              schedule.Rail,
		SpeedTier:         req.SpeedTier,
		Amount:            req.Amount,
		AmountAfterFees:   req.Amount.Sub(breakdown.Total),
		SurgeMultiplier:   breakdown.SurgeMultiplier,
		NetworkFeeStroops: networkStroops,
		FeeDetails:        toFeeDetails(breakdown, req.Asset.String(), schedule.Rail),
	}, nil
}

func (e *FeeEstimator) schedule(ctx context.Context, tenantID, corridor string, tier fees.SpeedTier) (*fees.CorridorSchedule, error) {
	key := scheduleKey{tenantID: tenantID, corridor: corridor, tier: tier}
	now := e.cfg.Now()

	e.mu.RLock()
	entry, ok := e.schedules[key]
	e.mu.RUnlock()
	if ok && now.Before(entry.expires) {
		return entry.schedule, nil
	}

	s, err := e.store.EffectiveCorridorSchedule(ctx, tenantID, corridor, tier)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	e.schedules[key] = scheduleEntry{schedule: s, expires: now.Add(e.cfg.ScheduleTTL)}
	e.mu.Unlock()
	return s, nil
}

func (e *FeeEstimator) networkFeeStats() (hProtocol.FeeStats, error) {
	e.statsMu.Lock()
	defer e.statsMu.Unlock()
	now := e.cfg.Now()
	if now.Before(e.statsExpires) {
		return e.stats, nil
	}
	stats, err := e.feeStats.FeeStats()
	if err != nil {
		return hProtocol.FeeStats{}, err
	}
	e.stats, e.statsExpires = stats, now.Add(e.cfg.FeeStatsTTL)
	return stats, nil
}

// inclusionBid picks the per-operation inclusion fee bid for a speed tier
// from the fee actually charged over the last 5 ledgers, never below the
// last ledger's base fee.
func inclusionBid(stats hProtocol.FeeStats, tier fees.SpeedTier) int64 {
	bid := stats.FeeCharged.P50
	switch tier {
	case fees.SpeedExpress:
		bid = stats.FeeCharged.P90
	case fees.SpeedInstant:
		bid = stats.FeeCharged.P99
	}
	if bid < stats.LastLedgerBaseFee {
		bid = stats.LastLedgerBaseFee
	}
	return bid
}

func toFeeDetails(b fees.FeeBreakdown, asset, rail string) FeeDetails {
	details := make([]FeeDetail, 0, len(b.Lines))
	for _, l := range b.Lines {
		d := FeeDetail{Amount: l.Amount.StringFixed(stellarAssetPrecision)}
		switch l.Kind {
		case fees.FeeLineNetwork:
			d.Name, d.Description = "Stellar network fee", "Inclusion fee for the Stellar transaction, converted from XLM"
		case fees.FeeLineProcessing:
			d.Name, d.Description = "Anchor processing fee", "Flat and percentage fee for processing the transfer"
		case fees.FeeLineRail:
			d.Name = "Banking rail surcharge"
			if rail != "" {
				d.Name = rail + " banking rail surcharge"
			}
			d.Description = "Local banking rail cost, including time-of-day surge pricing"
			if b.SurgeMultiplier.GreaterThan(decimal.NewFromInt(1)) {
				d.Description += " (x" + b.SurgeMultiplier.String() + ")"
			}
		}
		details = append(details, d)
	}
	return FeeDetails{Total: b.Total.StringFixed(stellarAssetPrecision), Asset: asset, Details: details}
}

// FeeHandler exposes the fee estimation endpoint.
type FeeHandler struct {
	estimator *FeeEstimator
}

func NewFeeHandler(estimator *FeeEstimator) *FeeHandler {
	return &FeeHandler{estimator: estimator}
}

// Routes mounts GET /estimate under the caller's prefix.
func (h *FeeHandler) Routes() func(r chi.Router) {
	return func(r chi.Router) {
		r.Get("/estimate", h.estimate)
	}
}

type feeEstimateResponse struct {
	Corridor          string     `json:"corridor"`
	Rail              string     `json:"rail,omitempty"`
	SpeedTier         string     `json:"speed_tier"`
	Amount            string     `json:"amount"`
	AmountAfterFees   string     `json:"amount_after_fees"`
	SurgeMultiplier   string     `json:"surge_multiplier"`
	NetworkFeeStroops int64      `json:"network_fee_stroops"`
	FeeDetails        FeeDetails `json:"fee_details"`
}

// estimate handles GET /estimate?asset=&destination_currency=&amount=&speed_tier=.
func (h *FeeHandler) estimate(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	asset := ParseSEP38Asset(q.Get("asset"))
	if asset.Scheme != "stellar" || asset.Code == "" {
		api.BadRequest(w, "asset must be a Stellar asset in SEP-38 format, e.g. stellar:USDC:G...")
		return
	}
	currency := strings.ToUpper(strings.TrimSpace(q.Get("destination_currency")))
	if len(currency) != 3 {
		api.BadRequest(w, "destination_currency must be an ISO 4217 code")
		return
	}
	amount, err := decimal.NewFromString(q.Get("amount"))
	if err != nil || !amount.IsPositive() || amount.Exponent() < -stellarAssetPrecision {
		api.BadRequest(w, "amount must be a positive number with at most 7 decimal places")
		return
	}
	tier := fees.SpeedTier(q.Get("speed_tier"))
	if tier == "" {
		tier = fees.SpeedStandard
	}
	if !tier.Valid() {
		api.BadRequest(w, "speed_tier must be one of standard, express, instant")
		return
	}

	est, err := h.estimator.Estimate(r.Context(), FeeEstimateRequest{
		TenantID:            tenant.IDFromContext(r.Context()),
		Asset:               asset,
		DestinationCurrency: currency,
		SpeedTier:           tier,
		Amount:              amount,
	})
	if err != nil {
		if errors.Is(err, domain.ErrFeeScheduleNotFound) {
			api.NotFound(w, "no fee schedule for this corridor and speed tier")
			return
		}
		if errors.Is(err, ErrAmountBelowFees) {
			api.UnprocessableEntity(w, "AMOUNT_TOO_SMALL", "amount does not cover fees")
			return
		}
		api.InternalError(w, err)
		return
	}

	api.JSON(w, http.StatusOK, feeEstimateResponse{
		Corridor:          est.Corridor,
		Rail:              est.Rail,
		SpeedTier:         string(est.SpeedTier),
		Amount:            est.Amount.StringFixed(stellarAssetPrecision),
		AmountAfterFees:   est.AmountAfterFees.StringFixed(stellarAssetPrecision),
		SurgeMultiplier:   est.SurgeMultiplier.String(),
		NetworkFeeStroops: est.NetworkFeeStroops,
		FeeDetails:        est.FeeDetails,
	})
}
