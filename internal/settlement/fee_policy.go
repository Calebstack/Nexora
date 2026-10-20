package settlement

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// SettlementTier identifies the settlement speed/priority tier for dynamic fee pricing.
type SettlementTier string

const (
	TierPriority SettlementTier = "priority" // 99th percentile (p99)
	TierStandard SettlementTier = "standard" // Mode fee (mode)
	TierEconomy  SettlementTier = "economy"  // Minimum fee (min)
)

// Valid reports whether t is one of the supported settlement tiers.
func (t SettlementTier) Valid() bool {
	switch t {
	case TierPriority, TierStandard, TierEconomy:
		return true
	default:
		return false
	}
}

const (
	// DefaultFeeStatsPollInterval is the interval at which Horizon /fee-stats is polled.
	DefaultFeeStatsPollInterval = 5 * time.Second

	// DefaultCongestionThreshold is the ledger capacity usage threshold (85%) above which
	// exponential fee multipliers are applied.
	DefaultCongestionThreshold = 0.85

	// DefaultExponentialBase is the base of the exponential multiplier curve applied when
	// ledger capacity exceeds DefaultCongestionThreshold.
	DefaultExponentialBase = 4.0

	// DefaultMaxTransactionFeeStroops is the default hard maximum fee ceiling per transaction
	// (100,000 stroops = 0.01 XLM) to protect tenant treasury balances.
	DefaultMaxTransactionFeeStroops = int64(100_000)
)

var (
	ErrInvalidSettlementTier = errors.New("invalid settlement tier: must be priority, standard, or economy")
	ErrInvalidOperationCount = errors.New("operation count must be at least 1")
)

type settlementTierCtxKey struct{}

// WithSettlementTier attaches a desired SettlementTier to the context for Engine.SubmitTransfer.
func WithSettlementTier(ctx context.Context, tier SettlementTier) context.Context {
	return context.WithValue(ctx, settlementTierCtxKey{}, tier)
}

// SettlementTierFromContext extracts the SettlementTier from ctx, defaulting to TierStandard.
func SettlementTierFromContext(ctx context.Context) SettlementTier {
	if v, ok := ctx.Value(settlementTierCtxKey{}).(SettlementTier); ok && v.Valid() {
		return v
	}
	return TierStandard
}

// HorizonFeeStatsSnapshot represents a point-in-time reading from Horizon's /fee-stats endpoint.
type HorizonFeeStatsSnapshot struct {
	LastLedger          uint32    `json:"last_ledger"`
	LastLedgerBaseFee   int64     `json:"last_ledger_base_fee"`
	LedgerCapacityUsage float64   `json:"ledger_capacity_usage"`
	MinFee              int64     `json:"min_fee"`
	ModeFee             int64     `json:"mode_fee"`
	P10Fee              int64     `json:"p10_fee"`
	P50Fee              int64     `json:"p50_fee"`
	P90Fee              int64     `json:"p90_fee"`
	P95Fee              int64     `json:"p95_fee"`
	P99Fee              int64     `json:"p99_fee"`
	FetchedAt           time.Time `json:"fetched_at"`
}

// HorizonFeeStatsFetcher fetches the current fee stats from Horizon.
type HorizonFeeStatsFetcher interface {
	FetchFeeStats(ctx context.Context) (*HorizonFeeStatsSnapshot, error)
}

// HTTPFeeStatsFetcher polls Horizon's GET /fee-stats endpoint over HTTP.
type HTTPFeeStatsFetcher struct {
	baseURL    string
	httpClient *http.Client
}

// NewHTTPFeeStatsFetcher constructs an HTTP fetcher targeting a Horizon server URL.
func NewHTTPFeeStatsFetcher(horizonURL string, httpClient *http.Client) *HTTPFeeStatsFetcher {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
	}
	return &HTTPFeeStatsFetcher{
		baseURL:    strings.TrimRight(horizonURL, "/"),
		httpClient: httpClient,
	}
}

type rawHorizonFeeDistribution struct {
	Min   flexibleInt64 `json:"min"`
	Mode flexibleInt64 `json:"mode"`
	P10  flexibleInt64 `json:"p10"`
	P50  flexibleInt64 `json:"p50"`
	P90  flexibleInt64 `json:"p90"`
	P95  flexibleInt64 `json:"p95"`
	P99  flexibleInt64 `json:"p99"`
}

type rawHorizonFeeStatsResponse struct {
	LastLedger          flexibleInt64             `json:"last_ledger"`
	LastLedgerBaseFee   flexibleInt64             `json:"last_ledger_base_fee"`
	LedgerCapacityUsage flexibleFloat64           `json:"ledger_capacity_usage"`
	FeeCharged          rawHorizonFeeDistribution `json:"fee_charged"`
	// Direct flat fields supported for simplified mock payloads
	MinFee  flexibleInt64 `json:"min_fee"`
	ModeFee flexibleInt64 `json:"mode_fee"`
	P99Fee  flexibleInt64 `json:"p99_fee"`
}

type flexibleInt64 int64

func (f *flexibleInt64) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `" `)
	if s == "" || s == "null" {
		*f = 0
		return nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return err
	}
	*f = flexibleInt64(v)
	return nil
}

type flexibleFloat64 float64

func (f *flexibleFloat64) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `" `)
	if s == "" || s == "null" {
		*f = 0
		return nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}
	*f = flexibleFloat64(v)
	return nil
}

// FetchFeeStats queries GET /fee-stats (with fallback to /fee_stats) and parses the snapshot.
func (f *HTTPFeeStatsFetcher) FetchFeeStats(ctx context.Context) (*HorizonFeeStatsSnapshot, error) {
	snap, status, err := f.doGet(ctx, f.baseURL+"/fee-stats")
	if err == nil {
		return snap, nil
	}
	if status == http.StatusNotFound {
		if snapAlt, _, errAlt := f.doGet(ctx, f.baseURL+"/fee_stats"); errAlt == nil {
			return snapAlt, nil
		}
	}
	return nil, err
}

func (f *HTTPFeeStatsFetcher) doGet(ctx context.Context, urlStr string) (*HorizonFeeStatsSnapshot, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("build fee-stats request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("request fee-stats: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read fee-stats body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, fmt.Errorf("horizon fee-stats returned status %d: %s", resp.StatusCode, string(body))
	}

	var raw rawHorizonFeeStatsResponse
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, resp.StatusCode, fmt.Errorf("parse fee-stats response: %w", err)
	}

	minFee := int64(raw.FeeCharged.Min)
	if minFee <= 0 {
		minFee = int64(raw.MinFee)
	}
	modeFee := int64(raw.FeeCharged.Mode)
	if modeFee <= 0 {
		modeFee = int64(raw.ModeFee)
	}
	p99Fee := int64(raw.FeeCharged.P99)
	if p99Fee <= 0 {
		p99Fee = int64(raw.P99Fee)
	}
	baseFee := int64(raw.LastLedgerBaseFee)
	if baseFee <= 0 {
		baseFee = MinBaseFeeStroops
	}

	return &HorizonFeeStatsSnapshot{
		LastLedger:          uint32(raw.LastLedger),
		LastLedgerBaseFee:   baseFee,
		LedgerCapacityUsage: float64(raw.LedgerCapacityUsage),
		MinFee:              minFee,
		ModeFee:             modeFee,
		P10Fee:              int64(raw.FeeCharged.P10),
		P50Fee:              int64(raw.FeeCharged.P50),
		P90Fee:              int64(raw.FeeCharged.P90),
		P95Fee:              int64(raw.FeeCharged.P95),
		P99Fee:              p99Fee,
		FetchedAt:           time.Now().UTC(),
	}, http.StatusOK, nil
}

// FeePolicyConfig configures the MempoolCongestionDetector and dynamic fee multiplier policy.
type FeePolicyConfig struct {
	// PollInterval controls how often Horizon /fee-stats is polled (default 5s).
	PollInterval time.Duration
	// CongestionThreshold is the ledger capacity usage ratio above which exponential
	// multipliers are applied (default 0.85 = 85%).
	CongestionThreshold float64
	// ExponentialBase controls the steepness of the exponential fee multiplier when
	// LedgerCapacityUsage > CongestionThreshold (default 4.0).
	ExponentialBase float64
	// MinBaseFeeStroops is the floor per-operation fee (default 100 stroops).
	MinBaseFeeStroops int64
	// MaxTransactionFeeStroops is the hard maximum total fee ceiling per transaction
	// to protect tenant treasury balances (default 100,000 stroops).
	MaxTransactionFeeStroops int64
}

// DefaultFeePolicyConfig returns standard production settings for dynamic fee calculation.
func DefaultFeePolicyConfig() FeePolicyConfig {
	return FeePolicyConfig{
		PollInterval:             DefaultFeeStatsPollInterval,
		CongestionThreshold:      DefaultCongestionThreshold,
		ExponentialBase:          DefaultExponentialBase,
		MinBaseFeeStroops:        MinBaseFeeStroops,
		MaxTransactionFeeStroops: DefaultMaxTransactionFeeStroops,
	}
}

// TierFeeQuote contains the detailed fee breakdown computed by FeePolicy for a transaction.
type TierFeeQuote struct {
	Tier                SettlementTier `json:"tier"`
	OperationCount      int            `json:"operation_count"`
	LedgerCapacityUsage float64        `json:"ledger_capacity_usage"`
	Congested           bool           `json:"congested"`
	Multiplier          float64        `json:"multiplier"`
	RawTierFeeStroops   int64          `json:"raw_tier_fee_stroops"`
	PerOpFeeStroops     int64          `json:"per_op_fee_stroops"`
	TotalFeeStroops     int64          `json:"total_fee_stroops"`
	CeilingCapped       bool           `json:"ceiling_capped"`
}

// FeePolicy polls Horizon /fee-stats to detect mempool congestion and computes dynamic
// transaction fees across Priority (p99), Standard (mode), and Economy (min) tiers.
type FeePolicy struct {
	fetcher HorizonFeeStatsFetcher
	cfg     FeePolicyConfig

	mu       sync.RWMutex
	snapshot *HorizonFeeStatsSnapshot
	pollRuns int64

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewFeePolicy creates a new FeePolicy with normalized configuration defaults.
func NewFeePolicy(fetcher HorizonFeeStatsFetcher, cfg FeePolicyConfig) *FeePolicy {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = DefaultFeeStatsPollInterval
	}
	if cfg.CongestionThreshold <= 0 || cfg.CongestionThreshold >= 1.0 {
		cfg.CongestionThreshold = DefaultCongestionThreshold
	}
	if cfg.ExponentialBase <= 1.0 {
		cfg.ExponentialBase = DefaultExponentialBase
	}
	if cfg.MinBaseFeeStroops <= 0 {
		cfg.MinBaseFeeStroops = MinBaseFeeStroops
	}
	if cfg.MaxTransactionFeeStroops <= 0 {
		cfg.MaxTransactionFeeStroops = DefaultMaxTransactionFeeStroops
	}

	return &FeePolicy{
		fetcher: fetcher,
		cfg:     cfg,
		stopCh:  make(chan struct{}),
	}
}

// Start launches the background goroutine that polls Horizon /fee-stats every PollInterval (5s).
func (p *FeePolicy) Start(ctx context.Context) {
	// Perform an immediate initial poll so fee stats are available right away
	_ = p.PollOnce(ctx)

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		ticker := time.NewTicker(p.cfg.PollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-p.stopCh:
				return
			case <-ticker.C:
				if err := p.PollOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
					log.Warn().Err(err).Msg("settlement fee policy: failed to poll horizon /fee-stats")
				}
			}
		}
	}()
}

// Stop halts the background /fee-stats polling loop.
func (p *FeePolicy) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
		p.wg.Wait()
	})
}

// PollOnce fetches the latest /fee-stats from Horizon and updates the cached congestion snapshot.
func (p *FeePolicy) PollOnce(ctx context.Context) error {
	if p.fetcher == nil {
		return nil
	}
	snap, err := p.fetcher.FetchFeeStats(ctx)
	if err != nil {
		return err
	}
	if snap != nil {
		p.mu.Lock()
		cp := *snap
		p.snapshot = &cp
		p.pollRuns++
		p.mu.Unlock()
	}
	return nil
}

// UpdateSnapshot allows directly injecting a fee stats snapshot (useful for testing or external feeds).
func (p *FeePolicy) UpdateSnapshot(snap HorizonFeeStatsSnapshot) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := snap
	if cp.FetchedAt.IsZero() {
		cp.FetchedAt = time.Now().UTC()
	}
	p.snapshot = &cp
}

// CurrentSnapshot returns a copy of the latest cached Horizon fee stats snapshot.
func (p *FeePolicy) CurrentSnapshot() (HorizonFeeStatsSnapshot, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.snapshot == nil {
		return HorizonFeeStatsSnapshot{}, false
	}
	return *p.snapshot, true
}

// PollCount returns the number of successful polls executed by the detector.
func (p *FeePolicy) PollCount() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.pollRuns
}

// ComputeExponentialMultiplier calculates the fee multiplier for a given ledger capacity usage.
// Below or at CongestionThreshold (85%), the multiplier is 1.0.
// Above 85%, the multiplier grows exponentially: ExponentialBase ^ ((usage - threshold) / (1 - threshold)).
func (p *FeePolicy) ComputeExponentialMultiplier(capacityUsage float64) float64 {
	if capacityUsage <= p.cfg.CongestionThreshold {
		return 1.0
	}
	span := 1.0 - p.cfg.CongestionThreshold
	if span <= 0 {
		span = 0.15
	}
	normalizedExcess := (capacityUsage - p.cfg.CongestionThreshold) / span
	if normalizedExcess < 0 {
		normalizedExcess = 0
	}
	// Cap normalizedExcess at 2.0 to prevent float64 overflow on malformed inputs > 1.0
	if normalizedExcess > 2.0 {
		normalizedExcess = 2.0
	}
	return math.Pow(p.cfg.ExponentialBase, normalizedExcess)
}

// QuoteFee computes the optimal per-operation and total transaction fee for the requested tier
// and operation count, applying exponential congestion multipliers above 85% ledger capacity
// and enforcing the hard maximum ceiling per transaction.
func (p *FeePolicy) QuoteFee(ctx context.Context, tier SettlementTier, operationCount int) (*TierFeeQuote, error) {
	if !tier.Valid() {
		return nil, fmt.Errorf("%w: %q", ErrInvalidSettlementTier, tier)
	}
	if operationCount < 1 {
		return nil, ErrInvalidOperationCount
	}

	snap, ok := p.CurrentSnapshot()
	if !ok && p.fetcher != nil {
		_ = p.PollOnce(ctx)
		snap, ok = p.CurrentSnapshot()
	}

	var rawTierFee int64
	var capacityUsage float64
	floorFee := p.cfg.MinBaseFeeStroops

	if ok {
		capacityUsage = snap.LedgerCapacityUsage
		if snap.LastLedgerBaseFee > floorFee {
			floorFee = snap.LastLedgerBaseFee
		}
		switch tier {
		case TierPriority:
			rawTierFee = snap.P99Fee
		case TierStandard:
			rawTierFee = snap.ModeFee
		case TierEconomy:
			rawTierFee = snap.MinFee
		}
	}

	if rawTierFee < floorFee {
		rawTierFee = floorFee
	}

	congested := capacityUsage > p.cfg.CongestionThreshold
	multiplier := p.ComputeExponentialMultiplier(capacityUsage)

	perOpFee := int64(math.Ceil(math.Round(float64(rawTierFee)*multiplier*1e6) / 1e6))
	if perOpFee < floorFee {
		perOpFee = floorFee
	}

	totalFee := perOpFee * int64(operationCount)
	ceilingCapped := false

	// Enforce hard maximum ceiling per transaction to protect tenant treasury balances
	if totalFee > p.cfg.MaxTransactionFeeStroops {
		ceilingCapped = true
		totalFee = p.cfg.MaxTransactionFeeStroops
		perOpFee = totalFee / int64(operationCount)
		if perOpFee < 1 {
			perOpFee = 1
		}
		// Recompute totalFee from integer perOpFee so perOpFee * operationCount <= MaxTransactionFeeStroops
		totalFee = perOpFee * int64(operationCount)
	}

	return &TierFeeQuote{
		Tier:                tier,
		OperationCount:      operationCount,
		LedgerCapacityUsage: capacityUsage,
		Congested:           congested,
		Multiplier:          multiplier,
		RawTierFeeStroops:   rawTierFee,
		PerOpFeeStroops:     perOpFee,
		TotalFeeStroops:     totalFee,
		CeilingCapped:       ceilingCapped,
	}, nil
}

// CalculateFeeForTier is a convenience helper returning the total transaction fee in stroops
// for a single-operation transaction at the given tier.
func (p *FeePolicy) CalculateFeeForTier(ctx context.Context, tier SettlementTier) (int64, error) {
	q, err := p.QuoteFee(ctx, tier, 1)
	if err != nil {
		return 0, err
	}
	return q.TotalFeeStroops, nil
}
