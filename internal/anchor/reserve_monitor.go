package anchor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/alerting"
	"github.com/Lumen-Nexora/Nexora/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"github.com/stellar/go/clients/horizonclient"
	hProtocol "github.com/stellar/go/protocols/horizon"
)

// ratioPrecision is the number of decimal places kept for reserve ratios.
const ratioPrecision int32 = 18

// ReserveAsset is an anchor-issued Stellar asset backed 1:1 by off-chain
// reserves held at partner banks.
type ReserveAsset struct {
	Code   string
	Issuer string
	// ReserveCurrency is the ISO 4217 currency the asset redeems for
	// (e.g. USD). Bank balances in any other currency are rejected rather
	// than silently converted.
	ReserveCurrency string
	// DistributionAccounts are issuer-controlled accounts whose holdings are
	// unsold inventory, not liabilities, and are excluded from supply.
	DistributionAccounts []string
}

func (a ReserveAsset) dedupKey() string { return "anchor-reserve:" + a.Code + ":" + a.Issuer }

// BankBalance is one off-chain partner bank account balance.
type BankBalance struct {
	Partner    string
	AccountRef string
	Currency   string
	Amount     decimal.Decimal
	AsOf       time.Time
}

// BankLedger reports off-chain reserve balances held at partner banks.
type BankLedger interface {
	Balances(ctx context.Context, currency string) ([]BankBalance, error)
}

// HorizonAssetSource is satisfied by *horizonclient.Client.
type HorizonAssetSource interface {
	Assets(request horizonclient.AssetRequest) (hProtocol.AssetsPage, error)
	AccountDetail(request horizonclient.AccountRequest) (hProtocol.Account, error)
}

// IncidentRaiser is satisfied by *alerting.Monitor.
type IncidentRaiser interface {
	Raise(ctx context.Context, inc alerting.Incident) error
	Clear(ctx context.Context, inc alerting.Incident) error
}

// ReserveSnapshot is a point-in-time solvency reading. Ratio and Deviation
// are nil when off-chain reserves are zero.
type ReserveSnapshot struct {
	ID               string
	AssetCode        string
	Issuer           string
	ReserveCurrency  string
	OnChainSupply    decimal.Decimal
	OffChainReserves decimal.Decimal
	Ratio            *decimal.Decimal
	Deviation        *decimal.Decimal
	Breached         bool
	SnapshotHour     time.Time
	CapturedAt       time.Time
}

// ReserveSnapshotStore persists append-only hourly snapshots.
type ReserveSnapshotStore interface {
	// InsertHourly records s unless a snapshot for the same asset and
	// SnapshotHour exists, reporting whether a row was written.
	InsertHourly(ctx context.Context, s *ReserveSnapshot) (bool, error)
	// ListSince returns snapshots captured at or after since, oldest first.
	ListSince(ctx context.Context, assetCode, issuer string, since time.Time) ([]ReserveSnapshot, error)
}

// ReserveMonitorConfig configures the monitor. Zero values fall back to
// defaults.
type ReserveMonitorConfig struct {
	Assets []ReserveAsset
	// Tolerance is the maximum allowed |ratio - 1| (default 0.005 = 0.5%).
	Tolerance decimal.Decimal
	// RunwayWindow is the history used to project liquidity runway (default 24h).
	RunwayWindow time.Duration
	Source       string
	Now          func() time.Time
}

// ReserveMonitor compares on-chain supply with off-chain reserves.
type ReserveMonitor struct {
	horizon   HorizonAssetSource
	bank      BankLedger
	snapshots ReserveSnapshotStore
	alerts    IncidentRaiser
	cfg       ReserveMonitorConfig
}

func NewReserveMonitor(horizon HorizonAssetSource, bank BankLedger, snapshots ReserveSnapshotStore, alerts IncidentRaiser, cfg ReserveMonitorConfig) *ReserveMonitor {
	if !cfg.Tolerance.IsPositive() {
		cfg.Tolerance = decimal.RequireFromString("0.005")
	}
	if cfg.RunwayWindow <= 0 {
		cfg.RunwayWindow = 24 * time.Hour
	}
	if cfg.Source == "" {
		cfg.Source = "nexora-reserve-monitor"
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &ReserveMonitor{horizon: horizon, bank: bank, snapshots: snapshots, alerts: alerts, cfg: cfg}
}

// Assess reads live balances for asset and computes the reserve ratio
// OnChainSupply / OffChainReserves. Costs 1 + len(DistributionAccounts)
// Horizon calls and one bank ledger read.
func (m *ReserveMonitor) Assess(ctx context.Context, asset ReserveAsset) (*ReserveSnapshot, error) {
	supply, err := m.onChainSupply(asset)
	if err != nil {
		return nil, err
	}
	reserves, err := m.offChainReserves(ctx, asset)
	if err != nil {
		return nil, err
	}

	now := m.cfg.Now().UTC()
	s := &ReserveSnapshot{
		AssetCode:        asset.Code,
		Issuer:           asset.Issuer,
		ReserveCurrency:  asset.ReserveCurrency,
		OnChainSupply:    supply,
		OffChainReserves: reserves,
		SnapshotHour:     now.Truncate(time.Hour),
		CapturedAt:       now,
	}
	switch {
	case reserves.IsPositive():
		ratio := supply.DivRound(reserves, ratioPrecision)
		deviation := ratio.Sub(decimal.NewFromInt(1)).Abs()
		s.Ratio, s.Deviation = &ratio, &deviation
		s.Breached = deviation.GreaterThan(m.cfg.Tolerance)
	default:
		// No reserves: any outstanding supply is unbacked.
		s.Breached = supply.IsPositive()
	}
	return s, nil
}

// Run evaluates every configured asset: it pages on-call when a ratio leaves
// tolerance, resolves the page when it returns, and records the first
// reading of each hour as an immutable snapshot. Intended to be scheduled
// more often than hourly; per-asset failures do not stop other assets.
func (m *ReserveMonitor) Run(ctx context.Context) error {
	var errs []error
	for _, asset := range m.cfg.Assets {
		s, err := m.Assess(ctx, asset)
		if err != nil {
			errs = append(errs, fmt.Errorf("assess %s: %w", asset.Code, err))
			continue
		}
		inc := m.incident(asset, s)
		if s.Breached {
			err = m.alerts.Raise(ctx, inc)
		} else {
			err = m.alerts.Clear(ctx, inc)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("alert %s: %w", asset.Code, err))
		}
		if _, err := m.snapshots.InsertHourly(ctx, s); err != nil {
			errs = append(errs, fmt.Errorf("snapshot %s: %w", asset.Code, err))
		}
	}
	return errors.Join(errs...)
}

// LiquidityView is the operations dashboard row for one asset.
type LiquidityView struct {
	Snapshot *ReserveSnapshot
	// Surplus is OffChainReserves - OnChainSupply; negative means a shortfall.
	Surplus decimal.Decimal
	// ReserveChangePerHour is the average hourly change in off-chain reserves
	// over the runway window, nil without history.
	ReserveChangePerHour *decimal.Decimal
	// RunwayHours is how long current reserves last at that rate, nil when
	// reserves are not declining.
	RunwayHours *decimal.Decimal
}

// Liquidity assesses every asset live and projects runway from the
// snapshot history. O(a * h) for a assets and h hourly snapshots in the window.
func (m *ReserveMonitor) Liquidity(ctx context.Context) ([]LiquidityView, error) {
	views := make([]LiquidityView, 0, len(m.cfg.Assets))
	for _, asset := range m.cfg.Assets {
		s, err := m.Assess(ctx, asset)
		if err != nil {
			return nil, fmt.Errorf("assess %s: %w", asset.Code, err)
		}
		history, err := m.snapshots.ListSince(ctx, asset.Code, asset.Issuer, s.CapturedAt.Add(-m.cfg.RunwayWindow))
		if err != nil {
			return nil, fmt.Errorf("list snapshots %s: %w", asset.Code, err)
		}
		view := LiquidityView{Snapshot: s, Surplus: s.OffChainReserves.Sub(s.OnChainSupply)}
		if len(history) > 0 {
			oldest := history[0]
			if hours := decimal.NewFromFloat(s.CapturedAt.Sub(oldest.CapturedAt).Hours()); hours.IsPositive() {
				rate := s.OffChainReserves.Sub(oldest.OffChainReserves).DivRound(hours, stellarAssetPrecision)
				view.ReserveChangePerHour = &rate
				if rate.IsNegative() {
					runway := s.OffChainReserves.DivRound(rate.Neg(), 2)
					view.RunwayHours = &runway
				}
			}
		}
		views = append(views, view)
	}
	return views, nil
}

func (m *ReserveMonitor) onChainSupply(asset ReserveAsset) (decimal.Decimal, error) {
	page, err := m.horizon.Assets(horizonclient.AssetRequest{ForAssetCode: asset.Code, ForAssetIssuer: asset.Issuer})
	if err != nil {
		return decimal.Zero, fmt.Errorf("horizon assets: %w", err)
	}
	var stat *hProtocol.AssetStat
	for i := range page.Embedded.Records {
		if r := &page.Embedded.Records[i]; r.Code == asset.Code && r.Issuer == asset.Issuer {
			stat = r
			break
		}
	}
	if stat == nil {
		return decimal.Zero, fmt.Errorf("asset %s:%s not found on Horizon", asset.Code, asset.Issuer)
	}

	// Every unit held outside the issuer: trustlines in each authorization
	// state, claimable balances, liquidity pools and Soroban contracts.
	supply := decimal.Zero
	for _, amount := range []string{
		stat.Balances.Authorized,
		stat.Balances.AuthorizedToMaintainLiabilities,
		stat.Balances.Unauthorized,
		stat.ClaimableBalancesAmount,
		stat.LiquidityPoolsAmount,
		stat.ContractsAmount,
	} {
		v, err := parseAmount(amount)
		if err != nil {
			return decimal.Zero, fmt.Errorf("asset %s: %w", asset.Code, err)
		}
		supply = supply.Add(v)
	}

	for _, account := range asset.DistributionAccounts {
		acct, err := m.horizon.AccountDetail(horizonclient.AccountRequest{AccountID: account})
		if err != nil {
			return decimal.Zero, fmt.Errorf("horizon account %s: %w", account, err)
		}
		for _, b := range acct.Balances {
			if b.Code == asset.Code && b.Issuer == asset.Issuer {
				held, err := parseAmount(b.Balance)
				if err != nil {
					return decimal.Zero, fmt.Errorf("account %s: %w", account, err)
				}
				supply = supply.Sub(held)
			}
		}
	}
	return supply, nil
}

func (m *ReserveMonitor) offChainReserves(ctx context.Context, asset ReserveAsset) (decimal.Decimal, error) {
	balances, err := m.bank.Balances(ctx, asset.ReserveCurrency)
	if err != nil {
		return decimal.Zero, fmt.Errorf("bank ledger: %w", err)
	}
	total := decimal.Zero
	for _, b := range balances {
		if !strings.EqualFold(b.Currency, asset.ReserveCurrency) {
			return decimal.Zero, fmt.Errorf("bank account %s/%s is in %s, expected %s", b.Partner, b.AccountRef, b.Currency, asset.ReserveCurrency)
		}
		total = total.Add(b.Amount)
	}
	return total, nil
}

func (m *ReserveMonitor) incident(asset ReserveAsset, s *ReserveSnapshot) alerting.Incident {
	ratio := "undefined (no off-chain reserves)"
	if s.Ratio != nil {
		ratio = s.Ratio.StringFixed(6)
	}
	return alerting.Incident{
		DedupKey: asset.dedupKey(),
		Summary: fmt.Sprintf("%s reserve ratio %s is outside 1.00 ± %s%%",
			asset.Code, ratio, m.cfg.Tolerance.Mul(decimal.NewFromInt(100)).String()),
		Source:   m.cfg.Source,
		Severity: alerting.SeverityCritical,
		Details: map[string]any{
			"asset":              asset.Code + ":" + asset.Issuer,
			"on_chain_supply":    s.OnChainSupply.String(),
			"off_chain_reserves": s.OffChainReserves.String() + " " + asset.ReserveCurrency,
			"reserve_ratio":      ratio,
		},
		Timestamp: s.CapturedAt,
	}
}

// parseAmount parses a Horizon amount string, treating an empty value as zero.
func parseAmount(s string) (decimal.Decimal, error) {
	if s == "" {
		return decimal.Zero, nil
	}
	v, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Zero, fmt.Errorf("invalid amount %q: %w", s, err)
	}
	return v, nil
}

// ReserveHandler serves the liquidity dashboard for platform operations.
type ReserveHandler struct {
	monitor *ReserveMonitor
}

func NewReserveHandler(monitor *ReserveMonitor) *ReserveHandler {
	return &ReserveHandler{monitor: monitor}
}

// Routes mounts GET / under the caller's (operator-only) prefix.
func (h *ReserveHandler) Routes() func(r chi.Router) {
	return func(r chi.Router) {
		r.Get("/", h.list)
	}
}

type liquidityResponse struct {
	AssetCode            string  `json:"asset_code"`
	Issuer               string  `json:"issuer"`
	ReserveCurrency      string  `json:"reserve_currency"`
	OnChainSupply        string  `json:"on_chain_supply"`
	OffChainReserves     string  `json:"off_chain_reserves"`
	ReserveRatio         *string `json:"reserve_ratio"`
	Deviation            *string `json:"deviation"`
	WithinTolerance      bool    `json:"within_tolerance"`
	Surplus              string  `json:"surplus"`
	ReserveChangePerHour *string `json:"reserve_change_per_hour"`
	RunwayHours          *string `json:"runway_hours"`
	AsOf                 string  `json:"as_of"`
}

func (h *ReserveHandler) list(w http.ResponseWriter, r *http.Request) {
	views, err := h.monitor.Liquidity(r.Context())
	if err != nil {
		log.Error().Err(err).Msg("anchor reserves: liquidity assessment failed")
		api.Error(w, http.StatusBadGateway, "RESERVE_SOURCE_UNAVAILABLE", "could not read reserve balances")
		return
	}
	out := make([]liquidityResponse, 0, len(views))
	for _, v := range views {
		s := v.Snapshot
		out = append(out, liquidityResponse{
			AssetCode:            s.AssetCode,
			Issuer:               s.Issuer,
			ReserveCurrency:      s.ReserveCurrency,
			OnChainSupply:        s.OnChainSupply.StringFixed(stellarAssetPrecision),
			OffChainReserves:     s.OffChainReserves.String(),
			ReserveRatio:         decimalString(s.Ratio),
			Deviation:            decimalString(s.Deviation),
			WithinTolerance:      !s.Breached,
			Surplus:              v.Surplus.String(),
			ReserveChangePerHour: decimalString(v.ReserveChangePerHour),
			RunwayHours:          decimalString(v.RunwayHours),
			AsOf:                 s.CapturedAt.Format(time.RFC3339),
		})
	}
	api.JSON(w, http.StatusOK, map[string]any{"reserves": out})
}

func decimalString(d *decimal.Decimal) *string {
	if d == nil {
		return nil
	}
	s := d.String()
	return &s
}
