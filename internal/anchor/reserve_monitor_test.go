package anchor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/alerting"
	"github.com/go-chi/chi/v5"
	"github.com/shopspring/decimal"
	"github.com/stellar/go/clients/horizonclient"
	hProtocol "github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/protocols/horizon/base"
)

const (
	reserveIssuer       = "GISSUERXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
	reserveDistribution = "GDISTRIBUTIONXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
)

type fakeHorizonAssets struct {
	stat         hProtocol.AssetStat
	distribution string
	assetsErr    error
}

func (f *fakeHorizonAssets) Assets(req horizonclient.AssetRequest) (hProtocol.AssetsPage, error) {
	var page hProtocol.AssetsPage
	if f.assetsErr != nil {
		return page, f.assetsErr
	}
	if req.ForAssetCode == f.stat.Code && req.ForAssetIssuer == f.stat.Issuer {
		page.Embedded.Records = []hProtocol.AssetStat{f.stat}
	}
	return page, nil
}

func (f *fakeHorizonAssets) AccountDetail(req horizonclient.AccountRequest) (hProtocol.Account, error) {
	acct := hProtocol.Account{AccountID: req.AccountID}
	acct.Balances = []hProtocol.Balance{
		{Balance: "999.0000000", Asset: base.Asset{Type: "native"}},
		{Balance: f.distribution, Asset: base.Asset{Type: "credit_alphanum4", Code: f.stat.Code, Issuer: f.stat.Issuer}},
	}
	return acct, nil
}

type fakeBankLedger struct{ balances []BankBalance }

func (f *fakeBankLedger) Balances(context.Context, string) ([]BankBalance, error) {
	return f.balances, nil
}

type memSnapshotStore struct{ rows []ReserveSnapshot }

func (s *memSnapshotStore) InsertHourly(_ context.Context, snap *ReserveSnapshot) (bool, error) {
	for _, r := range s.rows {
		if r.AssetCode == snap.AssetCode && r.Issuer == snap.Issuer && r.SnapshotHour.Equal(snap.SnapshotHour) {
			return false, nil
		}
	}
	s.rows = append(s.rows, *snap)
	return true, nil
}

func (s *memSnapshotStore) ListSince(_ context.Context, code, issuer string, since time.Time) ([]ReserveSnapshot, error) {
	var out []ReserveSnapshot
	for _, r := range s.rows {
		if r.AssetCode == code && r.Issuer == issuer && !r.CapturedAt.Before(since) {
			out = append(out, r)
		}
	}
	return out, nil
}

type recordingRaiser struct{ raised, cleared []alerting.Incident }

func (r *recordingRaiser) Raise(_ context.Context, inc alerting.Incident) error {
	r.raised = append(r.raised, inc)
	return nil
}

func (r *recordingRaiser) Clear(_ context.Context, inc alerting.Incident) error {
	r.cleared = append(r.cleared, inc)
	return nil
}

func usdStat() hProtocol.AssetStat {
	stat := hProtocol.AssetStat{
		Asset:                   base.Asset{Type: "credit_alphanum4", Code: "USDC", Issuer: reserveIssuer},
		ClaimableBalancesAmount: "50.0000000",
		LiquidityPoolsAmount:    "25.0000000",
		ContractsAmount:         "25.0000000",
	}
	stat.Balances.Authorized = "1500.0000000"
	stat.Balances.AuthorizedToMaintainLiabilities = "0.0000000"
	stat.Balances.Unauthorized = "0.0000000"
	return stat
}

func reserveAsset() ReserveAsset {
	return ReserveAsset{Code: "USDC", Issuer: reserveIssuer, ReserveCurrency: "USD", DistributionAccounts: []string{reserveDistribution}}
}

type reserveFixture struct {
	now     time.Time
	horizon *fakeHorizonAssets
	bank    *fakeBankLedger
	store   *memSnapshotStore
	alerts  *recordingRaiser
	monitor *ReserveMonitor
}

// newReserveFixture sets on-chain supply to 1500 + 50 + 25 + 25 - 600 = 1000.
func newReserveFixture() *reserveFixture {
	f := &reserveFixture{
		now:     time.Date(2026, 10, 19, 10, 15, 0, 0, time.UTC),
		horizon: &fakeHorizonAssets{stat: usdStat(), distribution: "600.0000000"},
		bank:    &fakeBankLedger{balances: []BankBalance{{Partner: "bank-a", Currency: "USD", Amount: decimal.NewFromInt(1000)}}},
		store:   &memSnapshotStore{},
		alerts:  &recordingRaiser{},
	}
	f.monitor = NewReserveMonitor(f.horizon, f.bank, f.store, f.alerts, ReserveMonitorConfig{
		Assets: []ReserveAsset{reserveAsset()},
		Now:    func() time.Time { return f.now },
	})
	return f
}

func (f *reserveFixture) setReserves(amounts ...string) {
	f.bank.balances = nil
	for _, a := range amounts {
		f.bank.balances = append(f.bank.balances, BankBalance{Partner: "bank", Currency: "USD", Amount: decimal.RequireFromString(a)})
	}
}

func TestReserveMonitor_AssessSupplyAndRatio(t *testing.T) {
	f := newReserveFixture()
	f.setReserves("600", "400")

	s, err := f.monitor.Assess(context.Background(), reserveAsset())
	if err != nil {
		t.Fatalf("Assess: %v", err)
	}
	if !s.OnChainSupply.Equal(decimal.NewFromInt(1000)) {
		t.Fatalf("supply = %s, want 1000 (distribution holdings excluded)", s.OnChainSupply)
	}
	if !s.OffChainReserves.Equal(decimal.NewFromInt(1000)) || !s.Ratio.Equal(decimal.NewFromInt(1)) || s.Breached {
		t.Fatalf("snapshot = %+v", s)
	}
	if !s.SnapshotHour.Equal(time.Date(2026, 10, 19, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("snapshot hour = %s", s.SnapshotHour)
	}
}

func TestReserveMonitor_ToleranceBoundary(t *testing.T) {
	tests := []struct {
		reserves string
		breached bool
	}{
		{"995.1", false}, // ratio 1.004924...: inside
		{"1005", false},  // ratio 0.995024...: |dev| 0.4975% inside
		{"994.9", true},  // ratio 1.005126...: over-issued
		{"1005.1", true}, // ratio 0.994925...: under-backed by more than 0.5%
	}
	for _, tc := range tests {
		f := newReserveFixture()
		f.setReserves(tc.reserves)
		s, err := f.monitor.Assess(context.Background(), reserveAsset())
		if err != nil {
			t.Fatalf("Assess: %v", err)
		}
		if s.Breached != tc.breached {
			t.Errorf("reserves %s: ratio %s breached = %v, want %v", tc.reserves, s.Ratio, s.Breached, tc.breached)
		}
	}
}

func TestReserveMonitor_ExactlyHalfPercentIsWithinTolerance(t *testing.T) {
	f := newReserveFixture()
	f.horizon.distribution = "595.0000000" // supply 1005
	f.setReserves("1000")
	s, err := f.monitor.Assess(context.Background(), reserveAsset())
	if err != nil {
		t.Fatalf("Assess: %v", err)
	}
	if !s.Deviation.Equal(decimal.RequireFromString("0.005")) || s.Breached {
		t.Fatalf("deviation = %s breached = %v; a deviation of exactly 0.5%% is not 'greater than'", s.Deviation, s.Breached)
	}
}

func TestReserveMonitor_ZeroReservesAndCurrencyMismatch(t *testing.T) {
	f := newReserveFixture()
	f.setReserves()
	s, err := f.monitor.Assess(context.Background(), reserveAsset())
	if err != nil {
		t.Fatalf("Assess: %v", err)
	}
	if s.Ratio != nil || !s.Breached {
		t.Fatalf("zero reserves with outstanding supply must breach: %+v", s)
	}

	f.bank.balances = []BankBalance{{Partner: "bank", AccountRef: "eur-1", Currency: "EUR", Amount: decimal.NewFromInt(1000)}}
	if _, err := f.monitor.Assess(context.Background(), reserveAsset()); err == nil {
		t.Fatal("expected currency mismatch error")
	}
}

func TestReserveMonitor_RunAlertsAndSnapshotsHourly(t *testing.T) {
	f := newReserveFixture()
	ctx := context.Background()

	f.setReserves("990") // ratio 1.0101: breach
	if err := f.monitor.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(f.alerts.raised) != 1 || f.alerts.raised[0].Severity != alerting.SeverityCritical || f.alerts.raised[0].DedupKey != "anchor-reserve:USDC:"+reserveIssuer {
		t.Fatalf("raised = %+v", f.alerts.raised)
	}

	f.now = f.now.Add(20 * time.Minute) // same hour
	f.setReserves("1000")
	if err := f.monitor.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(f.alerts.cleared) != 1 {
		t.Fatalf("cleared = %d, want 1 after recovery", len(f.alerts.cleared))
	}
	if len(f.store.rows) != 1 || !f.store.rows[0].Breached {
		t.Fatalf("rows = %+v, want only the first (breached) reading of the hour", f.store.rows)
	}

	f.now = f.now.Add(time.Hour)
	if err := f.monitor.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(f.store.rows) != 2 {
		t.Fatalf("rows = %d, want a new snapshot for the next hour", len(f.store.rows))
	}
}

func TestReserveMonitor_RunContinuesPastFailingAsset(t *testing.T) {
	f := newReserveFixture()
	f.horizon.assetsErr = errors.New("horizon down")
	if err := f.monitor.Run(context.Background()); err == nil {
		t.Fatal("expected error")
	}
	if len(f.store.rows) != 0 || len(f.alerts.raised) != 0 {
		t.Fatal("a failed read must not be snapshotted or alerted")
	}
}

func TestReserveMonitor_LiquidityRunway(t *testing.T) {
	f := newReserveFixture()
	ctx := context.Background()

	f.setReserves("1100")
	if err := f.monitor.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	f.now = f.now.Add(10 * time.Hour)
	f.setReserves("1000") // reserves fell 100 over 10h: -10/h, 100h runway

	views, err := f.monitor.Liquidity(ctx)
	if err != nil {
		t.Fatalf("Liquidity: %v", err)
	}
	v := views[0]
	if !v.ReserveChangePerHour.Equal(decimal.NewFromInt(-10)) || !v.RunwayHours.Equal(decimal.NewFromInt(100)) {
		t.Fatalf("change/h = %v runway = %v", v.ReserveChangePerHour, v.RunwayHours)
	}
	if !v.Surplus.IsZero() {
		t.Fatalf("surplus = %s, want 0", v.Surplus)
	}

	f.setReserves("1200") // growing reserves: no runway limit
	views, _ = f.monitor.Liquidity(ctx)
	if views[0].RunwayHours != nil {
		t.Fatalf("runway = %s, want nil when reserves grow", views[0].RunwayHours)
	}
}

func TestReserveHandler_List(t *testing.T) {
	f := newReserveFixture()
	r := chi.NewRouter()
	r.Route("/admin/anchor-reserves", NewReserveHandler(f.monitor).Routes())

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/anchor-reserves", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body)
	}
	var body struct {
		Reserves []liquidityResponse `json:"reserves"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := body.Reserves[0]
	if got.AssetCode != "USDC" || got.OnChainSupply != "1000.0000000" || got.ReserveRatio == nil || *got.ReserveRatio != "1" || !got.WithinTolerance {
		t.Fatalf("row = %+v", got)
	}

	f.horizon.assetsErr = errors.New("down")
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/anchor-reserves", nil))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
}
