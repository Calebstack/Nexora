package anchor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/fees"
	"github.com/Lumen-Nexora/Nexora/internal/tenant"
	"github.com/go-chi/chi/v5"
	"github.com/shopspring/decimal"
	hProtocol "github.com/stellar/go/protocols/horizon"
)

const usdcIssuer = "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"

type fakeScheduleStore struct {
	byTenant map[string]*fees.CorridorSchedule // "" is the platform default
	calls    int
}

func (s *fakeScheduleStore) EffectiveCorridorSchedule(_ context.Context, tenantID, corridor string, tier fees.SpeedTier) (*fees.CorridorSchedule, error) {
	s.calls++
	if sch, ok := s.byTenant[tenantID]; ok && sch.Corridor == corridor && sch.SpeedTier == tier {
		return sch, nil
	}
	if sch, ok := s.byTenant[""]; ok && sch.Corridor == corridor && sch.SpeedTier == tier {
		return sch, nil
	}
	return nil, domain.ErrFeeScheduleNotFound
}

type fakeFeeStats struct {
	stats hProtocol.FeeStats
	calls int
}

func (f *fakeFeeStats) FeeStats() (hProtocol.FeeStats, error) {
	f.calls++
	return f.stats, nil
}

type fakeRates struct{ rate decimal.Decimal }

func (f fakeRates) GetRate(_ context.Context, from, to, _ string) (decimal.Decimal, error) {
	if from != "XLM" || to != "USDC" {
		return decimal.Zero, errors.New("unexpected pair")
	}
	return f.rate, nil
}

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func corridorSchedule(tier fees.SpeedTier, processingBps string) *fees.CorridorSchedule {
	return &fees.CorridorSchedule{
		Corridor:      "USDC:NGN",
		SpeedTier:     tier,
		Rail:          "NIP",
		Processing:    fees.Charge{Flat: dec("0.5"), Bps: dec(processingBps)},
		RailSurcharge: fees.Charge{Flat: dec("0.2")},
		Location:      time.UTC,
		SurgeWindows:  []fees.SurgeWindow{{StartMinute: 9 * 60, EndMinute: 11 * 60, Multiplier: dec("1.5")}},
		Operations:    2,
	}
}

func newTestEstimator(now *time.Time) (*FeeEstimator, *fakeScheduleStore, *fakeFeeStats) {
	store := &fakeScheduleStore{byTenant: map[string]*fees.CorridorSchedule{
		"":         corridorSchedule(fees.SpeedExpress, "20"),
		"tenant-b": corridorSchedule(fees.SpeedExpress, "10"),
	}}
	stats := &fakeFeeStats{stats: hProtocol.FeeStats{
		LastLedgerBaseFee: 100,
		FeeCharged:        hProtocol.FeeDistribution{P50: 100, P90: 250, P99: 1000},
	}}
	est := NewFeeEstimator(store, stats, fakeRates{rate: dec("0.0912345")}, FeeEstimatorConfig{
		ScheduleTTL: time.Minute,
		FeeStatsTTL: 5 * time.Second,
		Now:         func() time.Time { return *now },
	})
	return est, store, stats
}

func TestFeeEstimator_ItemizedBreakdown(t *testing.T) {
	now := time.Date(2026, 10, 19, 9, 30, 0, 0, time.UTC) // inside the surge window
	est, _, _ := newTestEstimator(&now)

	got, err := est.Estimate(context.Background(), FeeEstimateRequest{
		Asset:               ParseSEP38Asset("stellar:USDC:" + usdcIssuer),
		DestinationCurrency: "ngn",
		SpeedTier:           fees.SpeedExpress,
		Amount:              dec("250.1234567"),
	})
	if err != nil {
		t.Fatalf("Estimate: %v", err)
	}

	// Express bids p90 = 250 stroops/op * 2 ops = 500 stroops = 0.00005 XLM
	// * 0.0912345 = 0.000004561725 -> 0.0000046 USDC.
	if got.NetworkFeeStroops != 500 {
		t.Fatalf("network fee = %d stroops, want 500", got.NetworkFeeStroops)
	}
	// processing: 0.5 + 250.1234567*20/10000 = 1.0002469134 -> 1.0002470
	// rail:       0.2 * 1.5 surge = 0.3
	want := []string{"0.0000046", "1.0002470", "0.3000000"}
	if len(got.FeeDetails.Details) != 3 {
		t.Fatalf("details = %+v", got.FeeDetails.Details)
	}
	sum := decimal.Zero
	for i, d := range got.FeeDetails.Details {
		if d.Amount != want[i] {
			t.Errorf("detail %d (%s) = %s, want %s", i, d.Name, d.Amount, want[i])
		}
		sum = sum.Add(dec(d.Amount))
	}
	if got.FeeDetails.Total != "1.3002516" || !dec(got.FeeDetails.Total).Equal(sum) {
		t.Fatalf("total = %s, sum = %s, want 1.3002516", got.FeeDetails.Total, sum)
	}
	if got.FeeDetails.Asset != "stellar:USDC:"+usdcIssuer {
		t.Fatalf("asset = %s", got.FeeDetails.Asset)
	}
	if got.Corridor != "USDC:NGN" || !got.SurgeMultiplier.Equal(dec("1.5")) {
		t.Fatalf("corridor = %s surge = %s", got.Corridor, got.SurgeMultiplier)
	}
	if !got.AmountAfterFees.Equal(dec("248.8232051")) {
		t.Fatalf("amount after fees = %s, want 248.8232051", got.AmountAfterFees)
	}
}

func TestFeeEstimator_CachesSchedulesPerTenantAndFeeStats(t *testing.T) {
	now := time.Date(2026, 10, 19, 15, 0, 0, 0, time.UTC)
	est, store, stats := newTestEstimator(&now)
	req := func(tenantID string) *FeeEstimate {
		t.Helper()
		e, err := est.Estimate(context.Background(), FeeEstimateRequest{
			TenantID: tenantID, Asset: ParseSEP38Asset("stellar:USDC:" + usdcIssuer),
			DestinationCurrency: "NGN", SpeedTier: fees.SpeedExpress, Amount: dec("100"),
		})
		if err != nil {
			t.Fatalf("Estimate(%q): %v", tenantID, err)
		}
		return e
	}

	platform := req("tenant-a") // no override -> platform default (20 bps)
	override := req("tenant-b") // override (10 bps)
	req("tenant-a")
	req("tenant-b")
	if store.calls != 2 {
		t.Fatalf("store calls = %d, want 2 (one per tenant, then cache hits)", store.calls)
	}
	if stats.calls != 1 {
		t.Fatalf("fee stats calls = %d, want 1 within TTL", stats.calls)
	}
	if platform.FeeDetails.Details[1].Amount != "0.7000000" || override.FeeDetails.Details[1].Amount != "0.6000000" {
		t.Fatalf("processing platform=%s override=%s", platform.FeeDetails.Details[1].Amount, override.FeeDetails.Details[1].Amount)
	}

	now = now.Add(2 * time.Minute)
	req("tenant-a")
	if store.calls != 3 || stats.calls != 2 {
		t.Fatalf("after TTL: store calls = %d (want 3), stats calls = %d (want 2)", store.calls, stats.calls)
	}
}

func TestFeeEstimator_Errors(t *testing.T) {
	now := time.Date(2026, 10, 19, 15, 0, 0, 0, time.UTC)
	est, _, _ := newTestEstimator(&now)
	base := FeeEstimateRequest{Asset: ParseSEP38Asset("USDC"), DestinationCurrency: "NGN", SpeedTier: fees.SpeedExpress, Amount: dec("100")}

	missing := base
	missing.DestinationCurrency = "KES"
	if _, err := est.Estimate(context.Background(), missing); !errors.Is(err, domain.ErrFeeScheduleNotFound) {
		t.Fatalf("missing corridor err = %v", err)
	}

	tiny := base
	tiny.Amount = dec("0.5")
	if _, err := est.Estimate(context.Background(), tiny); !errors.Is(err, ErrAmountBelowFees) {
		t.Fatalf("tiny amount err = %v", err)
	}
}

func TestFeeHandler_Estimate(t *testing.T) {
	now := time.Date(2026, 10, 19, 15, 0, 0, 0, time.UTC)
	est, _, _ := newTestEstimator(&now)
	r := chi.NewRouter()
	r.Route("/anchor/fees", NewFeeHandler(est).Routes())

	do := func(query string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/anchor/fees/estimate?"+query, nil)
		req = req.WithContext(tenant.WithID(req.Context(), "tenant-b"))
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		return rec
	}

	rec := do("asset=stellar:USDC:" + usdcIssuer + "&destination_currency=NGN&amount=100&speed_tier=express")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body)
	}
	var body feeEstimateResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.FeeDetails.Total == "" || len(body.FeeDetails.Details) != 3 || body.SpeedTier != "express" {
		t.Fatalf("unexpected body: %+v", body)
	}

	for _, tc := range []struct {
		query string
		code  int
	}{
		{"asset=iso4217:USD&destination_currency=NGN&amount=1", http.StatusBadRequest},
		{"asset=USDC&destination_currency=NAIRA&amount=1", http.StatusBadRequest},
		{"asset=USDC&destination_currency=NGN&amount=1.12345678", http.StatusBadRequest},
		{"asset=USDC&destination_currency=NGN&amount=1&speed_tier=warp", http.StatusBadRequest},
		{"asset=USDC&destination_currency=GHS&amount=10&speed_tier=express", http.StatusNotFound},
		{"asset=USDC&destination_currency=NGN&amount=0.5&speed_tier=express", http.StatusUnprocessableEntity},
	} {
		if rec := do(tc.query); rec.Code != tc.code {
			t.Errorf("%s: status = %d, want %d (%s)", tc.query, rec.Code, tc.code, rec.Body)
		}
	}
}
