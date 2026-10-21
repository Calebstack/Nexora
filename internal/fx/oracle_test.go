package fx

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestCoinGeckoProvider_XLMToUSDC(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"stellar":{"usd":0.25,"eur":0.23}}`))
	}))
	defer srv.Close()

	p := NewCoinGeckoProviderWithURL(srv.URL, []string{"XLM-USDC", "USDC-XLM"})
	rate, err := p.GetRate(context.Background(), "XLM", "USDC", "1")
	if err != nil {
		t.Fatalf("GetRate: %v", err)
	}
	want := decimal.RequireFromString("0.25")
	if !rate.Equal(want) {
		t.Fatalf("rate = %s, want %s", rate, want)
	}
}

func TestCoinGeckoProvider_USDCToXLM(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"stellar":{"usd":0.25,"eur":0.23}}`))
	}))
	defer srv.Close()

	p := NewCoinGeckoProviderWithURL(srv.URL, []string{"USDC-XLM"})
	rate, err := p.GetRate(context.Background(), "USDC", "XLM", "1")
	if err != nil {
		t.Fatalf("GetRate: %v", err)
	}
	want := decimal.NewFromInt(1).Div(decimal.RequireFromString("0.25"))
	if !rate.Equal(want) {
		t.Fatalf("rate = %s, want %s", rate, want)
	}
}

func TestCoinGeckoProvider_UnsupportedPair(t *testing.T) {
	p := NewCoinGeckoProvider([]string{"XLM-USDC"})
	_, err := p.GetRate(context.Background(), "USDC", "EURC", "1")
	if err == nil {
		t.Fatal("expected error for unsupported pair")
	}
}

func TestIsNativeAssetAndTrustline(t *testing.T) {
	if !IsNativeAsset("xlm") || !IsNativeAsset("XLM") {
		t.Fatal("expected XLM to be native")
	}
	if AssetRequiresTrustline("XLM") {
		t.Fatal("XLM must not require a trustline")
	}
	if !AssetRequiresTrustline("USDC") {
		t.Fatal("USDC must require a trustline")
	}
}

func TestValidateAmountLimits_XLM(t *testing.T) {
	if err := validateAmountLimits("XLM", decimal.RequireFromString("0.0000001")); err != nil {
		t.Fatalf("min amount should pass: %v", err)
	}
	if err := validateAmountLimits("XLM", decimal.RequireFromString("0.00000005")); err == nil {
		t.Fatal("below min should fail")
	}
	if err := validateAmountLimits("XLM", decimal.NewFromInt(1_000_001)); err == nil {
		t.Fatal("above max should fail")
	}
	if err := validateAmountLimits("USDC", decimal.NewFromInt(1_000_001)); err != nil {
		t.Fatalf("non-XLM should skip XLM limits: %v", err)
	}
}

type mockSpotProvider struct {
	name   string
	ticker Ticker
	err    error
}

func (m *mockSpotProvider) Name() string {
	return m.name
}

func (m *mockSpotProvider) FetchTicker(_ context.Context, _ string) (Ticker, error) {
	if m.err != nil {
		return Ticker{}, m.err
	}
	return m.ticker, nil
}

func TestOracleAggregator_MultiSourceVWAP(t *testing.T) {
	now := time.Now().UTC()
	p1 := &mockSpotProvider{
		name: "binance",
		ticker: Ticker{
			Source:    "binance",
			Pair:      "XLM-USDC",
			Price:     decimal.RequireFromString("0.20"),
			Volume:    decimal.RequireFromString("1000"),
			Timestamp: now,
		},
	}
	p2 := &mockSpotProvider{
		name: "kraken",
		ticker: Ticker{
			Source:    "kraken",
			Pair:      "XLM-USDC",
			Price:     decimal.RequireFromString("0.22"),
			Volume:    decimal.RequireFromString("3000"),
			Timestamp: now,
		},
	}
	p3 := &mockSpotProvider{
		name: "stellar_dex",
		ticker: Ticker{
			Source:    "stellar_dex",
			Pair:      "XLM-USDC",
			Price:     decimal.RequireFromString("0.21"),
			Volume:    decimal.RequireFromString("2000"),
			Timestamp: now,
		},
	}

	agg := NewOracleAggregator([]SpotRateProvider{p1, p2, p3}, []string{"XLM-USDC"})
	rate, err := agg.AggregateWithTime(context.Background(), "XLM-USDC", now)
	if err != nil {
		t.Fatalf("unexpected aggregate error: %v", err)
	}

	// VWAP: (0.20*1000 + 0.22*3000 + 0.21*2000) / (1000 + 3000 + 2000)
	// (200 + 660 + 420) / 6000 = 1280 / 6000 = 0.2133333333333333
	want := decimal.RequireFromString("1280").Div(decimal.RequireFromString("6000"))
	if !rate.Equal(want) {
		t.Fatalf("rate = %s, want %s", rate, want)
	}
}

func TestOracleAggregator_TrimmedMeanEliminatesOutliers(t *testing.T) {
	now := time.Now().UTC()
	// Build 10 feeds where bottom is 0.10, top is manipulated spike at 10.00, middle 8 are between 0.20 and 0.22
	providers := make([]SpotRateProvider, 10)
	for i := 0; i < 10; i++ {
		var price string
		switch i {
		case 0:
			price = "0.01" // Low outlier
		case 9:
			price = "15.00" // Manipulated sudden price spike
		default:
			price = fmt.Sprintf("0.2%d", i) // 0.21 .. 0.28
		}
		providers[i] = &mockSpotProvider{
			name: fmt.Sprintf("prov_%d", i),
			ticker: Ticker{
				Source:    fmt.Sprintf("prov_%d", i),
				Pair:      "XLM-USDC",
				Price:     decimal.RequireFromString(price),
				Volume:    decimal.RequireFromString("100"),
				Timestamp: now,
			},
		}
	}

	agg := NewOracleAggregator(providers, []string{"XLM-USDC"})
	agg.SetTrimPercent(0.10) // 10% bottom (index 0) and 10% top (index 9) discarded

	rate, err := agg.AggregateWithTime(context.Background(), "XLM-USDC", now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Rate must not be polluted by the 15.00 spike or 0.01 dump
	if rate.GreaterThan(decimal.RequireFromString("0.30")) || rate.LessThan(decimal.RequireFromString("0.20")) {
		t.Fatalf("trimmed mean failed to exclude outlier spike: got rate %s", rate)
	}
}

func TestOracleAggregator_StalenessGuard15Seconds(t *testing.T) {
	evalTime := time.Now().UTC()
	staleTime := evalTime.Add(-16 * time.Second) // 16s old > 15s max
	freshTime := evalTime.Add(-5 * time.Second)

	p1 := &mockSpotProvider{
		name: "p1",
		ticker: Ticker{
			Source:    "p1",
			Pair:      "XLM-USDC",
			Price:     decimal.RequireFromString("0.20"),
			Volume:    decimal.RequireFromString("1000"),
			Timestamp: staleTime,
		},
	}
	p2 := &mockSpotProvider{
		name: "p2",
		ticker: Ticker{
			Source:    "p2",
			Pair:      "XLM-USDC",
			Price:     decimal.RequireFromString("0.21"),
			Volume:    decimal.RequireFromString("1000"),
			Timestamp: staleTime,
		},
	}
	p3 := &mockSpotProvider{
		name: "p3",
		ticker: Ticker{
			Source:    "p3",
			Pair:      "XLM-USDC",
			Price:     decimal.RequireFromString("0.22"),
			Volume:    decimal.RequireFromString("1000"),
			Timestamp: staleTime,
		},
	}

	agg := NewOracleAggregator([]SpotRateProvider{p1, p2, p3}, []string{"XLM-USDC"})
	agg.SetStalenessGuard(15 * time.Second)

	_, err := agg.AggregateWithTime(context.Background(), "XLM-USDC", evalTime)
	if err == nil {
		t.Fatal("expected error due to all feeds being stale (> 15s)")
	}

	// Now make one feed fresh and set min valid sources to 1
	p1.ticker.Timestamp = freshTime
	agg.SetMinValidSources(1)

	rate, err := agg.AggregateWithTime(context.Background(), "XLM-USDC", evalTime)
	if err != nil {
		t.Fatalf("unexpected error after refreshing feed: %v", err)
	}
	if !rate.Equal(decimal.RequireFromString("0.20")) {
		t.Fatalf("rate = %s, want 0.20", rate)
	}
}

func TestOracleAggregator_DisconnectedFeedsFallback(t *testing.T) {
	now := time.Now().UTC()
	p1 := &mockSpotProvider{
		name: "binance_down",
		err:  fmt.Errorf("connection refused: dial tcp timeout"),
	}
	p2 := &mockSpotProvider{
		name: "kraken_down",
		err:  fmt.Errorf("503 Service Unavailable"),
	}
	p3 := &mockSpotProvider{
		name: "stellar_dex_alive",
		ticker: Ticker{
			Source:    "stellar_dex",
			Pair:      "XLM-USDC",
			Price:     decimal.RequireFromString("0.25"),
			Volume:    decimal.RequireFromString("500"),
			Timestamp: now,
		},
	}

	agg := NewOracleAggregator([]SpotRateProvider{p1, p2, p3}, []string{"XLM-USDC"})
	agg.SetMinValidSources(1)

	rate, err := agg.AggregateWithTime(context.Background(), "XLM-USDC", now)
	if err != nil {
		t.Fatalf("expected graceful fallback to available feed, got error: %v", err)
	}
	if !rate.Equal(decimal.RequireFromString("0.25")) {
		t.Fatalf("rate = %s, want 0.25", rate)
	}
}

func TestOracleAggregator_ProviderInterface(t *testing.T) {
	now := time.Now().UTC()
	p1 := &mockSpotProvider{name: "p1", ticker: Ticker{Price: decimal.RequireFromString("0.20"), Timestamp: now}}
	p2 := &mockSpotProvider{name: "p2", ticker: Ticker{Price: decimal.RequireFromString("0.20"), Timestamp: now}}
	p3 := &mockSpotProvider{name: "p3", ticker: Ticker{Price: decimal.RequireFromString("0.20"), Timestamp: now}}

	agg := NewOracleAggregator([]SpotRateProvider{p1, p2, p3}, []string{"XLM-USDC"})
	rate, err := agg.GetRate(context.Background(), "XLM", "USDC", "1")
	if err != nil {
		t.Fatalf("GetRate: %v", err)
	}
	if !rate.Equal(decimal.RequireFromString("0.20")) {
		t.Fatalf("rate = %s, want 0.20", rate)
	}
}

