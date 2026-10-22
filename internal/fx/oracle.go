package fx

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

const (
	defaultCoinGeckoURL = "https://api.coingecko.com/api/v3/simple/price"
	oracleHTTPTimeout   = 8 * time.Second
)

// CoinGeckoProvider is an oracle fallback that prices XLM via CoinGecko
// when Stellar DEX order-book liquidity is unavailable.
type CoinGeckoProvider struct {
	baseURL string
	client  *http.Client
	pairs   []string
}

// NewCoinGeckoProvider creates an oracle provider for the given XLM pairs.
// pairs should use "FROM-TO" form (e.g. "XLM-USDC"). When empty, DefaultXLMFXPairs is used.
func NewCoinGeckoProvider(pairs []string) *CoinGeckoProvider {
	if len(pairs) == 0 {
		pairs = DefaultXLMFXPairs()
	}
	return &CoinGeckoProvider{
		baseURL: defaultCoinGeckoURL,
		client:  &http.Client{Timeout: oracleHTTPTimeout},
		pairs:   append([]string(nil), pairs...),
	}
}

// NewCoinGeckoProviderWithURL is used by tests to inject a mock base URL.
func NewCoinGeckoProviderWithURL(baseURL string, pairs []string) *CoinGeckoProvider {
	p := NewCoinGeckoProvider(pairs)
	p.baseURL = strings.TrimRight(baseURL, "/")
	return p
}

func (p *CoinGeckoProvider) SupportedPairs() []string {
	return p.pairs
}

// GetRate returns units of `to` per one unit of `from`, derived from CoinGecko
// XLM/USD and XLM/EUR spot prices (USDC≈USD, EURC≈EUR).
func (p *CoinGeckoProvider) GetRate(ctx context.Context, from, to, _ string) (decimal.Decimal, error) {
	from = normalizeAsset(from)
	to = normalizeAsset(to)
	pairKey := from + "-" + to
	supported := false
	for _, pair := range p.pairs {
		if pair == pairKey {
			supported = true
			break
		}
	}
	if !supported {
		return decimal.Zero, fmt.Errorf("oracle unsupported pair %s", pairKey)
	}

	prices, err := p.fetchPrices(ctx)
	if err != nil {
		return decimal.Zero, err
	}

	rate, err := rateFromOraclePrices(from, to, prices)
	if err != nil {
		return decimal.Zero, err
	}
	return rate, nil
}

type coinGeckoPriceResponse map[string]map[string]float64

func (p *CoinGeckoProvider) fetchPrices(ctx context.Context) (map[string]decimal.Decimal, error) {
	url := p.baseURL
	if !strings.Contains(url, "?") {
		url = fmt.Sprintf("%s?ids=stellar&vs_currencies=usd,eur", p.baseURL)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("oracle request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "nexora-fx-oracle/1.0")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("oracle fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("oracle status %d", resp.StatusCode)
	}

	var body coinGeckoPriceResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("oracle decode: %w", err)
	}
	stellar, ok := body["stellar"]
	if !ok {
		return nil, fmt.Errorf("oracle missing stellar price")
	}

	out := make(map[string]decimal.Decimal, 2)
	if usd, ok := stellar["usd"]; ok && usd > 0 {
		out["USD"] = decimal.NewFromFloat(usd)
	}
	if eur, ok := stellar["eur"]; ok && eur > 0 {
		out["EUR"] = decimal.NewFromFloat(eur)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("oracle returned no usable XLM prices")
	}
	return out, nil
}

// rateFromOraclePrices maps XLM↔USDC/EURC using USD/EUR spot (stablecoin peg).
func rateFromOraclePrices(from, to string, prices map[string]decimal.Decimal) (decimal.Decimal, error) {
	switch {
	case from == "XLM" && to == "USDC":
		p, ok := prices["USD"]
		if !ok {
			return decimal.Zero, fmt.Errorf("oracle missing XLM/USD")
		}
		return p, nil
	case from == "USDC" && to == "XLM":
		p, ok := prices["USD"]
		if !ok {
			return decimal.Zero, fmt.Errorf("oracle missing XLM/USD")
		}
		return decimal.NewFromInt(1).Div(p), nil
	case from == "XLM" && to == "EURC":
		p, ok := prices["EUR"]
		if !ok {
			return decimal.Zero, fmt.Errorf("oracle missing XLM/EUR")
		}
		return p, nil
	case from == "EURC" && to == "XLM":
		p, ok := prices["EUR"]
		if !ok {
			return decimal.Zero, fmt.Errorf("oracle missing XLM/EUR")
		}
		return decimal.NewFromInt(1).Div(p), nil
	default:
		return decimal.Zero, fmt.Errorf("oracle cannot price %s-%s", from, to)
	}
}

// Ticker represents a spot price ticker from an independent feed provider.
type Ticker struct {
	Source    string
	Pair      string
	Price     decimal.Decimal
	Volume    decimal.Decimal
	Timestamp time.Time
}

// SpotRateProvider fetches spot ticker data for currency pairs.
type SpotRateProvider interface {
	Name() string
	FetchTicker(ctx context.Context, pair string) (Ticker, error)
}

// BinanceSpotProvider fetches spot rates from Binance public ticker API.
type BinanceSpotProvider struct {
	baseURL string
	client  *http.Client
}

// NewBinanceSpotProvider creates a new Binance spot rate provider.
func NewBinanceSpotProvider(baseURL string) *BinanceSpotProvider {
	if baseURL == "" {
		baseURL = "https://api.binance.com"
	}
	return &BinanceSpotProvider{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: oracleHTTPTimeout},
	}
}

func (b *BinanceSpotProvider) Name() string {
	return "binance"
}

func (b *BinanceSpotProvider) FetchTicker(ctx context.Context, pair string) (Ticker, error) {
	symbol := binanceSymbol(pair)
	if symbol == "" {
		return Ticker{}, fmt.Errorf("binance: unsupported pair %s", pair)
	}
	url := fmt.Sprintf("%s/api/v3/ticker/24hr?symbol=%s", b.baseURL, symbol)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Ticker{}, fmt.Errorf("binance request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return Ticker{}, fmt.Errorf("binance fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Ticker{}, fmt.Errorf("binance status %d", resp.StatusCode)
	}

	var payload struct {
		LastPrice string `json:"lastPrice"`
		Volume    string `json:"volume"`
		CloseTime int64  `json:"closeTime"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return Ticker{}, fmt.Errorf("binance decode: %w", err)
	}

	price, err := decimal.NewFromString(payload.LastPrice)
	if err != nil {
		return Ticker{}, fmt.Errorf("binance parse price: %w", err)
	}
	volume, err := decimal.NewFromString(payload.Volume)
	if err != nil {
		volume = decimal.Zero
	}

	ts := time.Now().UTC()
	if payload.CloseTime > 0 {
		ts = time.UnixMilli(payload.CloseTime).UTC()
	}

	return Ticker{
		Source:    b.Name(),
		Pair:      pair,
		Price:     price,
		Volume:    volume,
		Timestamp: ts,
	}, nil
}

func binanceSymbol(pair string) string {
	parts := strings.Split(pair, "-")
	if len(parts) != 2 {
		return ""
	}
	from, to := parts[0], parts[1]
	if to == "USDC" || to == "USD" {
		to = "USDT"
	}
	return from + to
}

// KrakenSpotProvider fetches spot rates from Kraken public ticker API.
type KrakenSpotProvider struct {
	baseURL string
	client  *http.Client
}

// NewKrakenSpotProvider creates a new Kraken spot rate provider.
func NewKrakenSpotProvider(baseURL string) *KrakenSpotProvider {
	if baseURL == "" {
		baseURL = "https://api.kraken.com"
	}
	return &KrakenSpotProvider{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: oracleHTTPTimeout},
	}
}

func (k *KrakenSpotProvider) Name() string {
	return "kraken"
}

func (k *KrakenSpotProvider) FetchTicker(ctx context.Context, pair string) (Ticker, error) {
	krakenPair := krakenPairCode(pair)
	if krakenPair == "" {
		return Ticker{}, fmt.Errorf("kraken: unsupported pair %s", pair)
	}
	url := fmt.Sprintf("%s/0/public/Ticker?pair=%s", k.baseURL, krakenPair)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Ticker{}, fmt.Errorf("kraken request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := k.client.Do(req)
	if err != nil {
		return Ticker{}, fmt.Errorf("kraken fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Ticker{}, fmt.Errorf("kraken status %d", resp.StatusCode)
	}

	var payload struct {
		Error  []string                          `json:"error"`
		Result map[string]map[string]interface{} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return Ticker{}, fmt.Errorf("kraken decode: %w", err)
	}
	if len(payload.Error) > 0 {
		return Ticker{}, fmt.Errorf("kraken error: %s", strings.Join(payload.Error, ", "))
	}

	for _, data := range payload.Result {
		cArr, ok := data["c"].([]interface{})
		if !ok || len(cArr) == 0 {
			continue
		}
		priceStr, ok := cArr[0].(string)
		if !ok {
			continue
		}
		price, err := decimal.NewFromString(priceStr)
		if err != nil {
			continue
		}
		vol := decimal.Zero
		if vArr, ok := data["v"].([]interface{}); ok && len(vArr) > 1 {
			if vStr, ok := vArr[1].(string); ok {
				vol, _ = decimal.NewFromString(vStr)
			}
		}

		return Ticker{
			Source:    k.Name(),
			Pair:      pair,
			Price:     price,
			Volume:    vol,
			Timestamp: time.Now().UTC(),
		}, nil
	}

	return Ticker{}, fmt.Errorf("kraken: missing price for %s", pair)
}

func krakenPairCode(pair string) string {
	parts := strings.Split(pair, "-")
	if len(parts) != 2 {
		return ""
	}
	from, to := parts[0], parts[1]
	if from == "XLM" {
		from = "XXLM"
	}
	if to == "USD" || to == "USDC" {
		to = "ZUSD"
	} else if to == "EUR" || to == "EURC" {
		to = "ZEUR"
	}
	return from + to
}

// StellarDEXSpotProvider queries Stellar DEX orderbook depth and price.
type StellarDEXSpotProvider struct {
	horizonURL string
	issuer     string
	client     *http.Client
}

// NewStellarDEXSpotProvider creates a new Stellar DEX spot provider.
func NewStellarDEXSpotProvider(horizonURL string) *StellarDEXSpotProvider {
	return NewStellarDEXSpotProviderWithIssuer(horizonURL, "")
}

// NewStellarDEXSpotProviderWithIssuer creates a Stellar DEX spot provider with a specified asset issuer.
func NewStellarDEXSpotProviderWithIssuer(horizonURL, issuer string) *StellarDEXSpotProvider {
	if horizonURL == "" {
		horizonURL = "https://horizon.stellar.org"
	}
	return &StellarDEXSpotProvider{
		horizonURL: strings.TrimRight(horizonURL, "/"),
		issuer:     issuer,
		client:     &http.Client{Timeout: oracleHTTPTimeout},
	}
}

func (s *StellarDEXSpotProvider) Name() string {
	return "stellar_dex"
}

func (s *StellarDEXSpotProvider) FetchTicker(ctx context.Context, pair string) (Ticker, error) {
	parts := strings.Split(pair, "-")
	if len(parts) != 2 {
		return Ticker{}, fmt.Errorf("stellar_dex: invalid pair format %s", pair)
	}
	from, to := parts[0], parts[1]

	var query string
	if from == "XLM" {
		assetType := "credit_alphanum4"
		if len(to) > 4 {
			assetType = "credit_alphanum12"
		}
		query = fmt.Sprintf("selling_asset_type=native&buying_asset_type=%s&buying_asset_code=%s", assetType, to)
		if s.issuer != "" {
			query += fmt.Sprintf("&buying_asset_issuer=%s", s.issuer)
		}
	} else if to == "XLM" {
		assetType := "credit_alphanum4"
		if len(from) > 4 {
			assetType = "credit_alphanum12"
		}
		query = fmt.Sprintf("selling_asset_type=%s&selling_asset_code=%s&buying_asset_type=native", assetType, from)
		if s.issuer != "" {
			query += fmt.Sprintf("&selling_asset_issuer=%s", s.issuer)
		}
	} else {
		// Non-native pair
		fromType := "credit_alphanum4"
		if len(from) > 4 {
			fromType = "credit_alphanum12"
		}
		toType := "credit_alphanum4"
		if len(to) > 4 {
			toType = "credit_alphanum12"
		}
		query = fmt.Sprintf("selling_asset_type=%s&selling_asset_code=%s&buying_asset_type=%s&buying_asset_code=%s", fromType, from, toType, to)
		if s.issuer != "" {
			query += fmt.Sprintf("&buying_asset_issuer=%s", s.issuer)
		}
	}

	url := fmt.Sprintf("%s/order_book?%s", s.horizonURL, query)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Ticker{}, fmt.Errorf("stellar_dex request: %w", err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return Ticker{}, fmt.Errorf("stellar_dex fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Ticker{}, fmt.Errorf("stellar_dex status %d", resp.StatusCode)
	}

	var book struct {
		Asks []struct {
			Amount string `json:"amount"`
			Price  string `json:"price"`
		} `json:"asks"`
		Bids []struct {
			Amount string `json:"amount"`
			Price  string `json:"price"`
		} `json:"bids"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&book); err != nil {
		return Ticker{}, fmt.Errorf("stellar_dex decode: %w", err)
	}
	if len(book.Asks) == 0 && len(book.Bids) == 0 {
		return Ticker{}, fmt.Errorf("stellar_dex: empty orderbook for %s", pair)
	}

	var price decimal.Decimal
	vol := decimal.Zero
	if len(book.Asks) > 0 {
		bestAsk, err := decimal.NewFromString(book.Asks[0].Price)
		if err != nil {
			return Ticker{}, fmt.Errorf("stellar_dex parse price: %w", err)
		}
		price = bestAsk
		for _, ask := range book.Asks {
			if amt, err := decimal.NewFromString(ask.Amount); err == nil {
				vol = vol.Add(amt)
			}
		}
	} else {
		bestBid, err := decimal.NewFromString(book.Bids[0].Price)
		if err != nil {
			return Ticker{}, fmt.Errorf("stellar_dex parse price: %w", err)
		}
		price = bestBid
		for _, bid := range book.Bids {
			if amt, err := decimal.NewFromString(bid.Amount); err == nil {
				vol = vol.Add(amt)
			}
		}
	}

	return Ticker{
		Source:    s.Name(),
		Pair:      pair,
		Price:     price,
		Volume:    vol,
		Timestamp: time.Now().UTC(),
	}, nil
}

// OracleAggregator aggregates spot rates across multiple feeds with trimmed mean outlier filtering.
type OracleAggregator struct {
	providers       []SpotRateProvider
	stalenessGuard  time.Duration
	trimPercent     float64
	minValidSources int
	supportedPairs  []string
}

// NewOracleAggregator creates a new high-frequency FX rate aggregator.
func NewOracleAggregator(providers []SpotRateProvider, supportedPairs []string) *OracleAggregator {
	if len(supportedPairs) == 0 {
		supportedPairs = DefaultXLMFXPairs()
	}
	return &OracleAggregator{
		providers:       providers,
		stalenessGuard:  15 * time.Second,
		trimPercent:     0.10,
		minValidSources: 1,
		supportedPairs:  supportedPairs,
	}
}

// SetStalenessGuard configures maximum acceptable age for price updates.
func (a *OracleAggregator) SetStalenessGuard(d time.Duration) {
	a.stalenessGuard = d
}

// SetTrimPercent sets the percentage of outliers to discard from both ends.
func (a *OracleAggregator) SetTrimPercent(pct float64) {
	a.trimPercent = pct
}

// SetMinValidSources configures the minimum required non-stale sources.
func (a *OracleAggregator) SetMinValidSources(min int) {
	a.minValidSources = min
}

func (a *OracleAggregator) SupportedPairs() []string {
	return a.supportedPairs
}

// GetRate implements Provider interface.
func (a *OracleAggregator) GetRate(ctx context.Context, from, to, _ string) (decimal.Decimal, error) {
	from = normalizeAsset(from)
	to = normalizeAsset(to)
	pair := from + "-" + to
	supported := false
	for _, p := range a.supportedPairs {
		if p == pair {
			supported = true
			break
		}
	}
	if !supported {
		return decimal.Zero, fmt.Errorf("oracle unsupported pair %s", pair)
	}
	return a.Aggregate(ctx, pair)
}

// Aggregate ingests spot rates across all providers, filters outliers, and computes VWAP.
func (a *OracleAggregator) Aggregate(ctx context.Context, pair string) (decimal.Decimal, error) {
	return a.AggregateWithTime(ctx, pair, time.Now().UTC())
}

// AggregateWithTime allows time injection for deterministic testing of staleness.
func (a *OracleAggregator) AggregateWithTime(ctx context.Context, pair string, now time.Time) (decimal.Decimal, error) {
	if len(a.providers) < 3 {
		return decimal.Zero, fmt.Errorf("oracle aggregator requires at least 3 spot rate providers, configured: %d", len(a.providers))
	}

	type result struct {
		ticker Ticker
		err    error
	}

	results := make(chan result, len(a.providers))
	var wg sync.WaitGroup

	for _, p := range a.providers {
		wg.Add(1)
		go func(prov SpotRateProvider) {
			defer wg.Done()
			t, err := prov.FetchTicker(ctx, pair)
			results <- result{ticker: t, err: err}
		}(p)
	}

	wg.Wait()
	close(results)

	var validTickers []Ticker
	for r := range results {
		if r.err != nil {
			continue
		}
		// Staleness guard check: reject older than staleness window
		age := now.Sub(r.ticker.Timestamp)
		if age < 0 {
			age = -age
		}
		if age > a.stalenessGuard {
			continue
		}
		if r.ticker.Price.Sign() <= 0 {
			continue
		}
		validTickers = append(validTickers, r.ticker)
	}

	if len(validTickers) < a.minValidSources {
		return decimal.Zero, fmt.Errorf("insufficient valid non-stale rate feeds for %s: got %d, require at least %d", pair, len(validTickers), a.minValidSources)
	}

	// Filter outliers using Trimmed Mean
	filtered := ApplyTrimmedMean(validTickers, a.trimPercent)
	if len(filtered) == 0 {
		return decimal.Zero, fmt.Errorf("all rate feeds eliminated by outlier filter for %s", pair)
	}

	return CalculateVWAP(filtered), nil
}

// ApplyTrimmedMean sorts tickers and eliminates top and bottom outliers.
func ApplyTrimmedMean(tickers []Ticker, trimPercent float64) []Ticker {
	n := len(tickers)
	if n < 3 {
		return tickers
	}

	sorted := make([]Ticker, n)
	copy(sorted, tickers)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Price.LessThan(sorted[j].Price)
	})

	trimCount := int(math.Floor(float64(n) * trimPercent))

	// For sample sizes where floor is 0 but outlier spikes exist, apply median filtering
	if trimCount == 0 && n >= 3 {
		medianPrice := sorted[n/2].Price
		if medianPrice.Sign() > 0 {
			var nonOutliers []Ticker
			for _, t := range sorted {
				diff := t.Price.Sub(medianPrice).Abs()
				ratio := diff.Div(medianPrice)
				// If deviation exceeds 50% from median, treat as manipulation/spike
				if ratio.GreaterThan(decimal.RequireFromString("0.50")) {
					continue
				}
				nonOutliers = append(nonOutliers, t)
			}
			if len(nonOutliers) >= 1 {
				return nonOutliers
			}
		}
		return sorted
	}

	if 2*trimCount >= n {
		trimCount = (n - 1) / 2
	}

	return sorted[trimCount : n-trimCount]
}

// CalculateVWAP calculates the Volume-Weighted Average Price across tickers.
// Falls back to simple arithmetic mean if total volume is zero.
func CalculateVWAP(tickers []Ticker) decimal.Decimal {
	if len(tickers) == 0 {
		return decimal.Zero
	}

	totalWeighted := decimal.Zero
	totalVolume := decimal.Zero

	for _, t := range tickers {
		if t.Volume.Sign() > 0 {
			weighted := t.Price.Mul(t.Volume)
			totalWeighted = totalWeighted.Add(weighted)
			totalVolume = totalVolume.Add(t.Volume)
		}
	}

	if totalVolume.Sign() > 0 {
		return totalWeighted.Div(totalVolume)
	}

	// Fallback to simple mean
	sum := decimal.Zero
	for _, t := range tickers {
		sum = sum.Add(t.Price)
	}
	return sum.Div(decimal.NewFromInt(int64(len(tickers))))
}

