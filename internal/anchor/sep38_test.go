package anchor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

type mockRateProvider struct {
	rates map[string]decimal.Decimal
}

func newMockRateProvider() *mockRateProvider {
	return &mockRateProvider{
		rates: map[string]decimal.Decimal{
			"USDC-XLM":  decimal.RequireFromString("9.5000000"), // 1 USDC = 9.5 XLM
			"XLM-USDC":  decimal.RequireFromString("0.1052631"), // 1 XLM = 0.1052631 USDC
			"USDC-EURC": decimal.RequireFromString("0.9200000"),
			"EURC-USDC": decimal.RequireFromString("1.0869565"),
			"USDC-NGN":  decimal.RequireFromString("1500.0000000"),
			"NGN-USDC":  decimal.RequireFromString("0.0006666"),
		},
	}
}

func (m *mockRateProvider) GetRate(_ context.Context, sellAsset, buyAsset string) (decimal.Decimal, error) {
	key := sellAsset + "-" + buyAsset
	if rate, ok := m.rates[key]; ok {
		return rate, nil
	}
	return decimal.Zero, errors.New("rate not found")
}

func setupTestSEP38Service() *SEP38Service {
	provider := newMockRateProvider()
	return NewSEP38Service(SEP38ServiceConfig{
		RateProvider: provider,
		QuoteTTL:     60 * time.Second,
	})
}

func TestSEP38Service_GetPrices_CalculatesRealTimeOracleQuotes(t *testing.T) {
	svc := setupTestSEP38Service()

	resp, err := svc.GetPrices(context.Background(), "stellar:USDC:GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5", nil)
	if err != nil {
		t.Fatalf("GetPrices error: %v", err)
	}
	if len(resp.BuyAssets) == 0 {
		t.Fatalf("expected buy assets returned")
	}

	foundXLM := false
	for _, item := range resp.BuyAssets {
		if item.Asset == "stellar:native" {
			foundXLM = true
			if item.Price != "9.5000000" {
				t.Fatalf("expected price 9.5000000 for XLM, got %s", item.Price)
			}
		}
	}
	if !foundXLM {
		t.Fatalf("expected stellar:native in buy assets")
	}
}

func TestSEP38Service_CalculatePrice_PriceInversionAndFees(t *testing.T) {
	svc := setupTestSEP38Service()

	// 1. Direct sell_amount: Sell 100 USDC for XLM
	// Rate = 9.5. FeeBps = 15 (0.15%). FixedFee = 0.
	// Gross Sell = 100. Fee = 100 * 0.0015 = 0.15.
	// Net Sell = 99.85. BuyAmount = 99.85 * 9.5 = 948.575.
	sellAmt := decimal.RequireFromString("100.0")
	priceResp, err := svc.CalculatePrice(context.Background(), SEP38PriceRequest{
		Context:    "sep24",
		SellAsset:  "stellar:USDC:GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
		BuyAsset:   "stellar:native",
		SellAmount: &sellAmt,
	})
	if err != nil {
		t.Fatalf("CalculatePrice error: %v", err)
	}

	if priceResp.Price != "9.5000000" {
		t.Fatalf("expected base price 9.5000000, got %s", priceResp.Price)
	}
	if priceResp.Fee.Total != "0.1500000" {
		t.Fatalf("expected fee 0.1500000, got %s", priceResp.Fee.Total)
	}
	if priceResp.BuyAmount != "948.5750000" {
		t.Fatalf("expected buy amount 948.5750000, got %s", priceResp.BuyAmount)
	}

	// 2. Price Inversion: Client requests exact buy_amount of 948.575 XLM
	// Required net sell = 948.575 / 9.5 = 99.85
	// Gross sell = 99.85 / (1 - 0.0015) = 100.0000000
	buyAmt := decimal.RequireFromString("948.575")
	invResp, err := svc.CalculatePrice(context.Background(), SEP38PriceRequest{
		Context:   "sep31",
		SellAsset: "stellar:USDC:GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
		BuyAsset:  "stellar:native",
		BuyAmount: &buyAmt,
	})
	if err != nil {
		t.Fatalf("CalculatePrice with price inversion error: %v", err)
	}

	if invResp.SellAmount != "100.0000000" {
		t.Fatalf("expected inverted sell amount 100.0000000, got %s", invResp.SellAmount)
	}
	if invResp.Fee.Total != "0.1500000" {
		t.Fatalf("expected inverted fee 0.1500000, got %s", invResp.Fee.Total)
	}
}

func TestSEP38Service_EnforcesMinAndMaxLimits(t *testing.T) {
	svc := setupTestSEP38Service()

	// Limit for USDC-XLM: min = 0.1, max = 100000.0
	// 1. Test below minimum
	tooSmall := decimal.RequireFromString("0.05")
	_, err := svc.CalculatePrice(context.Background(), SEP38PriceRequest{
		Context:    "sep24",
		SellAsset:  "USDC",
		BuyAsset:   "XLM",
		SellAmount: &tooSmall,
	})
	if !errors.Is(err, ErrAmountOutOfLimits) {
		t.Fatalf("expected ErrAmountOutOfLimits for 0.05 USDC, got: %v", err)
	}

	// 2. Test above maximum
	tooLarge := decimal.RequireFromString("200000.0")
	_, err = svc.CalculatePrice(context.Background(), SEP38PriceRequest{
		Context:    "sep24",
		SellAsset:  "USDC",
		BuyAsset:   "XLM",
		SellAmount: &tooLarge,
	})
	if !errors.Is(err, ErrAmountOutOfLimits) {
		t.Fatalf("expected ErrAmountOutOfLimits for 200000 USDC, got: %v", err)
	}
}

func TestSEP38Service_FirmQuoteLifecycleAndAtomicBinding(t *testing.T) {
	svc := setupTestSEP38Service()

	sellAmt := decimal.RequireFromString("50.0")
	quoteResp, err := svc.RequestQuote(context.Background(), SEP38QuoteRequest{
		Context:    "sep24",
		SellAsset:  "USDC",
		BuyAsset:   "XLM",
		SellAmount: &sellAmt,
	})
	if err != nil {
		t.Fatalf("RequestQuote error: %v", err)
	}
	if quoteResp.ID == "" {
		t.Fatalf("expected firm quote id")
	}

	// 1. Fetch quote by ID
	quote, err := svc.GetQuote(context.Background(), quoteResp.ID)
	if err != nil {
		t.Fatalf("GetQuote error: %v", err)
	}
	if quote.Consumed {
		t.Fatalf("new quote should not be consumed")
	}
	if quote.ExpiresAt.IsZero() {
		t.Fatalf("quote must have expiration timestamp")
	}

	// 2. Atomically bind firm quote to subsequent SEP-24 transfer
	transferID := "sep24-transfer-uuid-1"
	boundQuote, err := svc.BindQuoteToTransfer(context.Background(), quoteResp.ID, transferID, "")
	if err != nil {
		t.Fatalf("BindQuoteToTransfer error: %v", err)
	}
	if !boundQuote.Consumed {
		t.Fatalf("bound quote must be marked consumed")
	}
	if boundQuote.ConsumedBy == nil || *boundQuote.ConsumedBy != transferID {
		t.Fatalf("expected consumed_by %s, got %v", transferID, boundQuote.ConsumedBy)
	}

	// 3. Second binding attempt must be rejected (QUOTE_ALREADY_USED)
	_, err = svc.BindQuoteToTransfer(context.Background(), quoteResp.ID, "transfer-2", "")
	if !errors.Is(err, ErrQuoteAlreadyUsed) {
		t.Fatalf("expected ErrQuoteAlreadyUsed on second bind, got: %v", err)
	}
}

func TestSEP38Service_RejectsExpiredQuote(t *testing.T) {
	svc := setupTestSEP38Service()

	// Create quote that expires almost immediately
	sellAmt := decimal.RequireFromString("10.0")
	quoteResp, err := svc.RequestQuote(context.Background(), SEP38QuoteRequest{
		Context:     "sep24",
		SellAsset:   "USDC",
		BuyAsset:    "XLM",
		SellAmount:  &sellAmt,
		ExpireAfter: "1ms",
	})
	if err != nil {
		t.Fatalf("RequestQuote error: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	// Attempt to bind expired quote
	_, err = svc.BindQuoteToTransfer(context.Background(), quoteResp.ID, "transfer-expired", "")
	if !errors.Is(err, ErrQuoteExpired) {
		t.Fatalf("expected ErrQuoteExpired for expired quote, got: %v", err)
	}
}

func TestSEP38Handler_HTTPRoutes(t *testing.T) {
	svc := setupTestSEP38Service()
	handler := NewSEP38Handler(svc)

	mux := http.NewServeMux()
	mux.HandleFunc("/prices", handler.GetPrices)
	mux.HandleFunc("/price", handler.GetPrice)
	mux.HandleFunc("/quote", handler.PostQuote)

	srv := httptest.NewServer(mux)
	defer srv.Close()

	// 1. GET /prices?sell_asset=USDC
	pResp, err := http.Get(srv.URL + "/prices?sell_asset=USDC")
	if err != nil {
		t.Fatalf("GET /prices error: %v", err)
	}
	defer pResp.Body.Close()
	if pResp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 from GET /prices, got %d", pResp.StatusCode)
	}

	// 2. GET /price?context=sep24&sell_asset=USDC&buy_asset=XLM&sell_amount=10
	prResp, err := http.Get(srv.URL + "/price?context=sep24&sell_asset=USDC&buy_asset=XLM&sell_amount=10")
	if err != nil {
		t.Fatalf("GET /price error: %v", err)
	}
	defer prResp.Body.Close()
	if prResp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 from GET /price, got %d", prResp.StatusCode)
	}

	// 3. POST /quote
	sellAmount := decimal.RequireFromString("25.0")
	qReq := SEP38QuoteRequest{
		Context:    "sep24",
		SellAsset:  "USDC",
		BuyAsset:   "XLM",
		SellAmount: &sellAmount,
	}
	qBytes, _ := json.Marshal(qReq)
	postResp, err := http.Post(srv.URL+"/quote", "application/json", bytes.NewReader(qBytes))
	if err != nil {
		t.Fatalf("POST /quote error: %v", err)
	}
	defer postResp.Body.Close()
	if postResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected status 201 from POST /quote, got %d", postResp.StatusCode)
	}

	var qOut SEP38QuoteResponse
	_ = json.NewDecoder(postResp.Body).Decode(&qOut)
	if qOut.ID == "" {
		t.Fatalf("expected quote id in response")
	}
	if qOut.Price != "9.5000000" {
		t.Fatalf("expected price 9.5000000, got %s", qOut.Price)
	}
}
