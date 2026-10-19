package anchor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/tenant"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

var (
	ErrQuoteExpired           = domain.ErrQuoteExpired
	ErrQuoteAlreadyUsed       = domain.ErrQuoteAlreadyUsed
	ErrQuoteOwnershipMismatch = domain.ErrQuoteOwnershipMismatch
	ErrAmountOutOfLimits      = domain.ErrAmountOutOfLimits
	ErrQuoteNotFound          = errors.New("quote not found")
	ErrInvalidContext         = errors.New("context must be 'sep24' or 'sep31'")
	ErrAmountsExclusive       = errors.New("exactly one of sell_amount or buy_amount must be provided")
	ErrAssetPairUnsupported   = errors.New("asset pair not supported for quoting")
)

// RateProvider resolves real-time exchange rates between two assets.
type RateProvider interface {
	GetRate(ctx context.Context, sellAsset, buyAsset string) (decimal.Decimal, error)
}

// PairLimit specifies allowable quote boundaries and fee structures for an asset pair.
type PairLimit struct {
	MinAmount decimal.Decimal
	MaxAmount decimal.Decimal
	FeeBps    int
	FixedFee  decimal.Decimal
}

// SEP38Asset parses and formats SEP-38 asset identification strings.
type SEP38Asset struct {
	Scheme string // "stellar" | "iso4217"
	Code   string // e.g. "USDC", "USD", "XLM"
	Issuer string // Stellar issuer public key, if applicable
}

func ParseSEP38Asset(s string) SEP38Asset {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "stellar:") {
		parts := strings.Split(s, ":")
		if len(parts) == 2 && parts[1] == "native" {
			return SEP38Asset{Scheme: "stellar", Code: "XLM"}
		}
		if len(parts) == 3 {
			return SEP38Asset{Scheme: "stellar", Code: parts[1], Issuer: parts[2]}
		}
		if len(parts) == 2 {
			return SEP38Asset{Scheme: "stellar", Code: parts[1]}
		}
	}
	if strings.HasPrefix(s, "iso4217:") {
		parts := strings.Split(s, ":")
		if len(parts) == 2 {
			return SEP38Asset{Scheme: "iso4217", Code: parts[1]}
		}
	}
	return SEP38Asset{Scheme: "stellar", Code: s}
}

func (a SEP38Asset) String() string {
	if a.Scheme == "iso4217" {
		return fmt.Sprintf("iso4217:%s", a.Code)
	}
	if a.Code == "XLM" || a.Code == "native" {
		return "stellar:native"
	}
	if a.Issuer != "" {
		return fmt.Sprintf("stellar:%s:%s", a.Code, a.Issuer)
	}
	return fmt.Sprintf("stellar:%s", a.Code)
}

func (a SEP38Asset) CleanCode() string {
	if a.Code == "native" {
		return "XLM"
	}
	return a.Code
}

// SEP38Quote represents a firm exchange quote issued for a client.
type SEP38Quote struct {
	ID          string          `json:"id"`
	Context     string          `json:"context"` // "sep24" | "sep31"
	SellAsset   string          `json:"sell_asset"`
	BuyAsset    string          `json:"buy_asset"`
	SellAmount  decimal.Decimal `json:"sell_amount"`
	BuyAmount   decimal.Decimal `json:"buy_amount"`
	Price       decimal.Decimal `json:"price"`
	TotalPrice  decimal.Decimal `json:"total_price"`
	FeeAmount   decimal.Decimal `json:"fee_amount"`
	FeeAsset    string          `json:"fee_asset"`
	ExpiresAt   time.Time       `json:"expires_at"`
	Consumed    bool            `json:"consumed"`
	ConsumedBy  *string         `json:"consumed_by,omitempty"` // Transfer or transaction ID
	ConsumedAt  *time.Time      `json:"consumed_at,omitempty"`
	TenantID    string          `json:"tenant_id,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
}

type SEP38QuoteStore interface {
	Save(ctx context.Context, q *SEP38Quote) error
	Get(ctx context.Context, id string) (*SEP38Quote, error)
	MarkConsumed(ctx context.Context, id, transferID, tenantID string) (*SEP38Quote, error)
}

type MemorySEP38QuoteStore struct {
	mu     sync.RWMutex
	quotes map[string]*SEP38Quote
}

func NewMemorySEP38QuoteStore() *MemorySEP38QuoteStore {
	return &MemorySEP38QuoteStore{quotes: make(map[string]*SEP38Quote)}
}

func (s *MemorySEP38QuoteStore) Save(_ context.Context, q *SEP38Quote) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := *q
	s.quotes[q.ID] = &copied
	return nil
}

func (s *MemorySEP38QuoteStore) Get(_ context.Context, id string) (*SEP38Quote, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q, ok := s.quotes[id]
	if !ok {
		return nil, ErrQuoteNotFound
	}
	copied := *q
	return &copied, nil
}

func (s *MemorySEP38QuoteStore) MarkConsumed(_ context.Context, id, transferID, tenantID string) (*SEP38Quote, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	q, ok := s.quotes[id]
	if !ok {
		return nil, ErrQuoteNotFound
	}

	if q.Consumed {
		return nil, ErrQuoteAlreadyUsed
	}

	if !q.ExpiresAt.IsZero() && time.Now().UTC().After(q.ExpiresAt) {
		return nil, ErrQuoteExpired
	}

	if tenantID != "" && q.TenantID != "" && q.TenantID != tenantID {
		return nil, ErrQuoteOwnershipMismatch
	}

	now := time.Now().UTC()
	q.Consumed = true
	q.ConsumedBy = &transferID
	q.ConsumedAt = &now

	copied := *q
	return &copied, nil
}

// SEP38ServiceConfig configures the SEP-38 pricing and quote engine.
type SEP38ServiceConfig struct {
	RateProvider RateProvider
	QuoteStore   SEP38QuoteStore
	QuoteTTL     time.Duration // Default 60 seconds
	PairLimits   map[string]PairLimit
}

// SEP38Service implements SEP-38 RFQ & Firm Quote Anchor Pricing Service.
type SEP38Service struct {
	rateProvider RateProvider
	quoteStore   SEP38QuoteStore
	quoteTTL     time.Duration
	pairLimits   map[string]PairLimit
	mu           sync.RWMutex
}

func NewSEP38Service(cfg SEP38ServiceConfig) *SEP38Service {
	quoteTTL := cfg.QuoteTTL
	if quoteTTL <= 0 {
		quoteTTL = 60 * time.Second
	}

	store := cfg.QuoteStore
	if store == nil {
		store = NewMemorySEP38QuoteStore()
	}

	limits := cfg.PairLimits
	if limits == nil {
		limits = defaultPairLimits()
	}

	return &SEP38Service{
		rateProvider: cfg.RateProvider,
		quoteStore:   store,
		quoteTTL:     quoteTTL,
		pairLimits:   limits,
	}
}

func defaultPairLimits() map[string]PairLimit {
	defaultLimit := PairLimit{
		MinAmount: decimal.RequireFromString("0.1"),
		MaxAmount: decimal.RequireFromString("500000.0"),
		FeeBps:    10, // 0.1%
		FixedFee:  decimal.Zero,
	}

	return map[string]PairLimit{
		"XLM-USDC": {MinAmount: decimal.RequireFromString("1.0"), MaxAmount: decimal.RequireFromString("1000000.0"), FeeBps: 15},
		"USDC-XLM": {MinAmount: decimal.RequireFromString("0.1"), MaxAmount: decimal.RequireFromString("100000.0"), FeeBps: 15},
		"USDC-EURC": defaultLimit,
		"EURC-USDC": defaultLimit,
		"USD-USDC":  defaultLimit,
		"USDC-USD":  defaultLimit,
		"NGN-USDC":  {MinAmount: decimal.RequireFromString("1000.0"), MaxAmount: decimal.RequireFromString("50000000.0"), FeeBps: 25},
		"USDC-NGN":  {MinAmount: decimal.RequireFromString("1.0"), MaxAmount: decimal.RequireFromString("50000.0"), FeeBps: 25},
	}
}

func (s *SEP38Service) pairKey(sell, buy string) string {
	return fmt.Sprintf("%s-%s", strings.ToUpper(sell), strings.ToUpper(buy))
}

func (s *SEP38Service) getLimit(sell, buy string) PairLimit {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit, ok := s.pairLimits[s.pairKey(sell, buy)]; ok {
		return limit
	}
	return PairLimit{
		MinAmount: decimal.RequireFromString("0.0000001"),
		MaxAmount: decimal.RequireFromString("10000000.0"),
		FeeBps:    10,
	}
}

// SetPairLimit configures operational limits and fee for an asset pair.
func (s *SEP38Service) SetPairLimit(sell, buy string, limit PairLimit) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pairLimits[s.pairKey(sell, buy)] = limit
}

type SEP38PriceRequest struct {
	Context    string           `json:"context"` // "sep24" | "sep31"
	SellAsset  string           `json:"sell_asset"`
	BuyAsset   string           `json:"buy_asset"`
	SellAmount *decimal.Decimal `json:"sell_amount,omitempty"`
	BuyAmount  *decimal.Decimal `json:"buy_amount,omitempty"`
}

type SEP38Fee struct {
	Total string `json:"total"`
	Asset string `json:"asset"`
}

type SEP38PriceResponse struct {
	TotalPrice string   `json:"total_price"`
	Price      string   `json:"price"`
	SellAmount string   `json:"sell_amount"`
	BuyAmount  string   `json:"buy_amount"`
	Fee        SEP38Fee `json:"fee"`
}

type SEP38PricesItem struct {
	Asset    string `json:"asset"`
	Price    string `json:"price"`
	Decimals int    `json:"decimals"`
}

type SEP38PricesResponse struct {
	BuyAssets []SEP38PricesItem `json:"buy_assets"`
}

type SEP38QuoteRequest struct {
	Context     string           `json:"context"`
	SellAsset   string           `json:"sell_asset"`
	BuyAsset    string           `json:"buy_asset"`
	SellAmount  *decimal.Decimal `json:"sell_amount,omitempty"`
	BuyAmount   *decimal.Decimal `json:"buy_amount,omitempty"`
	ExpireAfter string           `json:"expire_after,omitempty"`
}

type SEP38QuoteResponse struct {
	ID         string   `json:"id"`
	ExpiresAt  string   `json:"expires_at"`
	TotalPrice string   `json:"total_price"`
	Price      string   `json:"price"`
	SellAsset  string   `json:"sell_asset"`
	SellAmount string   `json:"sell_amount"`
	BuyAsset   string   `json:"buy_asset"`
	BuyAmount  string   `json:"buy_amount"`
	Fee        SEP38Fee `json:"fee"`
}

// GetPrices calculates indicative prices for all supported buy assets for a given sell asset.
func (s *SEP38Service) GetPrices(ctx context.Context, sellAssetStr string, sellAmount *decimal.Decimal) (*SEP38PricesResponse, error) {
	sellAsset := ParseSEP38Asset(sellAssetStr)
	sellCode := sellAsset.CleanCode()

	var defaultTargets []string
	switch sellCode {
	case "USDC":
		defaultTargets = []string{"XLM", "EURC", "NGN", "USD"}
	case "XLM":
		defaultTargets = []string{"USDC", "EURC"}
	case "EURC":
		defaultTargets = []string{"USDC", "XLM"}
	default:
		defaultTargets = []string{"USDC", "XLM"}
	}

	buyAssets := make([]SEP38PricesItem, 0, len(defaultTargets))
	for _, targetCode := range defaultTargets {
		if strings.EqualFold(sellCode, targetCode) {
			continue
		}
		rate, err := s.rateProvider.GetRate(ctx, sellCode, targetCode)
		if err != nil || rate.Sign() <= 0 {
			continue
		}
		buyAssets = append(buyAssets, SEP38PricesItem{
			Asset:    ParseSEP38Asset(targetCode).String(),
			Price:    rate.StringFixed(7),
			Decimals: 7,
		})
	}

	return &SEP38PricesResponse{BuyAssets: buyAssets}, nil
}

// CalculatePrice resolves price calculation with strict price inversion and limits enforcement.
func (s *SEP38Service) CalculatePrice(ctx context.Context, req SEP38PriceRequest) (*SEP38PriceResponse, error) {
	if req.Context != "sep24" && req.Context != "sep31" {
		return nil, ErrInvalidContext
	}
	if (req.SellAmount == nil && req.BuyAmount == nil) || (req.SellAmount != nil && req.BuyAmount != nil) {
		return nil, ErrAmountsExclusive
	}

	sellAsset := ParseSEP38Asset(req.SellAsset)
	buyAsset := ParseSEP38Asset(req.BuyAsset)
	sellCode := sellAsset.CleanCode()
	buyCode := buyAsset.CleanCode()

	if strings.EqualFold(sellCode, buyCode) {
		return nil, domain.ErrInvalidAsset
	}

	rate, err := s.rateProvider.GetRate(ctx, sellCode, buyCode)
	if err != nil {
		return nil, fmt.Errorf("fetch exchange rate %s->%s: %w", sellCode, buyCode, err)
	}
	if rate.Sign() <= 0 {
		return nil, errors.New("exchange rate must be positive")
	}

	limit := s.getLimit(sellCode, buyCode)

	var sellAmt, buyAmt, feeAmt decimal.Decimal

	if req.SellAmount != nil {
		sellAmt = *req.SellAmount
		if sellAmt.Sign() <= 0 {
			return nil, domain.ErrInvalidQuoteAmount
		}
		if sellAmt.LessThan(limit.MinAmount) || sellAmt.GreaterThan(limit.MaxAmount) {
			return nil, fmt.Errorf("%w: sell amount %s outside limits [%s, %s]", ErrAmountOutOfLimits, sellAmt, limit.MinAmount, limit.MaxAmount)
		}

		// Calculate fee on sell amount
		feeAmt = sellAmt.Mul(decimal.NewFromInt(int64(limit.FeeBps))).Div(decimal.NewFromInt(10000)).Add(limit.FixedFee)
		netSell := sellAmt.Sub(feeAmt)
		if netSell.Sign() <= 0 {
			return nil, errors.New("fee exceeds gross sell amount")
		}
		buyAmt = netSell.Mul(rate)
	} else {
		// Price Inversion: client specifies buy_amount, anchor computes required sell_amount
		buyAmt = *req.BuyAmount
		if buyAmt.Sign() <= 0 {
			return nil, domain.ErrInvalidQuoteAmount
		}

		// netSell = buyAmt / rate
		netSell := buyAmt.DivRound(rate, 7)
		// sellAmt = (netSell + FixedFee) / (1 - FeeBps/10000)
		oneMinusFeeRatio := decimal.NewFromInt(1).Sub(decimal.NewFromInt(int64(limit.FeeBps)).Div(decimal.NewFromInt(10000)))
		sellAmt = netSell.Add(limit.FixedFee).DivRound(oneMinusFeeRatio, 7)
		feeAmt = sellAmt.Sub(netSell)

		if sellAmt.LessThan(limit.MinAmount) || sellAmt.GreaterThan(limit.MaxAmount) {
			return nil, fmt.Errorf("%w: calculated sell amount %s outside limits [%s, %s]", ErrAmountOutOfLimits, sellAmt, limit.MinAmount, limit.MaxAmount)
		}
	}

	totalPrice := buyAmt.DivRound(sellAmt, 7)

	return &SEP38PriceResponse{
		TotalPrice: totalPrice.StringFixed(7),
		Price:      rate.StringFixed(7),
		SellAmount: sellAmt.StringFixed(7),
		BuyAmount:  buyAmt.StringFixed(7),
		Fee: SEP38Fee{
			Total: feeAmt.StringFixed(7),
			Asset: sellAsset.String(),
		},
	}, nil
}

// RequestQuote issues a firm quote with a configurable expiration timestamp (default 60s).
func (s *SEP38Service) RequestQuote(ctx context.Context, req SEP38QuoteRequest) (*SEP38QuoteResponse, error) {
	priceResp, err := s.CalculatePrice(ctx, SEP38PriceRequest{
		Context:    req.Context,
		SellAsset:  req.SellAsset,
		BuyAsset:   req.BuyAsset,
		SellAmount: req.SellAmount,
		BuyAmount:  req.BuyAmount,
	})
	if err != nil {
		return nil, err
	}

	sellAmt, _ := decimal.NewFromString(priceResp.SellAmount)
	buyAmt, _ := decimal.NewFromString(priceResp.BuyAmount)
	price, _ := decimal.NewFromString(priceResp.Price)
	totalPrice, _ := decimal.NewFromString(priceResp.TotalPrice)
	feeAmt, _ := decimal.NewFromString(priceResp.Fee.Total)

	quoteTTL := s.quoteTTL
	if req.ExpireAfter != "" {
		if d, err := time.ParseDuration(req.ExpireAfter); err == nil && d > 0 && d <= 300*time.Second {
			quoteTTL = d
		}
	}

	now := time.Now().UTC()
	expiresAt := now.Add(quoteTTL)
	tenantID := tenant.IDFromContext(ctx)

	quote := &SEP38Quote{
		ID:          uuid.New().String(),
		Context:     req.Context,
		SellAsset:   req.SellAsset,
		BuyAsset:    req.BuyAsset,
		SellAmount:  sellAmt,
		BuyAmount:   buyAmt,
		Price:       price,
		TotalPrice:  totalPrice,
		FeeAmount:   feeAmt,
		FeeAsset:    priceResp.Fee.Asset,
		ExpiresAt:   expiresAt,
		Consumed:    false,
		TenantID:    tenantID,
		CreatedAt:   now,
	}

	if err := s.quoteStore.Save(ctx, quote); err != nil {
		return nil, fmt.Errorf("save firm quote: %w", err)
	}

	return &SEP38QuoteResponse{
		ID:         quote.ID,
		ExpiresAt:  expiresAt.Format(time.RFC3339),
		TotalPrice: priceResp.TotalPrice,
		Price:      priceResp.Price,
		SellAsset:  quote.SellAsset,
		SellAmount: priceResp.SellAmount,
		BuyAsset:   quote.BuyAsset,
		BuyAmount:  priceResp.BuyAmount,
		Fee:        priceResp.Fee,
	}, nil
}

// BindQuoteToTransfer atomically binds and consumes a firm quote for a subsequent SEP-24 / SEP-31 transfer.
func (s *SEP38Service) BindQuoteToTransfer(ctx context.Context, quoteID, transferID, expectedTenantID string) (*SEP38Quote, error) {
	return s.quoteStore.MarkConsumed(ctx, quoteID, transferID, expectedTenantID)
}

// GetQuote retrieves an issued firm quote by its ID.
func (s *SEP38Service) GetQuote(ctx context.Context, id string) (*SEP38Quote, error) {
	return s.quoteStore.Get(ctx, id)
}

// -------------------------------------------------------------------------
// SEP-38 HTTP Handler
// -------------------------------------------------------------------------

type SEP38Handler struct {
	svc *SEP38Service
}

func NewSEP38Handler(svc *SEP38Service) *SEP38Handler {
	return &SEP38Handler{svc: svc}
}

func (h *SEP38Handler) GetPrices(w http.ResponseWriter, r *http.Request) {
	sellAsset := r.URL.Query().Get("sell_asset")
	if sellAsset == "" {
		http.Error(w, "sell_asset is required", http.StatusBadRequest)
		return
	}

	var sellAmount *decimal.Decimal
	if amtStr := r.URL.Query().Get("sell_amount"); amtStr != "" {
		if amt, err := decimal.NewFromString(amtStr); err == nil {
			sellAmount = &amt
		}
	}

	resp, err := h.svc.GetPrices(r.Context(), sellAsset, sellAmount)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *SEP38Handler) GetPrice(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	contextStr := q.Get("context")
	sellAsset := q.Get("sell_asset")
	buyAsset := q.Get("buy_asset")
	sellAmountStr := q.Get("sell_amount")
	buyAmountStr := q.Get("buy_amount")

	var sellAmt, buyAmt *decimal.Decimal
	if sellAmountStr != "" {
		if amt, err := decimal.NewFromString(sellAmountStr); err == nil {
			sellAmt = &amt
		}
	}
	if buyAmountStr != "" {
		if amt, err := decimal.NewFromString(buyAmountStr); err == nil {
			buyAmt = &amt
		}
	}

	resp, err := h.svc.CalculatePrice(r.Context(), SEP38PriceRequest{
		Context:    contextStr,
		SellAsset:  sellAsset,
		BuyAsset:   buyAsset,
		SellAmount: sellAmt,
		BuyAmount:  buyAmt,
	})
	if err != nil {
		if errors.Is(err, ErrAmountOutOfLimits) || errors.Is(err, ErrAmountsExclusive) || errors.Is(err, ErrInvalidContext) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *SEP38Handler) PostQuote(w http.ResponseWriter, r *http.Request) {
	var req SEP38QuoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	resp, err := h.svc.RequestQuote(r.Context(), req)
	if err != nil {
		if errors.Is(err, ErrAmountOutOfLimits) || errors.Is(err, ErrAmountsExclusive) || errors.Is(err, ErrInvalidContext) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}
