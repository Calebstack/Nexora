package fx

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/fees"
	"github.com/Lumen-Nexora/Nexora/internal/stellar"
	"github.com/Lumen-Nexora/Nexora/internal/tenant"
	"github.com/Lumen-Nexora/Nexora/internal/wallet"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

const (
	quoteTTL          = 30 * time.Second
	quoteKeyPrefix    = "fx:quote:"
	refreshInterval   = 30 * time.Second
	activePairMaxIdle = 10 * time.Minute
)

// FXQuoteAuditRepo persists quote snapshots as an audit trail.
// Redis is the live store; Postgres is the audit log.
type FXQuoteAuditRepo interface {
	CreateQuote(ctx context.Context, q *domain.Quote) error
	MarkQuoteUsed(ctx context.Context, quoteID, conversionID string) error
}

// ConversionRepo persists executed conversions.
type ConversionRepo interface {
	Create(ctx context.Context, c *domain.Conversion) error
}

// Service is the FX domain service interface.
type Service interface {
	GetQuote(ctx context.Context, fromAsset, toAsset, amount string) (*domain.Quote, error)
	ExecuteConversion(ctx context.Context, walletID, quoteID string, minAmountOut *decimal.Decimal, maxSlippageBps *int) (*domain.Conversion, error)
	GetRates(ctx context.Context, from, to string) (*RateResponse, error)
	BindQuoteToTransfer(ctx context.Context, quoteID, transferID, expectedTenantID string) (*domain.Quote, error)
}

type service struct {
	walletRepo     wallet.Repository
	conversionRepo ConversionRepo
	auditRepo      FXQuoteAuditRepo
	feeSvc         fees.Service
	stellar        stellar.Client
	redis          redis.UniversalClient
	rateCache      *RateCache
	volatility     *VolatilityEngine
	triangulation  *TriangulationDetector
	lockRepo       LockRepository
	usdcIssuer     string
	providers      []Provider
	spreadBps      int

	activePairsMu sync.RWMutex
	activePairs   map[string]time.Time
}

// markUsedScript atomically verifies ownership and then claims a quote.
//
// The ownership check runs before the quote is marked used and inside the
// same Lua invocation as the claim, so a caller who presents someone else's
// quote_id can never consume it. ARGV[1] is the caller's tenant ID and
// ARGV[2] is "1" when the wallet has a tenant, "0" otherwise. Every branch
// returns immediately after the single GET, so a miss, a foreign quote and an
// already-used quote are indistinguishable by timing and the ID is not an
// existence oracle.
//
// Returns the original quote JSON on success, or a Redis error on failure.
var markUsedScript = redis.NewScript(`
local data = redis.call('GET', KEYS[1])
if not data then return redis.error_reply('QUOTE_EXPIRED') end
if ARGV[2] ~= '1' then return redis.error_reply('QUOTE_OWNERSHIP_MISMATCH') end
local q = cjson.decode(data)
if q.OrgID ~= ARGV[1] then return redis.error_reply('QUOTE_OWNERSHIP_MISMATCH') end
if q.Used then return redis.error_reply('QUOTE_ALREADY_USED') end
q.Used = true
redis.call('SET', KEYS[1], cjson.encode(q), 'KEEPTTL')
return data
`)

// NewService constructs the FX service and starts the background rate refresh.
func NewService(
	walletRepo wallet.Repository,
	convRepo ConversionRepo,
	auditRepo FXQuoteAuditRepo,
	feeSvc fees.Service,
	stellarClient stellar.Client,
	redisClient redis.UniversalClient,
	usdcIssuer string,
	providers []Provider,
	spreadBps int,
) Service {
	s := &service{
		walletRepo:     walletRepo,
		conversionRepo: convRepo,
		auditRepo:      auditRepo,
		feeSvc:         feeSvc,
		stellar:        stellarClient,
		redis:          redisClient,
		rateCache:      NewRateCache(redisClient),
		volatility:     NewVolatilityEngine(),
		usdcIssuer:     usdcIssuer,
		providers:      providers,
		spreadBps:      spreadBps,
		activePairs:    make(map[string]time.Time),
	}
	go s.backgroundRefresh(context.Background())
	return s
}

// SetTriangulationDetector wires the cross-pair triangulation pricing validator.
func (s *service) SetTriangulationDetector(d *TriangulationDetector) {
	s.triangulation = d
}

// SetLockRepository wires the rate lock repository into the service.
func (s *service) SetLockRepository(repo LockRepository) {
	s.lockRepo = repo
}

// WithLockRepository sets the rate lock repository and returns the service instance.
func (s *service) WithLockRepository(repo LockRepository) *service {
	s.lockRepo = repo
	return s
}

// GetQuote prices a conversion, stores the quote in Redis with a 30-second TTL,
// and writes an audit row to Postgres. Returns the quote with its ID token.
func (s *service) GetQuote(ctx context.Context, fromAsset, toAsset, amount string) (*domain.Quote, error) {
	fromAsset, toAsset, err := validateFXPair(fromAsset, toAsset)
	if err != nil {
		return nil, err
	}

	rateResp, err := s.GetRates(ctx, fromAsset, toAsset)
	if err != nil {
		return nil, err
	}

	if s.triangulation != nil {
		directRate, _ := rateResp.Rate.Float64()
		ratesMap := make(map[string]float64)
		s.activePairsMu.RLock()
		for pair := range s.activePairs {
			parts := strings.Split(pair, ":")
			if len(parts) == 2 {
				if r, ok := s.rateCache.Get(ctx, parts[0], parts[1]); ok {
					if flt, _ := r.Rate.Float64(); flt > 0 {
						ratesMap[parts[0]+"-"+parts[1]] = flt
					}
				}
			}
		}
		s.activePairsMu.RUnlock()

		if res, err := s.triangulation.ValidateQuote(fromAsset, toAsset, directRate, ratesMap); err != nil && res.Halt {
			return nil, err
		}
	}

	fromAmt, err := decimal.NewFromString(amount)
	if err != nil {
		return nil, domain.ErrInvalidAsset
	}
	if fromAmt.Sign() <= 0 {
		return nil, domain.ErrInvalidQuoteAmount
	}
	if err := validateAmountLimits(fromAsset, fromAmt); err != nil {
		return nil, err
	}

	toAmt := fromAmt.Mul(rateResp.Rate)
	tenantID := tenant.IDFromContext(ctx)

	feeAmt := decimal.Zero
	if feeResult, feeErr := s.feeSvc.CalculateConversionFee(ctx, tenantID, fromAsset, fromAmt); feeErr == nil {
		feeAmt = feeResult.FeeAmount
	}

	q := &domain.Quote{
		ID:                    uuid.New().String(),
		OrgID:                 tenantID,
		FromAsset:             fromAsset,
		ToAsset:               toAsset,
		FromAmount:            fromAmt,
		ToAmount:              toAmt,
		Rate:                  rateResp.Rate,
		Fee:                   feeAmt,
		ExpiresAt:             time.Now().UTC().Add(quoteTTL),
		Used:                  false,
		FromRequiresTrustline: AssetRequiresTrustline(fromAsset),
		ToRequiresTrustline:   AssetRequiresTrustline(toAsset),
	}

	data, err := json.Marshal(q)
	if err != nil {
		return nil, fmt.Errorf("marshal quote: %w", err)
	}
	if err := s.redis.Set(ctx, quoteKeyPrefix+q.ID, data, quoteTTL).Err(); err != nil {
		return nil, fmt.Errorf("store quote: %w", err)
	}

	if s.auditRepo != nil {
		_ = s.auditRepo.CreateQuote(ctx, q)
	}

	return q, nil
}

// ExecuteConversion fetches a quote by ID from Redis, validates it has not expired
// or been used, verifies ownership, atomically marks it used, and records the conversion.
func (s *service) ExecuteConversion(ctx context.Context, walletID, quoteID string, minAmountOut *decimal.Decimal, maxSlippageBps *int) (*domain.Conversion, error) {
	w, err := s.walletRepo.GetByID(ctx, walletID)
	if err != nil {
		return nil, err
	}

	// The claim is conditional on ownership: the tenant ID travels into the
	// Lua script so a foreign quote is rejected before it is marked used.
	expectedOrg := ""
	hasTenant := "0"
	if w.TenantID != nil {
		expectedOrg = *w.TenantID
		hasTenant = "1"
	}

	// Check if quoteID corresponds to a bounded rate lock when LockRepository is configured.
	if s.lockRepo != nil {
		if lock, err := s.lockRepo.Get(ctx, quoteID, expectedOrg); err == nil && lock != nil {
			// Enforce tenant isolation
			if w.TenantID == nil || lock.TenantID != *w.TenantID {
				return nil, domain.ErrQuoteOwnershipMismatch
			}

			// Sub-second precision expiration: even 1 millisecond after deadline returns QUOTE_EXPIRED
			now := time.Now().UTC().Truncate(time.Microsecond)
			if !now.Before(lock.ExpiresAt.UTC().Truncate(time.Microsecond)) {
				return nil, domain.ErrQuoteExpired
			}

			// Issue #210: Rate locks require min_amount_out to protect callers against slippage
			if minAmountOut == nil {
				return nil, fmt.Errorf("%w: min_amount_out is required for rate lock conversions", domain.ErrSlippageExceeded)
			}

			destAmt := lock.Amount.Mul(lock.LockedRate)
			if destAmt.LessThan(*minAmountOut) {
				return nil, fmt.Errorf("%w: destination amount %s is less than minimum required %s", domain.ErrSlippageExceeded, destAmt.String(), minAmountOut.String())
			}

			convID := uuid.New().String()
			if err := s.lockRepo.ConsumeLock(ctx, lock.ID, convID); err != nil {
				return nil, err
			}

			conv := &domain.Conversion{
				ID:             convID,
				WalletID:       walletID,
				SourceAsset:    lock.FromAsset,
				DestAsset:      lock.ToAsset,
				SourceAmount:   lock.Amount,
				DestAmount:     destAmt,
				Rate:           lock.LockedRate,
				MinAmountOut:   minAmountOut,
				MaxSlippageBps: maxSlippageBps,
				RateLockID:     &lock.ID,
				CreatedAt:      now,
			}
			if err := s.conversionRepo.Create(ctx, conv); err != nil {
				return nil, fmt.Errorf("persist conversion: %w", err)
			}

			return conv, nil
		}
	}

	result, err := markUsedScript.Run(ctx, s.redis,
		[]string{quoteKeyPrefix + quoteID}, expectedOrg, hasTenant).Result()
	if err != nil {
		// Redis error replies may be prefixed with "ERR " by some clients.
		msg := strings.TrimPrefix(err.Error(), "ERR ")
		switch msg {
		case "QUOTE_EXPIRED":
			return nil, domain.ErrQuoteExpired
		case "QUOTE_ALREADY_USED":
			return nil, domain.ErrQuoteAlreadyUsed
		case "QUOTE_OWNERSHIP_MISMATCH":
			return nil, domain.ErrQuoteOwnershipMismatch
		default:
			return nil, fmt.Errorf("claim quote: %w", err)
		}
	}

	var q domain.Quote
	if err := json.Unmarshal([]byte(result.(string)), &q); err != nil {
		return nil, fmt.Errorf("decode quote: %w", err)
	}

	// Ownership is enforced atomically in markUsedScript; this is a defensive
	// re-check for a quote that decoded differently than it was written.
	if w.TenantID == nil || q.OrgID != *w.TenantID {
		return nil, domain.ErrQuoteOwnershipMismatch
	}

	// Expiry: reject if the quote has already expired server-side (even by 1 millisecond).
	if !q.ExpiresAt.IsZero() && !time.Now().UTC().Before(q.ExpiresAt) {
		return nil, domain.ErrQuoteExpired
	}

	// Amount: reject non-positive quote amounts defensively.
	if q.FromAmount.Sign() <= 0 || q.ToAmount.Sign() <= 0 {
		return nil, domain.ErrInvalidQuoteAmount
	}

	if minAmountOut != nil && q.ToAmount.LessThan(*minAmountOut) {
		return nil, fmt.Errorf("%w: destination amount %s is less than minimum required %s", domain.ErrSlippageExceeded, q.ToAmount.String(), minAmountOut.String())
	}

	if maxSlippageBps != nil {
		if err := ValidateSlippageToleranceBps(*maxSlippageBps); err != nil {
			return nil, err
		}
	}

	conv := &domain.Conversion{
		ID:             uuid.New().String(),
		WalletID:       walletID,
		SourceAsset:    q.FromAsset,
		DestAsset:      q.ToAsset,
		SourceAmount:   q.FromAmount,
		DestAmount:     q.ToAmount,
		FeeAmount:      q.Fee,
		Rate:           q.Rate,
		MinAmountOut:   minAmountOut,
		MaxSlippageBps: maxSlippageBps,
		CreatedAt:      time.Now().UTC(),
	}
	if err := s.conversionRepo.Create(ctx, conv); err != nil {
		return nil, fmt.Errorf("persist conversion: %w", err)
	}

	if s.auditRepo != nil {
		_ = s.auditRepo.MarkQuoteUsed(ctx, q.ID, conv.ID)
	}

	return conv, nil
}

// BindQuoteToTransfer atomically claims a quote and binds it to a subsequent transfer (e.g. SEP-24 or SEP-31).
func (s *service) BindQuoteToTransfer(ctx context.Context, quoteID, transferID, expectedTenantID string) (*domain.Quote, error) {
	hasTenant := "0"
	if expectedTenantID != "" {
		hasTenant = "1"
	}
	result, err := markUsedScript.Run(ctx, s.redis,
		[]string{quoteKeyPrefix + quoteID}, expectedTenantID, hasTenant).Result()
	if err != nil {
		msg := strings.TrimPrefix(err.Error(), "ERR ")
		switch msg {
		case "QUOTE_EXPIRED":
			return nil, domain.ErrQuoteExpired
		case "QUOTE_ALREADY_USED":
			return nil, domain.ErrQuoteAlreadyUsed
		case "QUOTE_OWNERSHIP_MISMATCH":
			return nil, domain.ErrQuoteOwnershipMismatch
		default:
			return nil, fmt.Errorf("claim quote: %w", err)
		}
	}

	var q domain.Quote
	if err := json.Unmarshal([]byte(result.(string)), &q); err != nil {
		return nil, fmt.Errorf("decode quote: %w", err)
	}

	if !q.ExpiresAt.IsZero() && q.ExpiresAt.Before(time.Now().UTC()) {
		return nil, domain.ErrQuoteExpired
	}

	if s.auditRepo != nil {
		_ = s.auditRepo.MarkQuoteUsed(ctx, q.ID, transferID)
	}

	return &q, nil
}

// GetRates returns a rate for the given pair, serving from the Redis cache and
// falling back to a live provider call on a cache miss.
func (s *service) GetRates(ctx context.Context, from, to string) (*RateResponse, error) {
	from, to, err := validateFXPair(from, to)
	if err != nil {
		return nil, err
	}

	if resp, ok := s.rateCache.Get(ctx, from, to); ok {
		return resp, nil
	}

	resp, err := s.fetchRate(ctx, from, to)
	if err != nil {
		return nil, err
	}

	s.rateCache.Set(ctx, from, to, resp)
	s.registerActivePair(from + ":" + to)
	return resp, nil
}

// GetLiveRates bypasses the read cache and fetches a fresh provider rate. It
// is used by the shared SSE poller, so thousands of stream clients for a pair
// still result in just one provider request per refresh interval.
func (s *service) GetLiveRates(ctx context.Context, from, to string) (*RateResponse, error) {
	from, to, err := validateFXPair(from, to)
	if err != nil {
		return nil, err
	}
	resp, err := s.fetchRate(ctx, from, to)
	if err != nil {
		return nil, err
	}
	s.rateCache.Set(ctx, from, to, resp)
	s.registerActivePair(from + ":" + to)
	return resp, nil
}

func (s *service) fetchRate(ctx context.Context, from, to string) (*RateResponse, error) {
	pairKey := from + "-" + to
	var selected Provider
	var startIndex int
	if cachedResp, ok := s.rateCache.Get(ctx, from, to); ok && cachedResp.Provider != "" {
		for i, p := range s.providers {
			if fmt.Sprintf("%T", p) == cachedResp.Provider {
				startIndex = i
				break
			}
		}
	}

	var midRate decimal.Decimal
	var err error

	for i := 0; i < len(s.providers); i++ {
		idx := (startIndex + i) % len(s.providers)
		p := s.providers[idx]
		supported := false
		for _, pair := range p.SupportedPairs() {
			if pair == pairKey {
				supported = true
				break
			}
		}
		if !supported {
			continue
		}

		midRate, err = p.GetRate(ctx, from, to, "1")
		if err == nil {
			selected = p
			break
		}
	}

	if selected == nil {
		return nil, fmt.Errorf("no provider for pair %s: %w", pairKey, err)
	}

	// Check if failover occurred: if there was a cached response from a different provider, or startIndex was non-zero and we tried a different provider / failed over.
	// Actually, if we hit a provider other than the cached one (or if cached provider failed and we fell back), invalidate or reset.
	// The requirement: "After provider failover, the cache should immediately invalidate stale entries and fetch fresh rates from the new provider."
	// And "Reset the cache entry timestamp on provider failover events, or invalidate all cached rates when the active provider changes."
	if cachedResp, ok := s.rateCache.Get(ctx, from, to); ok {
		if cachedResp.Provider != "" && cachedResp.Provider != fmt.Sprintf("%T", selected) {
			_ = s.redis.Del(ctx, rateKeyPrefix+from+":"+to).Err()
		}
	}

	pair := from + ":" + to
	volatility := s.volatility.Observe(pair, midRate.InexactFloat64(), time.Now().UTC())
	multiplier := s.volatility.SpreadMultiplier(pair, volatility)
	finalRate, adjustedSpreadBps := priceWithVolatility(midRate, s.spreadBps, multiplier)

	return &RateResponse{
		Rate:          finalRate,
		MidMarketRate: midRate,
		SpreadBps:     adjustedSpreadBps,
		Provider:      fmt.Sprintf("%T", selected),
		CachedAt:      time.Now().UTC(),
		Stale:         false,
	}, nil
}

func (s *service) registerActivePair(pair string) {
	s.activePairsMu.Lock()
	s.activePairs[pair] = time.Now()
	s.activePairsMu.Unlock()
}

// evictStalePairs removes active pairs that haven't been queried within maxAge,
// so backgroundRefresh stops polling pairs no one is asking about anymore.
func (s *service) evictStalePairs(maxAge time.Duration) {
	cutoff := time.Now().Add(-maxAge)
	s.activePairsMu.Lock()
	defer s.activePairsMu.Unlock()
	for pair, lastQueried := range s.activePairs {
		if lastQueried.Before(cutoff) {
			delete(s.activePairs, pair)
		}
	}
}

// backgroundRefresh polls active pairs every 30 seconds and refreshes the
// Redis rate cache, evicting pairs that haven't been queried recently so the
// map and the refresh workload don't grow unbounded.
func (s *service) backgroundRefresh(ctx context.Context) {
	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.evictStalePairs(activePairMaxIdle)

			s.activePairsMu.RLock()
			pairs := make([]string, 0, len(s.activePairs))
			for pair := range s.activePairs {
				pairs = append(pairs, pair)
			}
			s.activePairsMu.RUnlock()

			for _, pair := range pairs {
				parts := strings.SplitN(pair, ":", 2)
				if len(parts) != 2 {
					continue
				}
				from, to := parts[0], parts[1]
				if resp, err := s.fetchRate(ctx, from, to); err == nil {
					s.rateCache.Set(ctx, from, to, resp)
				}
			}
		}
	}
}
