package anchor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/api"
	"github.com/Lumen-Nexora/Nexora/internal/crypto"
	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/tenant"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
)

// PayoutCorridors maps each supported payout currency (ISO 4217) to its
// country (ISO 3166-1 alpha-2). All four currencies have 2 minor units.
var PayoutCorridors = map[string]string{
	"NGN": "NG",
	"KES": "KE",
	"GHS": "GH",
	"ZAR": "ZA",
}

const fiatMinorUnits int32 = 2

// PayoutStatus is the lifecycle state of one payout.
type PayoutStatus string

const (
	PayoutQueued     PayoutStatus = "queued"
	PayoutSubmitting PayoutStatus = "submitting"
	PayoutSubmitted  PayoutStatus = "submitted"
	PayoutCompleted  PayoutStatus = "completed"
	PayoutFailed     PayoutStatus = "failed"
	PayoutReversed   PayoutStatus = "reversed"
)

// ReservationState tracks the stablecoin hold behind a payout. Held and
// captured amounts both count against the wallet's available balance;
// released amounts are returned to it.
type ReservationState string

const (
	ReservationHeld     ReservationState = "held"
	ReservationCaptured ReservationState = "captured"
	ReservationReleased ReservationState = "released"
)

// PayoutOutcome is a verified banking result.
type PayoutOutcome string

const (
	PayoutOutcomeSuccess  PayoutOutcome = "success"
	PayoutOutcomeFailure  PayoutOutcome = "failure"
	PayoutOutcomeReversal PayoutOutcome = "reversal"
)

var (
	ErrPayoutNotFound          = errors.New("payout not found")
	ErrPayoutBatchNotFound     = errors.New("payout batch not found")
	ErrInvalidPayout           = errors.New("invalid payout instruction")
	ErrInvalidPayoutTransition = errors.New("payout cannot take this outcome in its current state")
	ErrIdempotencyKeyReused    = errors.New("idempotency key was used with a different payout batch")
	// ErrPayoutRejected marks a definitive partner rejection. Partners wrap
	// it so the queue fails the payout and releases its reservation instead
	// of retrying.
	ErrPayoutRejected = errors.New("payout rejected by partner")
	// ErrCallbackUnverified is returned by PayoutPartner.VerifyCallback when
	// the callback's authenticity cannot be established.
	ErrCallbackUnverified = errors.New("payout callback could not be verified")
)

// MaxPayoutBatchSize bounds one batch.
const MaxPayoutBatchSize = 500

// PayoutBeneficiary is the destination bank account; it is encrypted at rest.
type PayoutBeneficiary struct {
	AccountName   string `json:"account_name"`
	AccountNumber string `json:"account_number"`
	BankCode      string `json:"bank_code"`
}

// PayoutInstruction is one disbursement in a batch request.
type PayoutInstruction struct {
	Reference           string            `json:"reference"`
	Partner             string            `json:"partner"`
	DestinationCurrency string            `json:"destination_currency"`
	DestinationAmount   decimal.Decimal   `json:"destination_amount"`
	SourceAsset         string            `json:"source_asset"`
	SourceAmount        decimal.Decimal   `json:"source_amount"`
	Beneficiary         PayoutBeneficiary `json:"beneficiary"`
}

// PayoutBatchRequest submits instructions funded from one tenant custodial wallet.
type PayoutBatchRequest struct {
	TenantID       string
	WalletID       string
	IdempotencyKey string
	Instructions   []PayoutInstruction
}

// Payout is a persisted disbursement.
type Payout struct {
	ID                    string
	BatchID               string
	TenantID              string
	WalletID              string
	Reference             string
	Partner               string
	DestinationCurrency   string
	Country               string
	DestinationAmount     decimal.Decimal
	SourceAsset           string
	SourceIssuer          string
	SourceAmount          decimal.Decimal
	BeneficiaryCiphertext []byte
	Status                PayoutStatus
	Reservation           ReservationState
	ProviderRef           string
	Attempts              int
	NextAttemptAt         time.Time
	LastError             string
	CreatedAt             time.Time
	UpdatedAt             time.Time
	CompletedAt           *time.Time
}

// PayoutBatch groups payouts submitted together.
type PayoutBatch struct {
	ID             string
	TenantID       string
	WalletID       string
	IdempotencyKey string
	RequestHash    string
	Payouts        []*Payout
	CreatedAt      time.Time
}

// BalanceHold is the total a batch reserves from one wallet balance.
type BalanceHold struct {
	WalletID  string
	AssetCode string
	Issuer    string
	Amount    decimal.Decimal
}

// PayoutEvent is a verified outcome to apply exactly once per
// (Partner, EventID).
type PayoutEvent struct {
	Partner  string
	EventID  string
	PayoutID string
	Outcome  PayoutOutcome
	Reason   string
	At       time.Time
}

// PayoutTransition is the effect of an outcome on a payout.
type PayoutTransition struct {
	Status      PayoutStatus
	Reservation ReservationState
	// Release reports whether the reserved amount returns to the wallet.
	Release bool
}

// NextPayoutState is the payout state machine:
//
//	success:  submitting|submitted -> completed, held -> captured
//	failure:  submitting|submitted -> failed,    held -> released
//	reversal: completed            -> reversed,  captured -> released
func NextPayoutState(status PayoutStatus, outcome PayoutOutcome) (PayoutTransition, error) {
	inFlight := status == PayoutSubmitting || status == PayoutSubmitted
	switch {
	case outcome == PayoutOutcomeSuccess && inFlight:
		return PayoutTransition{Status: PayoutCompleted, Reservation: ReservationCaptured}, nil
	case outcome == PayoutOutcomeFailure && inFlight:
		return PayoutTransition{Status: PayoutFailed, Reservation: ReservationReleased, Release: true}, nil
	case outcome == PayoutOutcomeReversal && status == PayoutCompleted:
		return PayoutTransition{Status: PayoutReversed, Reservation: ReservationReleased, Release: true}, nil
	}
	return PayoutTransition{}, fmt.Errorf("%w: %s on %s", ErrInvalidPayoutTransition, outcome, status)
}

// PayoutStore persists batches, the payout queue and balance holds.
type PayoutStore interface {
	// CreateBatch writes the batch and reserves every hold in one
	// transaction: each wallet balance row is locked and a hold succeeds
	// only if balance - held >= amount, otherwise nothing is written and
	// domain.ErrInsufficientBalance is returned. Replaying an idempotency
	// key returns the stored batch with created=false.
	CreateBatch(ctx context.Context, b *PayoutBatch, holds []BalanceHold) (stored *PayoutBatch, created bool, err error)
	GetBatch(ctx context.Context, tenantID, batchID string) (*PayoutBatch, error)
	// ClaimForPartner leases due payouts for one partner, keeping at most
	// maxInFlight in submitting across every worker, and increments their
	// attempt counts.
	ClaimForPartner(ctx context.Context, partner string, maxInFlight int, now, leaseUntil time.Time) ([]*Payout, error)
	MarkSubmitted(ctx context.Context, payoutID, providerRef string, at time.Time) error
	// Reschedule returns a submitting payout to the queue after a transient error.
	Reschedule(ctx context.Context, payoutID string, next time.Time, lastErr string) error
	// ApplyEvent applies e via NextPayoutState and releases the hold when
	// required, atomically. A duplicate (Partner, EventID) is a no-op that
	// returns the current payout with applied=false.
	ApplyEvent(ctx context.Context, e PayoutEvent) (p *Payout, applied bool, err error)
}

// PayoutCallback is a partner callback after authentication. PayoutID is
// the Nexora payout ID the partner echoes back as its reference.
type PayoutCallback struct {
	EventID  string
	PayoutID string
	Outcome  PayoutOutcome
	Reason   string
}

// PayoutPartner is a local banking rail that disburses fiat.
type PayoutPartner interface {
	Name() string
	SupportedCurrencies() []string
	// Disburse submits p and returns the partner's reference. It must use
	// p.ID as the idempotency reference so a retried submission never pays
	// twice, and wrap ErrPayoutRejected for rejections retrying cannot fix.
	Disburse(ctx context.Context, p *Payout, b PayoutBeneficiary) (providerRef string, err error)
	// VerifyCallback authenticates and parses a callback, returning an error
	// wrapping ErrCallbackUnverified when authentication fails.
	VerifyCallback(ctx context.Context, payload []byte, headers http.Header) (*PayoutCallback, error)
}

// PayoutQueueConfig configures the queue. Zero values fall back to defaults.
type PayoutQueueConfig struct {
	// EncryptionKey (32 bytes) encrypts beneficiary details at rest.
	EncryptionKey []byte
	// SourceAssets maps each fundable stablecoin code (e.g. USDC, EURC) to
	// its Stellar issuer.
	SourceAssets map[string]string
	// PartnerConcurrency caps in-flight submissions per partner (default 4).
	PartnerConcurrency map[string]int
	// BaseBackoff and MaxBackoff bound transient retry delays (30s, 30m).
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	// Lease bounds one submission; a crashed worker's payout is reclaimed
	// after it (default 2m). It must exceed the partner client timeout.
	Lease        time.Duration
	PollInterval time.Duration
	Now          func() time.Time
}

// PayoutQueue accepts batch payouts, reserves their funding and disburses
// them through per-partner worker pools.
type PayoutQueue struct {
	store    PayoutStore
	partners map[string]PayoutPartner
	cfg      PayoutQueueConfig
}

func NewPayoutQueue(store PayoutStore, partners []PayoutPartner, cfg PayoutQueueConfig) (*PayoutQueue, error) {
	if len(cfg.EncryptionKey) != 32 {
		return nil, crypto.ErrKeyLength
	}
	if len(cfg.SourceAssets) == 0 {
		return nil, errors.New("payout queue: at least one source asset is required")
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 30 * time.Second
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 30 * time.Minute
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 2 * time.Minute
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	byName := make(map[string]PayoutPartner, len(partners))
	for _, p := range partners {
		byName[p.Name()] = p
	}
	return &PayoutQueue{store: store, partners: byName, cfg: cfg}, nil
}

// SubmitBatch validates the instructions, encrypts beneficiaries and
// atomically reserves the stablecoin balance for the whole batch. O(n log n)
// in the number of instructions (sorting holds for a fixed lock order).
func (q *PayoutQueue) SubmitBatch(ctx context.Context, req PayoutBatchRequest) (*PayoutBatch, bool, error) {
	if req.TenantID == "" || req.WalletID == "" || req.IdempotencyKey == "" {
		return nil, false, fmt.Errorf("%w: tenant, wallet and idempotency key are required", ErrInvalidPayout)
	}
	if n := len(req.Instructions); n == 0 || n > MaxPayoutBatchSize {
		return nil, false, fmt.Errorf("%w: a batch holds 1 to %d instructions", ErrInvalidPayout, MaxPayoutBatchSize)
	}

	hash, err := requestHash(req)
	if err != nil {
		return nil, false, err
	}
	now := q.cfg.Now().UTC()
	batch := &PayoutBatch{
		ID:             uuid.NewString(),
		TenantID:       req.TenantID,
		WalletID:       req.WalletID,
		IdempotencyKey: req.IdempotencyKey,
		RequestHash:    hash,
		Payouts:        make([]*Payout, 0, len(req.Instructions)),
		CreatedAt:      now,
	}
	totals := make(map[string]decimal.Decimal)
	references := make(map[string]struct{}, len(req.Instructions))
	for i, in := range req.Instructions {
		if err := q.validate(in); err != nil {
			return nil, false, fmt.Errorf("instruction %d: %w", i, err)
		}
		if _, dup := references[in.Reference]; dup {
			return nil, false, fmt.Errorf("instruction %d: %w: duplicate reference %q", i, ErrInvalidPayout, in.Reference)
		}
		references[in.Reference] = struct{}{}

		beneficiary, err := json.Marshal(in.Beneficiary)
		if err != nil {
			return nil, false, fmt.Errorf("marshal beneficiary: %w", err)
		}
		ciphertext, err := crypto.Encrypt(beneficiary, q.cfg.EncryptionKey)
		if err != nil {
			return nil, false, fmt.Errorf("encrypt beneficiary: %w", err)
		}
		currency := strings.ToUpper(in.DestinationCurrency)
		asset := strings.ToUpper(in.SourceAsset)
		batch.Payouts = append(batch.Payouts, &Payout{
			ID:                    uuid.NewString(),
			BatchID:               batch.ID,
			TenantID:              req.TenantID,
			WalletID:              req.WalletID,
			Reference:             in.Reference,
			Partner:               in.Partner,
			DestinationCurrency:   currency,
			Country:               PayoutCorridors[currency],
			DestinationAmount:     in.DestinationAmount,
			SourceAsset:           asset,
			SourceIssuer:          q.cfg.SourceAssets[asset],
			SourceAmount:          in.SourceAmount,
			BeneficiaryCiphertext: ciphertext,
			Status:                PayoutQueued,
			Reservation:           ReservationHeld,
			NextAttemptAt:         now,
			CreatedAt:             now,
			UpdatedAt:             now,
		})
		totals[asset] = totals[asset].Add(in.SourceAmount)
	}

	holds := make([]BalanceHold, 0, len(totals))
	for asset, amount := range totals {
		holds = append(holds, BalanceHold{WalletID: req.WalletID, AssetCode: asset, Issuer: q.cfg.SourceAssets[asset], Amount: amount})
	}
	// A fixed order keeps concurrent batches on the same wallet from deadlocking.
	sort.Slice(holds, func(i, j int) bool { return holds[i].AssetCode < holds[j].AssetCode })

	stored, created, err := q.store.CreateBatch(ctx, batch, holds)
	if err != nil {
		return nil, false, err
	}
	if !created && stored.RequestHash != hash {
		return nil, false, ErrIdempotencyKeyReused
	}
	return stored, created, nil
}

func (q *PayoutQueue) validate(in PayoutInstruction) error {
	if in.Reference == "" || len(in.Reference) > 64 {
		return fmt.Errorf("%w: reference must be 1-64 characters", ErrInvalidPayout)
	}
	currency := strings.ToUpper(in.DestinationCurrency)
	if _, ok := PayoutCorridors[currency]; !ok {
		return fmt.Errorf("%w: unsupported destination currency %q", ErrInvalidPayout, in.DestinationCurrency)
	}
	partner, ok := q.partners[in.Partner]
	if !ok {
		return fmt.Errorf("%w: unknown payout partner %q", ErrInvalidPayout, in.Partner)
	}
	if !supports(partner, currency) {
		return fmt.Errorf("%w: partner %s does not pay out %s", ErrInvalidPayout, in.Partner, currency)
	}
	if _, ok := q.cfg.SourceAssets[strings.ToUpper(in.SourceAsset)]; !ok {
		return fmt.Errorf("%w: unsupported source asset %q", ErrInvalidPayout, in.SourceAsset)
	}
	if !in.DestinationAmount.IsPositive() || in.DestinationAmount.Exponent() < -fiatMinorUnits {
		return fmt.Errorf("%w: destination_amount must be positive with at most %d decimals", ErrInvalidPayout, fiatMinorUnits)
	}
	if !in.SourceAmount.IsPositive() || in.SourceAmount.Exponent() < -stellarAssetPrecision {
		return fmt.Errorf("%w: source_amount must be positive with at most %d decimals", ErrInvalidPayout, stellarAssetPrecision)
	}
	b := in.Beneficiary
	if b.AccountName == "" || b.AccountNumber == "" || b.BankCode == "" {
		return fmt.Errorf("%w: beneficiary account_name, account_number and bank_code are required", ErrInvalidPayout)
	}
	return nil
}

func supports(p PayoutPartner, currency string) bool {
	for _, c := range p.SupportedCurrencies() {
		if strings.EqualFold(c, currency) {
			return true
		}
	}
	return false
}

// requestHash fingerprints the instructions so an idempotency key cannot be
// replayed with a different batch.
func requestHash(req PayoutBatchRequest) (string, error) {
	body, err := json.Marshal(struct {
		WalletID     string              `json:"wallet_id"`
		Instructions []PayoutInstruction `json:"instructions"`
	}{req.WalletID, req.Instructions})
	if err != nil {
		return "", fmt.Errorf("hash payout batch: %w", err)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

// GetBatch returns a tenant's batch with its payouts.
func (q *PayoutQueue) GetBatch(ctx context.Context, tenantID, batchID string) (*PayoutBatch, error) {
	return q.store.GetBatch(ctx, tenantID, batchID)
}

// Run drives one worker loop per partner until ctx is cancelled.
func (q *PayoutQueue) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for name := range q.partners {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			ticker := time.NewTicker(q.cfg.PollInterval)
			defer ticker.Stop()
			for ctx.Err() == nil {
				n, err := q.ProcessPartner(ctx, name)
				if err != nil {
					log.Error().Err(err).Str("partner", name).Msg("anchor payouts: process partner queue")
				}
				if n > 0 && err == nil {
					continue
				}
				select {
				case <-ctx.Done():
				case <-ticker.C:
				}
			}
		}(name)
	}
	wg.Wait()
}

// ProcessPartner claims up to the partner's free concurrency slots and
// submits them in parallel, returning how many were claimed.
func (q *PayoutQueue) ProcessPartner(ctx context.Context, name string) (int, error) {
	partner, ok := q.partners[name]
	if !ok {
		return 0, fmt.Errorf("unknown payout partner %q", name)
	}
	now := q.cfg.Now().UTC()
	claimed, err := q.store.ClaimForPartner(ctx, name, q.concurrency(name), now, now.Add(q.cfg.Lease))
	if err != nil {
		return 0, fmt.Errorf("claim payouts: %w", err)
	}
	var wg sync.WaitGroup
	for _, p := range claimed {
		wg.Add(1)
		go func(p *Payout) {
			defer wg.Done()
			if err := q.submit(ctx, partner, p); err != nil {
				log.Error().Err(err).Str("payout_id", p.ID).Str("partner", name).Msg("anchor payouts: record submission")
			}
		}(p)
	}
	wg.Wait()
	return len(claimed), nil
}

func (q *PayoutQueue) concurrency(partner string) int {
	if n := q.cfg.PartnerConcurrency[partner]; n > 0 {
		return n
	}
	return 4
}

func (q *PayoutQueue) submit(ctx context.Context, partner PayoutPartner, p *Payout) error {
	plain, err := crypto.Decrypt(p.BeneficiaryCiphertext, q.cfg.EncryptionKey)
	if err != nil {
		return q.retry(ctx, p, fmt.Errorf("decrypt beneficiary: %w", err))
	}
	var beneficiary PayoutBeneficiary
	if err := json.Unmarshal(plain, &beneficiary); err != nil {
		return q.retry(ctx, p, fmt.Errorf("decode beneficiary: %w", err))
	}

	ref, err := partner.Disburse(ctx, p, beneficiary)
	switch {
	case err == nil:
		return q.store.MarkSubmitted(ctx, p.ID, ref, q.cfg.Now().UTC())
	case errors.Is(err, ErrPayoutRejected):
		// A definitive rejection is a verified failure: release the hold.
		_, _, applyErr := q.store.ApplyEvent(ctx, PayoutEvent{
			Partner:  p.Partner,
			EventID:  "nexora:rejected:" + p.ID,
			PayoutID: p.ID,
			Outcome:  PayoutOutcomeFailure,
			Reason:   err.Error(),
			At:       q.cfg.Now().UTC(),
		})
		return applyErr
	default:
		// Ambiguous errors (timeouts, 5xx) may have reached the partner, so
		// the hold stays in place and the idempotent submission is retried.
		return q.retry(ctx, p, err)
	}
}

func (q *PayoutQueue) retry(ctx context.Context, p *Payout, cause error) error {
	delay := q.cfg.BaseBackoff
	for i := 1; i < p.Attempts && delay < q.cfg.MaxBackoff; i++ {
		delay *= 2
	}
	next := q.cfg.Now().UTC().Add(min(delay, q.cfg.MaxBackoff))
	return q.store.Reschedule(ctx, p.ID, next, cause.Error())
}

// HandleCallback authenticates a partner callback and applies its outcome.
// Duplicate deliveries of the same event are no-ops.
func (q *PayoutQueue) HandleCallback(ctx context.Context, partnerName string, payload []byte, headers http.Header) (*Payout, error) {
	partner, ok := q.partners[partnerName]
	if !ok {
		return nil, fmt.Errorf("%w: unknown partner %q", ErrCallbackUnverified, partnerName)
	}
	cb, err := partner.VerifyCallback(ctx, payload, headers)
	if err != nil {
		return nil, err
	}
	if cb.EventID == "" || cb.PayoutID == "" {
		return nil, fmt.Errorf("%w: callback must carry an event id and payout reference", ErrInvalidPayout)
	}
	p, _, err := q.store.ApplyEvent(ctx, PayoutEvent{
		Partner:  partnerName,
		EventID:  cb.EventID,
		PayoutID: cb.PayoutID,
		Outcome:  cb.Outcome,
		Reason:   cb.Reason,
		At:       q.cfg.Now().UTC(),
	})
	return p, err
}

// PayoutHandler exposes batch submission to tenants and the partner
// callback endpoint.
type PayoutHandler struct {
	queue *PayoutQueue
}

func NewPayoutHandler(queue *PayoutQueue) *PayoutHandler {
	return &PayoutHandler{queue: queue}
}

// Routes mounts the tenant API: POST /batches and GET /batches/{id}.
func (h *PayoutHandler) Routes() func(r chi.Router) {
	return func(r chi.Router) {
		r.Post("/batches", h.submitBatch)
		r.Get("/batches/{id}", h.getBatch)
	}
}

// CallbackRoutes mounts the unauthenticated partner callback, POST
// /{partner}; each partner authenticates its own payloads.
func (h *PayoutHandler) CallbackRoutes() func(r chi.Router) {
	return func(r chi.Router) {
		r.Post("/{partner}", h.callback)
	}
}

type submitBatchRequest struct {
	WalletID     string              `json:"wallet_id"`
	Instructions []PayoutInstruction `json:"instructions"`
}

type payoutResponse struct {
	ID                  string     `json:"id"`
	Reference           string     `json:"reference"`
	Partner             string     `json:"partner"`
	DestinationCurrency string     `json:"destination_currency"`
	DestinationAmount   string     `json:"destination_amount"`
	SourceAsset         string     `json:"source_asset"`
	SourceAmount        string     `json:"source_amount"`
	Status              string     `json:"status"`
	Reservation         string     `json:"reservation"`
	ProviderRef         string     `json:"provider_ref,omitempty"`
	CompletedAt         *time.Time `json:"completed_at,omitempty"`
}

type batchResponse struct {
	ID        string           `json:"id"`
	WalletID  string           `json:"wallet_id"`
	CreatedAt time.Time        `json:"created_at"`
	Payouts   []payoutResponse `json:"payouts"`
}

func toBatchResponse(b *PayoutBatch) batchResponse {
	out := batchResponse{ID: b.ID, WalletID: b.WalletID, CreatedAt: b.CreatedAt, Payouts: make([]payoutResponse, 0, len(b.Payouts))}
	for _, p := range b.Payouts {
		out.Payouts = append(out.Payouts, payoutResponse{
			ID:                  p.ID,
			Reference:           p.Reference,
			Partner:             p.Partner,
			DestinationCurrency: p.DestinationCurrency,
			DestinationAmount:   p.DestinationAmount.StringFixed(fiatMinorUnits),
			SourceAsset:         p.SourceAsset,
			SourceAmount:        p.SourceAmount.StringFixed(stellarAssetPrecision),
			Status:              string(p.Status),
			Reservation:         string(p.Reservation),
			ProviderRef:         p.ProviderRef,
			CompletedAt:         p.CompletedAt,
		})
	}
	return out
}

func (h *PayoutHandler) submitBatch(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 255 {
		api.BadRequest(w, "Idempotency-Key header is required")
		return
	}
	var req submitBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.BadRequest(w, "invalid request body")
		return
	}
	batch, created, err := h.queue.SubmitBatch(r.Context(), PayoutBatchRequest{
		TenantID:       tenant.IDFromContext(r.Context()),
		WalletID:       req.WalletID,
		IdempotencyKey: key,
		Instructions:   req.Instructions,
	})
	switch {
	case err == nil:
	case errors.Is(err, ErrInvalidPayout):
		api.BadRequest(w, err.Error())
		return
	case errors.Is(err, ErrIdempotencyKeyReused):
		api.Error(w, http.StatusConflict, "IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_BODY", err.Error())
		return
	case errors.Is(err, domain.ErrInsufficientBalance):
		api.UnprocessableEntity(w, "INSUFFICIENT_BALANCE", "available balance does not cover the batch")
		return
	case errors.Is(err, domain.ErrWalletNotFound):
		api.NotFound(w, "wallet not found")
		return
	default:
		api.InternalError(w, err)
		return
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	api.JSON(w, status, toBatchResponse(batch))
}

func (h *PayoutHandler) getBatch(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if _, err := uuid.Parse(id); err != nil {
		api.NotFound(w, "payout batch not found")
		return
	}
	batch, err := h.queue.GetBatch(r.Context(), tenant.IDFromContext(r.Context()), id)
	if err != nil {
		if errors.Is(err, ErrPayoutBatchNotFound) {
			api.NotFound(w, "payout batch not found")
			return
		}
		api.InternalError(w, err)
		return
	}
	api.JSON(w, http.StatusOK, toBatchResponse(batch))
}

func (h *PayoutHandler) callback(w http.ResponseWriter, r *http.Request) {
	payload, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		api.BadRequest(w, "invalid request body")
		return
	}
	p, err := h.queue.HandleCallback(r.Context(), chi.URLParam(r, "partner"), payload, r.Header)
	switch {
	case err == nil:
		api.JSON(w, http.StatusOK, map[string]string{"payout_id": p.ID, "status": string(p.Status)})
	case errors.Is(err, ErrCallbackUnverified):
		api.Error(w, http.StatusUnauthorized, "CALLBACK_UNVERIFIED", "callback could not be verified")
	case errors.Is(err, ErrInvalidPayout):
		api.BadRequest(w, err.Error())
	case errors.Is(err, ErrPayoutNotFound):
		api.NotFound(w, "payout not found")
	case errors.Is(err, ErrInvalidPayoutTransition):
		api.Error(w, http.StatusConflict, "INVALID_PAYOUT_TRANSITION", err.Error())
	default:
		api.InternalError(w, err)
	}
}
