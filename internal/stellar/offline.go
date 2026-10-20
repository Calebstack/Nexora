package stellar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/txnbuild"
)

const (
	// MaxEnvelopeExpirationLedgers is the strict maximum lifetime (100 ledgers) allowed
	// for an offline transaction XDR envelope from the ledger at generation.
	MaxEnvelopeExpirationLedgers uint32 = 100

	// DefaultEnvelopeExpirationLedgers is the default ledger window when none is specified.
	DefaultEnvelopeExpirationLedgers uint32 = 100
)

// EnvelopeStatus represents the lifecycle state of a cached offline XDR envelope.
type EnvelopeStatus string

const (
	EnvelopeStatusPendingSignature EnvelopeStatus = "pending_signature"
	EnvelopeStatusSigned           EnvelopeStatus = "signed"
	EnvelopeStatusBroadcasted      EnvelopeStatus = "broadcasted"
	EnvelopeStatusExpired          EnvelopeStatus = "expired"
)

var (
	// ErrEnvelopeNotFound is returned when no cached XDR envelope exists for a transaction ID.
	ErrEnvelopeNotFound = errors.New("offline transaction envelope not found")

	// ErrEnvelopeExpired is returned when the current ledger exceeds the envelope's expiration ledger.
	ErrEnvelopeExpired = errors.New("offline transaction envelope has expired")

	// ErrMaxLedgerWindowExceeded is returned when an envelope's expiration deadline exceeds
	// MaxEnvelopeExpirationLedgers (100 ledgers from generation).
	ErrMaxLedgerWindowExceeded = errors.New("envelope expiration deadline exceeds maximum 100 ledgers from generation")

	// ErrEnvelopeTampered is returned when a signed XDR's transaction hash or ledger bounds
	// do not match the cached unsigned envelope.
	ErrEnvelopeTampered = errors.New("signed XDR envelope does not match cached unsigned transaction")

	// ErrEnvelopeAlreadyBroadcasted is returned when attempting to re-submit a broadcasted envelope.
	ErrEnvelopeAlreadyBroadcasted = errors.New("offline transaction envelope has already been broadcasted")
)

// OfflineEnvelopeRecord represents a cached unsigned/signed Stellar transaction envelope
// stored in transaction_repo for asynchronous air-gapped signing.
type OfflineEnvelopeRecord struct {
	TransactionID      string         `json:"transaction_id"`
	TxHash             string         `json:"tx_hash"`
	UnsignedXDR        string         `json:"unsigned_xdr"`
	SignedXDR          string         `json:"signed_xdr,omitempty"`
	SourceAccount      string         `json:"source_account"`
	NetworkPassphrase  string         `json:"network_passphrase"`
	GeneratedAtLedger  uint32         `json:"generated_at_ledger"`
	ExpiresAtLedger    uint32         `json:"expires_at_ledger"`
	AuthorizedSigners  []string       `json:"authorized_signers"`
	RequiredSignatures int            `json:"required_signatures"`
	VerifiedSigners    []string       `json:"verified_signers,omitempty"`
	Status             EnvelopeStatus `json:"status"`
	HorizonLedger      int32          `json:"horizon_ledger,omitempty"`
	CreatedAt          time.Time      `json:"created_at"`
	UpdatedAt          time.Time      `json:"updated_at"`
	BroadcastedAt      *time.Time     `json:"broadcasted_at,omitempty"`
}

// TransactionEnvelopeRepository defines the transaction_repo persistence contract for
// storing and retrieving unsigned and signed XDR envelopes.
type TransactionEnvelopeRepository interface {
	StoreEnvelope(ctx context.Context, record *OfflineEnvelopeRecord) error
	GetEnvelope(ctx context.Context, transactionID string) (*OfflineEnvelopeRecord, error)
	UpdateEnvelope(ctx context.Context, record *OfflineEnvelopeRecord) error
	ListPendingEnvelopes(ctx context.Context) ([]*OfflineEnvelopeRecord, error)
}

// InMemoryEnvelopeRepository is a thread-safe TransactionEnvelopeRepository implementation.
type InMemoryEnvelopeRepository struct {
	mu      sync.RWMutex
	records map[string]*OfflineEnvelopeRecord
}

// NewInMemoryEnvelopeRepository creates an initialized in-memory transaction envelope cache.
func NewInMemoryEnvelopeRepository() *InMemoryEnvelopeRepository {
	return &InMemoryEnvelopeRepository{
		records: make(map[string]*OfflineEnvelopeRecord),
	}
}

func (r *InMemoryEnvelopeRepository) StoreEnvelope(_ context.Context, record *OfflineEnvelopeRecord) error {
	if record == nil || strings.TrimSpace(record.TransactionID) == "" {
		return errors.New("transaction_id is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := cloneEnvelopeRecord(record)
	r.records[cp.TransactionID] = cp
	return nil
}

func (r *InMemoryEnvelopeRepository) GetEnvelope(_ context.Context, transactionID string) (*OfflineEnvelopeRecord, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.records[strings.TrimSpace(transactionID)]
	if !ok {
		return nil, ErrEnvelopeNotFound
	}
	return cloneEnvelopeRecord(rec), nil
}

func (r *InMemoryEnvelopeRepository) UpdateEnvelope(_ context.Context, record *OfflineEnvelopeRecord) error {
	if record == nil || strings.TrimSpace(record.TransactionID) == "" {
		return errors.New("transaction_id is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.records[record.TransactionID]; !ok {
		return ErrEnvelopeNotFound
	}
	r.records[record.TransactionID] = cloneEnvelopeRecord(record)
	return nil
}

func (r *InMemoryEnvelopeRepository) ListPendingEnvelopes(_ context.Context) ([]*OfflineEnvelopeRecord, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*OfflineEnvelopeRecord, 0)
	for _, rec := range r.records {
		if rec.Status == EnvelopeStatusPendingSignature {
			out = append(out, cloneEnvelopeRecord(rec))
		}
	}
	return out, nil
}

func cloneEnvelopeRecord(in *OfflineEnvelopeRecord) *OfflineEnvelopeRecord {
	if in == nil {
		return nil
	}
	cp := *in
	if in.AuthorizedSigners != nil {
		cp.AuthorizedSigners = append([]string(nil), in.AuthorizedSigners...)
	}
	if in.VerifiedSigners != nil {
		cp.VerifiedSigners = append([]string(nil), in.VerifiedSigners...)
	}
	if in.BroadcastedAt != nil {
		t := *in.BroadcastedAt
		cp.BroadcastedAt = &t
	}
	return &cp
}

// LedgerSequenceProvider supplies the current closed Stellar ledger sequence number
// used to compute and enforce the 100-ledger envelope expiration deadline.
type LedgerSequenceProvider interface {
	CurrentLedgerSequence(ctx context.Context) (uint32, error)
}

// StaticLedgerSequenceProvider is a controllable ledger sequence provider for tests and offline nodes.
type StaticLedgerSequenceProvider struct {
	mu     sync.RWMutex
	ledger uint32
}

// NewStaticLedgerSequenceProvider creates a provider initialized at initialLedger.
func NewStaticLedgerSequenceProvider(initialLedger uint32) *StaticLedgerSequenceProvider {
	return &StaticLedgerSequenceProvider{ledger: initialLedger}
}

func (p *StaticLedgerSequenceProvider) CurrentLedgerSequence(_ context.Context) (uint32, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.ledger, nil
}

// SetLedger updates the current ledger sequence number.
func (p *StaticLedgerSequenceProvider) SetLedger(ledger uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ledger = ledger
}

// Advance increments the current ledger sequence by delta ledgers.
func (p *StaticLedgerSequenceProvider) Advance(delta uint32) uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ledger += delta
	return p.ledger
}

// GenerateEnvelopeRequest describes the parameters for building and caching an unsigned
// Stellar transaction XDR envelope for air-gapped signing.
type GenerateEnvelopeRequest struct {
	TransactionID      string
	SourceAccount      txnbuild.Account
	Operations         []txnbuild.Operation
	BaseFee            int64
	Memo               txnbuild.Memo
	ExpirationLedgers  uint32
	AuthorizedSigners  []string
	RequiredSignatures int
}

// SubmitSignedEnvelopeRequest carries a signed XDR envelope uploaded by an air-gapped system.
type SubmitSignedEnvelopeRequest struct {
	TransactionID string `json:"transaction_id"`
	SignedXDR     string `json:"signed_xdr"`
	AutoBroadcast *bool  `json:"auto_broadcast,omitempty"`
}

// OfflineSigningEngine manages the generation, caching, signature validation, ledger
// expiration enforcement, and Horizon broadcast of air-gapped transactions.
type OfflineSigningEngine struct {
	transactionRepo    TransactionEnvelopeRepository
	horizonClient      Client
	ledgerProvider     LedgerSequenceProvider
	networkPassphrase  string
	defaultSigners     []string
	defaultMinSigners  int
	autoBroadcastByDef bool
}

// OfflineEngineOption configures an OfflineSigningEngine.
type OfflineEngineOption func(*OfflineSigningEngine)

// WithDefaultAuthorizedSigners sets fallback authorized signing public keys and threshold.
func WithDefaultAuthorizedSigners(publicKeys []string, minRequired int) OfflineEngineOption {
	return func(e *OfflineSigningEngine) {
		e.defaultSigners = append([]string(nil), publicKeys...)
		if minRequired > 0 {
			e.defaultMinSigners = minRequired
		}
	}
}

// WithAutoBroadcast controls whether valid uploaded signed envelopes are broadcast to Horizon immediately.
func WithAutoBroadcast(enabled bool) OfflineEngineOption {
	return func(e *OfflineSigningEngine) {
		e.autoBroadcastByDef = enabled
	}
}

// NewOfflineSigningEngine creates a new OfflineSigningEngine backed by transaction_repo.
func NewOfflineSigningEngine(
	transactionRepo TransactionEnvelopeRepository,
	horizonClient Client,
	ledgerProvider LedgerSequenceProvider,
	stellarNetwork string,
	opts ...OfflineEngineOption,
) *OfflineSigningEngine {
	if transactionRepo == nil {
		transactionRepo = NewInMemoryEnvelopeRepository()
	}
	if ledgerProvider == nil {
		ledgerProvider = NewStaticLedgerSequenceProvider(1000)
	}
	e := &OfflineSigningEngine{
		transactionRepo:    transactionRepo,
		horizonClient:      horizonClient,
		ledgerProvider:     ledgerProvider,
		networkPassphrase:  ResolveNetworkPassphrase(stellarNetwork),
		defaultMinSigners:  1,
		autoBroadcastByDef: true,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(e)
		}
	}
	return e
}

// GenerateUnsignedEnvelope builds an unsigned Stellar transaction with strict ledger bounds
// (maximum 100 ledgers from generation), encodes it as base64 XDR, and stores it in transaction_repo.
func (e *OfflineSigningEngine) GenerateUnsignedEnvelope(ctx context.Context, req GenerateEnvelopeRequest) (*OfflineEnvelopeRecord, error) {
	txID := strings.TrimSpace(req.TransactionID)
	if txID == "" {
		return nil, errors.New("transaction_id is required")
	}
	if req.SourceAccount == nil {
		return nil, errors.New("source_account is required")
	}
	if len(req.Operations) == 0 {
		return nil, errors.New("at least one operation is required")
	}

	expirationLedgers := req.ExpirationLedgers
	if expirationLedgers == 0 {
		expirationLedgers = DefaultEnvelopeExpirationLedgers
	}
	if expirationLedgers > MaxEnvelopeExpirationLedgers {
		return nil, fmt.Errorf("%w: requested %d ledgers (max %d)", ErrMaxLedgerWindowExceeded, expirationLedgers, MaxEnvelopeExpirationLedgers)
	}

	currentLedger, err := e.ledgerProvider.CurrentLedgerSequence(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch current ledger sequence: %w", err)
	}
	expiresAtLedger := currentLedger + expirationLedgers

	baseFee := req.BaseFee
	if baseFee <= 0 {
		baseFee = int64(txnbuild.MinBaseFee)
	}

	txParams := txnbuild.TransactionParams{
		SourceAccount:        req.SourceAccount,
		IncrementSequenceNum: true,
		Operations:           req.Operations,
		BaseFee:              baseFee,
		Memo:                 req.Memo,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
			LedgerBounds: &txnbuild.LedgerBounds{
				MinLedger: currentLedger,
				MaxLedger: expiresAtLedger,
			},
		},
	}

	unsignedTx, err := txnbuild.NewTransaction(txParams)
	if err != nil {
		return nil, fmt.Errorf("build unsigned transaction: %w", err)
	}

	return e.CacheUnsignedTransaction(ctx, txID, unsignedTx, currentLedger, expiresAtLedger, req.AuthorizedSigners, req.RequiredSignatures)
}

// CacheUnsignedTransaction validates the ledger expiration window on an unsigned txnbuild.Transaction,
// serializes its XDR envelope, and stores it in transaction_repo.
func (e *OfflineSigningEngine) CacheUnsignedTransaction(
	ctx context.Context,
	transactionID string,
	unsignedTx *txnbuild.Transaction,
	generatedAtLedger uint32,
	expiresAtLedger uint32,
	authorizedSigners []string,
	requiredSignatures int,
) (*OfflineEnvelopeRecord, error) {
	transactionID = strings.TrimSpace(transactionID)
	if transactionID == "" {
		return nil, errors.New("transaction_id is required")
	}
	if unsignedTx == nil {
		return nil, errors.New("unsigned transaction is nil")
	}
	if expiresAtLedger <= generatedAtLedger {
		return nil, errors.New("expires_at_ledger must be greater than generated_at_ledger")
	}
	if expiresAtLedger-generatedAtLedger > MaxEnvelopeExpirationLedgers {
		return nil, fmt.Errorf("%w: window of %d ledgers exceeds max %d", ErrMaxLedgerWindowExceeded, expiresAtLedger-generatedAtLedger, MaxEnvelopeExpirationLedgers)
	}

	signers := authorizedSigners
	if len(signers) == 0 {
		signers = e.defaultSigners
	}
	if len(signers) == 0 {
		srcAcct := unsignedTx.SourceAccount()
		if srcAcct.AccountID != "" {
			signers = []string{srcAcct.AccountID}
		}
	}
	if len(signers) == 0 {
		return nil, ErrNoAuthorizedSigners
	}

	reqSigs := requiredSignatures
	if reqSigs <= 0 {
		reqSigs = e.defaultMinSigners
	}
	if reqSigs <= 0 {
		reqSigs = 1
	}

	unsignedXDR, err := unsignedTx.Base64()
	if err != nil {
		return nil, fmt.Errorf("encode unsigned transaction XDR: %w", err)
	}

	txHash, err := unsignedTx.HashHex(e.networkPassphrase)
	if err != nil {
		return nil, fmt.Errorf("compute unsigned transaction hash: %w", err)
	}

	now := time.Now().UTC()
	srcAccount := unsignedTx.SourceAccount()
	record := &OfflineEnvelopeRecord{
		TransactionID:      transactionID,
		TxHash:             txHash,
		UnsignedXDR:        unsignedXDR,
		SourceAccount:      srcAccount.AccountID,
		NetworkPassphrase:  e.networkPassphrase,
		GeneratedAtLedger:  generatedAtLedger,
		ExpiresAtLedger:    expiresAtLedger,
		AuthorizedSigners:  append([]string(nil), signers...),
		RequiredSignatures: reqSigs,
		Status:             EnvelopeStatusPendingSignature,
		CreatedAt:          now,
		UpdatedAt:          now,
	}

	if err := e.transactionRepo.StoreEnvelope(ctx, record); err != nil {
		return nil, fmt.Errorf("store unsigned envelope in transaction_repo: %w", err)
	}

	return record, nil
}

// GetUnsignedEnvelope retrieves a cached envelope from transaction_repo for an air-gapped signer,
// enforcing the ledger expiration deadline before returning.
func (e *OfflineSigningEngine) GetUnsignedEnvelope(ctx context.Context, transactionID string) (*OfflineEnvelopeRecord, error) {
	record, err := e.transactionRepo.GetEnvelope(ctx, transactionID)
	if err != nil {
		return nil, err
	}

	if err := e.checkLedgerExpiration(ctx, record); err != nil {
		return nil, err
	}

	return record, nil
}

// ListPendingEnvelopes returns all non-expired pending unsigned envelopes from transaction_repo.
func (e *OfflineSigningEngine) ListPendingEnvelopes(ctx context.Context) ([]*OfflineEnvelopeRecord, error) {
	records, err := e.transactionRepo.ListPendingEnvelopes(ctx)
	if err != nil {
		return nil, err
	}
	active := make([]*OfflineEnvelopeRecord, 0, len(records))
	for _, rec := range records {
		if err := e.checkLedgerExpiration(ctx, rec); err == nil {
			active = append(active, rec)
		}
	}
	return active, nil
}

// SubmitSignedEnvelope validates an uploaded signed XDR envelope from an air-gapped signer:
//  1. Verifies the envelope exists and has not already been broadcasted.
//  2. Enforces the strict 100-ledger expiration deadline against the current ledger sequence.
//  3. Verifies the signed XDR's transaction hash matches the original unsigned transaction hash.
//  4. Validates all cryptographic signatures against the authorized signing keys.
//  5. Stores the verified signed XDR in transaction_repo and broadcasts to Horizon when enabled.
func (e *OfflineSigningEngine) SubmitSignedEnvelope(ctx context.Context, req SubmitSignedEnvelopeRequest) (*OfflineEnvelopeRecord, error) {
	txID := strings.TrimSpace(req.TransactionID)
	if txID == "" {
		return nil, errors.New("transaction_id is required")
	}
	signedXDR := strings.TrimSpace(req.SignedXDR)
	if signedXDR == "" {
		return nil, errors.New("signed_xdr is required")
	}

	record, err := e.transactionRepo.GetEnvelope(ctx, txID)
	if err != nil {
		return nil, err
	}
	if record.Status == EnvelopeStatusBroadcasted {
		return nil, ErrEnvelopeAlreadyBroadcasted
	}

	// Enforce strict envelope expiration deadline (max 100 ledgers from generation)
	if err := e.checkLedgerExpiration(ctx, record); err != nil {
		return nil, err
	}

	genericTx, err := txnbuild.TransactionFromXDR(signedXDR)
	if err != nil {
		return nil, fmt.Errorf("parse signed XDR envelope: %w", err)
	}
	signedTx, ok := genericTx.Transaction()
	if !ok || signedTx == nil {
		return nil, errors.New("uploaded XDR must be a standard Stellar Transaction envelope")
	}

	// Ensure the underlying transaction body was not tampered with while offline
	signedHash, err := signedTx.HashHex(record.NetworkPassphrase)
	if err != nil {
		return nil, fmt.Errorf("compute hash of signed XDR: %w", err)
	}
	if !strings.EqualFold(signedHash, record.TxHash) {
		return nil, fmt.Errorf("%w: expected tx_hash %s, got %s", ErrEnvelopeTampered, record.TxHash, signedHash)
	}

	// Validate signatures against authorized signing keys before broadcast
	verifiedSigners, err := VerifyTransactionSignatures(
		signedTx,
		record.NetworkPassphrase,
		record.AuthorizedSigners,
		record.RequiredSignatures,
	)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	record.SignedXDR = signedXDR
	record.VerifiedSigners = verifiedSigners
	record.Status = EnvelopeStatusSigned
	record.UpdatedAt = now

	shouldBroadcast := e.autoBroadcastByDef
	if req.AutoBroadcast != nil {
		shouldBroadcast = *req.AutoBroadcast
	}

	if shouldBroadcast && e.horizonClient != nil {
		// Re-check ledger expiration immediately prior to broadcast
		if err := e.checkLedgerExpiration(ctx, record); err != nil {
			return nil, err
		}

		hResp, err := e.horizonClient.SubmitTransaction(signedTx)
		if err != nil {
			return nil, fmt.Errorf("broadcast signed envelope to horizon: %w", err)
		}

		bTime := time.Now().UTC()
		record.Status = EnvelopeStatusBroadcasted
		record.HorizonLedger = hResp.Ledger
		record.BroadcastedAt = &bTime
		record.UpdatedAt = bTime
	}

	if err := e.transactionRepo.UpdateEnvelope(ctx, record); err != nil {
		return nil, fmt.Errorf("update signed envelope in transaction_repo: %w", err)
	}

	return record, nil
}

func (e *OfflineSigningEngine) checkLedgerExpiration(ctx context.Context, record *OfflineEnvelopeRecord) error {
	if record.ExpiresAtLedger < record.GeneratedAtLedger ||
		record.ExpiresAtLedger-record.GeneratedAtLedger > MaxEnvelopeExpirationLedgers {
		record.Status = EnvelopeStatusExpired
		record.UpdatedAt = time.Now().UTC()
		_ = e.transactionRepo.UpdateEnvelope(ctx, record)
		return fmt.Errorf("%w: ledger window [%d, %d] exceeds 100 ledgers", ErrMaxLedgerWindowExceeded, record.GeneratedAtLedger, record.ExpiresAtLedger)
	}

	currentLedger, err := e.ledgerProvider.CurrentLedgerSequence(ctx)
	if err != nil {
		return fmt.Errorf("query current ledger sequence: %w", err)
	}

	if currentLedger > record.ExpiresAtLedger {
		if record.Status != EnvelopeStatusExpired {
			record.Status = EnvelopeStatusExpired
			record.UpdatedAt = time.Now().UTC()
			_ = e.transactionRepo.UpdateEnvelope(ctx, record)
		}
		return fmt.Errorf("%w: current ledger %d > expires_at_ledger %d", ErrEnvelopeExpired, currentLedger, record.ExpiresAtLedger)
	}

	return nil
}

// OfflineSigningHandler exposes REST API endpoints for air-gapped systems to retrieve
// unsigned transaction XDR envelopes and upload signed envelopes.
type OfflineSigningHandler struct {
	engine *OfflineSigningEngine
	router chi.Router
}

// NewOfflineSigningHandler creates an HTTP handler for air-gapped offline signing endpoints.
func NewOfflineSigningHandler(engine *OfflineSigningEngine) *OfflineSigningHandler {
	h := &OfflineSigningHandler{engine: engine}
	r := chi.NewRouter()
	h.RegisterRoutes(r)
	h.router = r
	return h
}

// RegisterRoutes mounts the offline signing endpoints onto r.
func (h *OfflineSigningHandler) RegisterRoutes(r chi.Router) {
	r.Get("/offline/transactions", h.HandleListPending)
	r.Get("/offline/transactions/{id}/xdr", h.HandleGetUnsignedXDR)
	r.Post("/offline/transactions/{id}/sign", h.HandleUploadSignedXDR)
	r.Post("/offline/transactions/{id}/xdr", h.HandleUploadSignedXDR)

	r.Get("/api/v1/offline/transactions", h.HandleListPending)
	r.Get("/api/v1/offline/transactions/{id}/xdr", h.HandleGetUnsignedXDR)
	r.Post("/api/v1/offline/transactions/{id}/sign", h.HandleUploadSignedXDR)
	r.Post("/api/v1/offline/transactions/{id}/xdr", h.HandleUploadSignedXDR)
}

func (h *OfflineSigningHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.router.ServeHTTP(w, r)
}

// HandleListPending handles GET /offline/transactions.
func (h *OfflineSigningHandler) HandleListPending(w http.ResponseWriter, r *http.Request) {
	records, err := h.engine.ListPendingEnvelopes(r.Context())
	if err != nil {
		writeOfflineError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeOfflineJSON(w, http.StatusOK, map[string]any{
		"transactions": records,
	})
}

// HandleGetUnsignedXDR handles GET /offline/transactions/{id}/xdr for air-gapped systems
// to retrieve the unsigned XDR envelope and signing metadata.
func (h *OfflineSigningHandler) HandleGetUnsignedXDR(w http.ResponseWriter, r *http.Request) {
	txID := strings.TrimSpace(chi.URLParam(r, "id"))
	if txID == "" {
		writeOfflineError(w, http.StatusBadRequest, "transaction id is required")
		return
	}

	rec, err := h.engine.GetUnsignedEnvelope(r.Context(), txID)
	if err != nil {
		switch {
		case errors.Is(err, ErrEnvelopeNotFound):
			writeOfflineError(w, http.StatusNotFound, err.Error())
		case errors.Is(err, ErrEnvelopeExpired), errors.Is(err, ErrMaxLedgerWindowExceeded):
			writeOfflineError(w, http.StatusGone, err.Error())
		default:
			writeOfflineError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	writeOfflineJSON(w, http.StatusOK, rec)
}

// HandleUploadSignedXDR handles POST /offline/transactions/{id}/sign for air-gapped systems
// to upload a signed XDR envelope for verification and Horizon broadcast.
func (h *OfflineSigningHandler) HandleUploadSignedXDR(w http.ResponseWriter, r *http.Request) {
	txID := strings.TrimSpace(chi.URLParam(r, "id"))
	if txID == "" {
		writeOfflineError(w, http.StatusBadRequest, "transaction id is required")
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeOfflineError(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	var req SubmitSignedEnvelopeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeOfflineError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}
	req.TransactionID = txID

	rec, err := h.engine.SubmitSignedEnvelope(r.Context(), req)
	if err != nil {
		switch {
		case errors.Is(err, ErrEnvelopeNotFound):
			writeOfflineError(w, http.StatusNotFound, err.Error())
		case errors.Is(err, ErrEnvelopeExpired), errors.Is(err, ErrMaxLedgerWindowExceeded):
			writeOfflineError(w, http.StatusGone, err.Error())
		case errors.Is(err, ErrUnauthorizedSignature), errors.Is(err, ErrInsufficientSignatures), errors.Is(err, ErrEnvelopeTampered):
			writeOfflineError(w, http.StatusUnprocessableEntity, err.Error())
		case errors.Is(err, ErrEnvelopeAlreadyBroadcasted):
			writeOfflineError(w, http.StatusConflict, err.Error())
		default:
			writeOfflineError(w, http.StatusBadGateway, err.Error())
		}
		return
	}

	writeOfflineJSON(w, http.StatusOK, rec)
}

func writeOfflineJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeOfflineError(w http.ResponseWriter, status int, msg string) {
	writeOfflineJSON(w, status, map[string]string{
		"error": msg,
	})
}

// Ensure Horizon response type is referenced cleanly.
var _ horizon.Transaction
