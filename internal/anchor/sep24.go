package anchor

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/google/uuid"
	"github.com/stellar/go/keypair"
)

// Sep24Client starts interactive deposits/withdrawals (SEP-24) against an
// anchor's TRANSFER_SERVER_SEP0024, handing back a URL to open in an iframe
// or webview rather than collecting bank details directly.
type Sep24Client struct {
	httpClient *http.Client
}

func NewSep24Client(httpClient *http.Client) *Sep24Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 20 * time.Second}
	}
	return &Sep24Client{httpClient: httpClient}
}

type interactiveRequest struct {
	AssetCode string `json:"asset_code"`
	Amount    string `json:"amount,omitempty"`
	Account   string `json:"account,omitempty"`
}

type interactiveResponse struct {
	Type  string `json:"type"`
	URL   string `json:"url"`
	ID    string `json:"id"`
	Error string `json:"error"`
}

// GetInteractiveUrl starts a SEP-24 interactive deposit or withdrawal
// ("deposit" or "withdraw") and returns the HTTPS URL the client should open
// to complete it, along with the anchor-assigned transaction id.
func (c *Sep24Client) GetInteractiveUrl(ctx context.Context, transferServerSep24, jwt, assetCode, account, amount, operation string) (interactiveURL, transactionID string, err error) {
	var path string
	switch operation {
	case domain.AnchorTxTypeDeposit:
		path = "/transactions/deposit/interactive"
	case domain.AnchorTxTypeWithdrawal, "withdraw":
		path = "/transactions/withdraw/interactive"
	default:
		return "", "", fmt.Errorf("unsupported sep-24 operation %q", operation)
	}

	reqBody, err := json.Marshal(interactiveRequest{AssetCode: assetCode, Amount: amount, Account: account})
	if err != nil {
		return "", "", fmt.Errorf("marshal interactive request: %w", err)
	}

	var out interactiveResponse
	if err := doAnchorRequest(ctx, c.httpClient, http.MethodPost, transferServerSep24+path, jwt, bytes.NewReader(reqBody), &out); err != nil {
		return "", "", fmt.Errorf("start sep-24 %s: %w", operation, err)
	}
	if out.Error != "" {
		return "", "", fmt.Errorf("anchor returned error: %s", out.Error)
	}
	if out.URL == "" {
		return "", "", fmt.Errorf("anchor did not return an interactive url")
	}
	parsed, err := url.Parse(out.URL)
	if err != nil || parsed.Scheme != "https" {
		return "", "", fmt.Errorf("anchor returned a non-https interactive url: %q", out.URL)
	}

	return out.URL, out.ID, nil
}

// PollTransaction fetches the current status of a SEP-24 transaction
// previously started with GetInteractiveUrl.
func (c *Sep24Client) PollTransaction(ctx context.Context, transferServerSep24, jwt, transactionID string) (*AnchorTransaction, error) {
	q := url.Values{}
	q.Set("id", transactionID)

	var out getTransactionResponse
	if err := doAnchorRequest(ctx, c.httpClient, http.MethodGet, transferServerSep24+"/transaction?"+q.Encode(), jwt, nil, &out); err != nil {
		return nil, fmt.Errorf("get sep-24 transaction: %w", err)
	}
	return &out.Transaction, nil
}

// -------------------------------------------------------------------------
// SEP-24 Interactive Session Coordinator & State Machine (Issue #22)
// -------------------------------------------------------------------------

type Sep24Status string

const (
	Sep24StatusIncomplete               Sep24Status = "incomplete"
	Sep24StatusPendingUserTransferStart Sep24Status = "pending_user_transfer_start"
	Sep24StatusPendingAnchor            Sep24Status = "pending_anchor"
	Sep24StatusCompleted                Sep24Status = "completed"
	Sep24StatusError                    Sep24Status = "error"
)

var (
	ErrInvalidStateTransition = errors.New("invalid sep-24 state transition")
	ErrSessionNotFound        = errors.New("sep-24 session not found")
	ErrInvalidSessionToken    = errors.New("invalid or expired sep-24 session token")
	ErrInvalidWebhookSig      = errors.New("invalid webhook signature")
)

var validSep24Transitions = map[Sep24Status]map[Sep24Status]bool{
	Sep24StatusIncomplete: {
		Sep24StatusPendingUserTransferStart: true,
		Sep24StatusError:                    true,
	},
	Sep24StatusPendingUserTransferStart: {
		Sep24StatusPendingAnchor: true,
		Sep24StatusCompleted:     true,
		Sep24StatusError:         true,
	},
	Sep24StatusPendingAnchor: {
		Sep24StatusCompleted: true,
		Sep24StatusError:     true,
	},
	Sep24StatusCompleted: {},
	Sep24StatusError:     {},
}

// Sep24Session represents an interactive deposit or withdrawal transaction session.
type Sep24Session struct {
	ID             string      `json:"id"`
	Type           string      `json:"type"` // "deposit" | "withdraw"
	Status         Sep24Status `json:"status"`
	Account        string      `json:"account"`
	AssetCode      string      `json:"asset_code"`
	Amount         string      `json:"amount,omitempty"`
	ExternalTxID   string      `json:"external_tx_id,omitempty"`
	InteractiveURL string      `json:"interactive_url"`
	Message        string      `json:"message,omitempty"`
	CreatedAt      time.Time   `json:"created_at"`
	UpdatedAt      time.Time   `json:"updated_at"`
	CompletedAt    *time.Time  `json:"completed_at,omitempty"`
}

// Sep24SessionTokenPayload holds the claims inside an encrypted session token.
type Sep24SessionTokenPayload struct {
	TransactionID string `json:"tx_id"`
	Account       string `json:"account"`
	AssetCode     string `json:"asset_code"`
	Operation     string `json:"operation"`
	ExpiresAt     int64  `json:"exp"`
	Nonce         string `json:"nonce"`
}

// Sep24Store defines the persistence interface for SEP-24 sessions.
type Sep24Store interface {
	Save(ctx context.Context, session *Sep24Session) error
	Get(ctx context.Context, id string) (*Sep24Session, error)
	UpdateStatus(ctx context.Context, id string, from, to Sep24Status, externalTxID, message string) (*Sep24Session, error)
}

// MemorySep24Store provides an atomic in-memory store for SEP-24 sessions.
type MemorySep24Store struct {
	mu       sync.RWMutex
	sessions map[string]*Sep24Session
}

func NewMemorySep24Store() *MemorySep24Store {
	return &MemorySep24Store{
		sessions: make(map[string]*Sep24Session),
	}
}

func (s *MemorySep24Store) Save(_ context.Context, session *Sep24Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := *session
	s.sessions[session.ID] = &copied
	return nil
}

func (s *MemorySep24Store) Get(_ context.Context, id string) (*Sep24Session, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.sessions[id]
	if !ok {
		return nil, ErrSessionNotFound
	}
	copied := *sess
	return &copied, nil
}

func (s *MemorySep24Store) UpdateStatus(_ context.Context, id string, from, to Sep24Status, externalTxID, message string) (*Sep24Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sess, ok := s.sessions[id]
	if !ok {
		return nil, ErrSessionNotFound
	}

	if from != "" && sess.Status != from {
		return nil, fmt.Errorf("%w: current status %s does not match expected %s", ErrInvalidStateTransition, sess.Status, from)
	}

	allowed, ok := validSep24Transitions[sess.Status][to]
	if !ok || !allowed {
		return nil, fmt.Errorf("%w: cannot transition from %s to %s", ErrInvalidStateTransition, sess.Status, to)
	}

	sess.Status = to
	sess.UpdatedAt = time.Now().UTC()
	if externalTxID != "" {
		sess.ExternalTxID = externalTxID
	}
	if message != "" {
		sess.Message = message
	}
	if to == Sep24StatusCompleted {
		now := time.Now().UTC()
		sess.CompletedAt = &now
	}

	copied := *sess
	return &copied, nil
}

// Sep24CoordinatorConfig configures the session coordinator.
type Sep24CoordinatorConfig struct {
	Store              Sep24Store
	EncryptionKey      []byte // 32 bytes for AES-256-GCM
	InteractiveBaseURL string // e.g. "https://anchor.example.com/interactive"
	WebhookSecret      []byte
	SessionTTL         time.Duration
}

// Sep24Coordinator manages interactive deposit/withdrawal sessions and webview tokens.
type Sep24Coordinator struct {
	store              Sep24Store
	aead               cipher.AEAD
	interactiveBaseURL string
	webhookSecret      []byte
	sessionTTL         time.Duration
}

func NewSep24Coordinator(cfg Sep24CoordinatorConfig) (*Sep24Coordinator, error) {
	if len(cfg.EncryptionKey) != 32 {
		return nil, errors.New("encryption key must be exactly 32 bytes for AES-256")
	}

	block, err := aes.NewCipher(cfg.EncryptionKey)
	if err != nil {
		return nil, fmt.Errorf("init aes cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("init gcm cipher: %w", err)
	}

	store := cfg.Store
	if store == nil {
		store = NewMemorySep24Store()
	}

	sessionTTL := cfg.SessionTTL
	if sessionTTL <= 0 {
		sessionTTL = 15 * time.Minute
	}

	baseURL := strings.TrimRight(cfg.InteractiveBaseURL, "/")
	if !strings.HasPrefix(baseURL, "https://") {
		return nil, errors.New("interactive base URL must use HTTPS")
	}

	return &Sep24Coordinator{
		store:              store,
		aead:               gcm,
		interactiveBaseURL: baseURL,
		webhookSecret:      cfg.WebhookSecret,
		sessionTTL:         sessionTTL,
	}, nil
}

// GenerateSessionToken encrypts a short-lived token for embedding in interactive webviews.
func (c *Sep24Coordinator) GenerateSessionToken(txID, account, assetCode, operation string) (string, error) {
	now := time.Now().UTC()
	payload := Sep24SessionTokenPayload{
		TransactionID: txID,
		Account:       account,
		AssetCode:     assetCode,
		Operation:     operation,
		ExpiresAt:     now.Add(c.sessionTTL).Unix(),
		Nonce:         uuid.New().String(),
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate gcm nonce: %w", err)
	}

	ciphertext := c.aead.Seal(nonce, nonce, data, nil)
	return base64.RawURLEncoding.EncodeToString(ciphertext), nil
}

// ValidateSessionToken decrypts and validates an interactive session token.
func (c *Sep24Coordinator) ValidateSessionToken(tokenString string) (*Sep24SessionTokenPayload, error) {
	ciphertext, err := base64.RawURLEncoding.DecodeString(tokenString)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid encoding", ErrInvalidSessionToken)
	}

	nonceSize := c.aead.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("%w: token payload too short", ErrInvalidSessionToken)
	}

	nonce, encrypted := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := c.aead.Open(nil, nonce, encrypted, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: decryption failed", ErrInvalidSessionToken)
	}

	var payload Sep24SessionTokenPayload
	if err := json.Unmarshal(plaintext, &payload); err != nil {
		return nil, fmt.Errorf("%w: malformed payload", ErrInvalidSessionToken)
	}

	if time.Now().UTC().Unix() > payload.ExpiresAt {
		return nil, fmt.Errorf("%w: token expired", ErrInvalidSessionToken)
	}

	return &payload, nil
}

// InitiateDeposit starts a SEP-24 interactive deposit and returns the webview URL.
func (c *Sep24Coordinator) InitiateDeposit(ctx context.Context, req interactiveRequest) (*interactiveResponse, error) {
	if req.AssetCode == "" {
		return nil, errors.New("asset_code is required")
	}
	if req.Account == "" {
		return nil, errors.New("account is required")
	}
	if _, err := keypair.ParseAddress(req.Account); err != nil {
		return nil, fmt.Errorf("invalid destination account: %w", err)
	}

	txID := uuid.New().String()
	token, err := c.GenerateSessionToken(txID, req.Account, req.AssetCode, domain.AnchorTxTypeDeposit)
	if err != nil {
		return nil, fmt.Errorf("generate session token: %w", err)
	}

	interactiveURL := fmt.Sprintf("%s/deposit?transaction_id=%s&token=%s", c.interactiveBaseURL, txID, url.QueryEscape(token))

	now := time.Now().UTC()
	session := &Sep24Session{
		ID:             txID,
		Type:           domain.AnchorTxTypeDeposit,
		Status:         Sep24StatusIncomplete,
		Account:        req.Account,
		AssetCode:      req.AssetCode,
		Amount:         req.Amount,
		InteractiveURL: interactiveURL,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	if err := c.store.Save(ctx, session); err != nil {
		return nil, fmt.Errorf("save sep-24 session: %w", err)
	}

	return &interactiveResponse{
		Type: "interactive_customer_info_needed",
		URL:  interactiveURL,
		ID:   txID,
	}, nil
}

// InitiateWithdraw starts a SEP-24 interactive withdrawal and returns the webview URL.
func (c *Sep24Coordinator) InitiateWithdraw(ctx context.Context, req interactiveRequest) (*interactiveResponse, error) {
	if req.AssetCode == "" {
		return nil, errors.New("asset_code is required")
	}
	if req.Account == "" {
		return nil, errors.New("account is required")
	}
	if _, err := keypair.ParseAddress(req.Account); err != nil {
		return nil, fmt.Errorf("invalid source account: %w", err)
	}

	txID := uuid.New().String()
	token, err := c.GenerateSessionToken(txID, req.Account, req.AssetCode, domain.AnchorTxTypeWithdrawal)
	if err != nil {
		return nil, fmt.Errorf("generate session token: %w", err)
	}

	interactiveURL := fmt.Sprintf("%s/withdraw?transaction_id=%s&token=%s", c.interactiveBaseURL, txID, url.QueryEscape(token))

	now := time.Now().UTC()
	session := &Sep24Session{
		ID:             txID,
		Type:           domain.AnchorTxTypeWithdrawal,
		Status:         Sep24StatusIncomplete,
		Account:        req.Account,
		AssetCode:      req.AssetCode,
		Amount:         req.Amount,
		InteractiveURL: interactiveURL,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	if err := c.store.Save(ctx, session); err != nil {
		return nil, fmt.Errorf("save sep-24 session: %w", err)
	}

	return &interactiveResponse{
		Type: "interactive_customer_info_needed",
		URL:  interactiveURL,
		ID:   txID,
	}, nil
}

// Sep24WebhookPayload is sent by external anchor KYC engines to advance session state.
type Sep24WebhookPayload struct {
	TransactionID string      `json:"transaction_id"`
	Status        Sep24Status `json:"status"`
	ExternalTxID  string      `json:"external_tx_id,omitempty"`
	AmountIn      string      `json:"amount_in,omitempty"`
	AmountOut     string      `json:"amount_out,omitempty"`
	Message       string      `json:"message,omitempty"`
}

// HandleWebhook validates anchor webhook callback and advances the state machine atomically.
func (c *Sep24Coordinator) HandleWebhook(ctx context.Context, payloadBytes []byte, signature string) (*Sep24Session, error) {
	if len(c.webhookSecret) > 0 {
		if signature == "" {
			return nil, fmt.Errorf("%w: missing signature header", ErrInvalidWebhookSig)
		}
		mac := hmac.New(sha256.New, c.webhookSecret)
		mac.Write(payloadBytes)
		expectedSig := hex.EncodeToString(mac.Sum(nil))
		if !hmac.Equal([]byte(signature), []byte(expectedSig)) {
			return nil, fmt.Errorf("%w: signature mismatch", ErrInvalidWebhookSig)
		}
	}

	var payload Sep24WebhookPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal webhook payload: %w", err)
	}

	if payload.TransactionID == "" {
		return nil, errors.New("transaction_id is required in webhook payload")
	}

	current, err := c.store.Get(ctx, payload.TransactionID)
	if err != nil {
		return nil, err
	}

	updated, err := c.store.UpdateStatus(ctx, payload.TransactionID, current.Status, payload.Status, payload.ExternalTxID, payload.Message)
	if err != nil {
		return nil, err
	}

	return updated, nil
}

// GetSession fetches a session by its transaction ID.
func (c *Sep24Coordinator) GetSession(ctx context.Context, txID string) (*Sep24Session, error) {
	return c.store.Get(ctx, txID)
}

// -------------------------------------------------------------------------
// SEP-24 HTTP Handler
// -------------------------------------------------------------------------

type Sep24Handler struct {
	coordinator *Sep24Coordinator
}

func NewSep24Handler(coordinator *Sep24Coordinator) *Sep24Handler {
	return &Sep24Handler{coordinator: coordinator}
}

func (h *Sep24Handler) DepositInteractive(w http.ResponseWriter, r *http.Request) {
	var req interactiveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	resp, err := h.coordinator.InitiateDeposit(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *Sep24Handler) WithdrawInteractive(w http.ResponseWriter, r *http.Request) {
	var req interactiveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	resp, err := h.coordinator.InitiateWithdraw(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *Sep24Handler) Webhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read payload error", http.StatusBadRequest)
		return
	}
	sig := r.Header.Get("X-Anchor-Signature")
	session, err := h.coordinator.HandleWebhook(r.Context(), body, sig)
	if err != nil {
		if errors.Is(err, ErrInvalidWebhookSig) {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		if errors.Is(err, ErrInvalidStateTransition) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(session)
}

func (h *Sep24Handler) GetTransaction(w http.ResponseWriter, r *http.Request) {
	txID := r.URL.Query().Get("id")
	if txID == "" {
		http.Error(w, "id parameter required", http.StatusBadRequest)
		return
	}
	session, err := h.coordinator.GetSession(r.Context(), txID)
	if err != nil {
		if errors.Is(err, ErrSessionNotFound) {
			http.Error(w, "transaction not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"transaction": session,
	})
}
