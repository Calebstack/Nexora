package anchor

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/alerting"
	"github.com/Lumen-Nexora/Nexora/internal/api"
	"github.com/Lumen-Nexora/Nexora/internal/crypto"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/stellar/go/keypair"
)

// Callback delivery headers. X-Nexora-Signature is "sha256=" + hex
// HMAC-SHA256(partner secret, X-Nexora-Timestamp-Ms + "." + raw body).
// When an anchor signing key is configured the SEP-6/24/31 standard
// "Signature: t=<unix seconds>, s=<base64 ed25519>" header is sent as well,
// signed over "<t>.<receiver host>.<raw body>".
const (
	HeaderCallbackSignature   = "X-Nexora-Signature"
	HeaderCallbackTimestampMs = "X-Nexora-Timestamp-Ms"
	HeaderCallbackDeliveryID  = "X-Nexora-Delivery-Id"
	HeaderCallbackAttempt     = "X-Nexora-Delivery-Attempt"
	HeaderSEPSignature        = "Signature"
)

// CallbackState is the lifecycle state of one callback delivery.
type CallbackState string

const (
	CallbackPending     CallbackState = "pending"
	CallbackDelivered   CallbackState = "delivered"
	CallbackQuarantined CallbackState = "quarantined"
	// CallbackExpired is the dead-letter state for deliveries that kept
	// failing for the whole retry window.
	CallbackExpired CallbackState = "expired"
)

var (
	ErrCallbackEndpointNotFound = errors.New("callback endpoint not found")
	ErrInvalidCallbackEndpoint  = errors.New("invalid callback endpoint")
	ErrInvalidCallback          = errors.New("invalid callback")
)

// minCallbackSecretBytes is the minimum shared-secret length (256 bits).
const minCallbackSecretBytes = 32

// CallbackEndpoint is a partner URL that receives transfer status callbacks.
type CallbackEndpoint struct {
	ID                  string
	Partner             string
	URL                 string
	SecretCiphertext    []byte
	ConsecutiveFailures int
	QuarantinedAt       *time.Time
	CreatedAt           time.Time
}

// CallbackDelivery is one status update queued for an endpoint. Payload is
// the exact body sent: {"transaction": <SEP transaction object>}.
type CallbackDelivery struct {
	ID                string
	EndpointID        string
	SEP               int
	TransactionID     string
	TransactionStatus string
	Payload           []byte
	State             CallbackState
	Attempts          int
	NextAttemptAt     time.Time
	ExpiresAt         time.Time
	LastError         string
	LastStatusCode    int
	CreatedAt         time.Time
	DeliveredAt       *time.Time
}

// ClaimedCallback is a delivery leased for sending, with its endpoint.
type ClaimedCallback struct {
	Delivery CallbackDelivery
	Endpoint CallbackEndpoint
}

// CallbackFailure records a failed attempt. A nil NextAttemptAt means the
// retry window is exhausted.
type CallbackFailure struct {
	DeliveryID      string
	EndpointID      string
	Attempts        int
	StatusCode      int
	Error           string
	NextAttemptAt   *time.Time
	QuarantineAfter int
	At              time.Time
}

// CallbackStore persists endpoints and the delivery outbox.
type CallbackStore interface {
	CreateEndpoint(ctx context.Context, e *CallbackEndpoint) error
	// Enqueue inserts d as pending, or as quarantined when its endpoint is
	// quarantined. It returns ErrCallbackEndpointNotFound for an unknown endpoint.
	Enqueue(ctx context.Context, d *CallbackDelivery) error
	// ClaimDue leases up to limit pending deliveries due at now by pushing
	// their next attempt to leaseUntil, skipping rows other workers hold.
	ClaimDue(ctx context.Context, now, leaseUntil time.Time, limit int) ([]ClaimedCallback, error)
	// RecordSuccess marks the delivery delivered and resets the endpoint's
	// consecutive failure count.
	RecordSuccess(ctx context.Context, deliveryID, endpointID string, attempts, statusCode int, at time.Time) error
	// RecordFailure stores the attempt and increments the endpoint's
	// consecutive failures. On reaching QuarantineAfter it quarantines the
	// endpoint and its pending deliveries; quarantined is true only for the
	// call that made that transition.
	RecordFailure(ctx context.Context, f CallbackFailure) (quarantined bool, err error)
	ListQuarantined(ctx context.Context, endpointID string, limit int) ([]CallbackDelivery, error)
	// ReleaseQuarantine lifts the endpoint's quarantine and requeues its
	// quarantined deliveries due at now with a fresh retry window ending at
	// expiresAt, returning how many were requeued.
	ReleaseQuarantine(ctx context.Context, endpointID string, now, expiresAt time.Time) (int, error)
}

// CallbackDispatcherConfig tunes delivery. Zero values fall back to defaults.
type CallbackDispatcherConfig struct {
	// EncryptionKey (32 bytes) encrypts partner secrets at rest.
	EncryptionKey []byte
	// SigningKey, when set, adds the SEP-standard Signature header. It must
	// be the key published as SIGNING_KEY in the anchor's stellar.toml.
	SigningKey *keypair.Full
	// RetryWindow is how long a delivery is retried (default 72h).
	RetryWindow time.Duration
	// BaseBackoff and MaxBackoff bound the exponential retry delay
	// (defaults 30s and 6h).
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	// QuarantineAfter is the consecutive failures that quarantine an
	// endpoint (default 20).
	QuarantineAfter int
	BatchSize       int
	Concurrency     int
	// Lease is how long a claimed delivery is hidden from other workers;
	// it must exceed the HTTP client timeout (default 2m).
	Lease time.Duration
	Now   func() time.Time
	// Jitter maps a backoff delay to the delay actually used. The default
	// draws uniformly from [d/2, d] so retries from one outage spread out.
	Jitter func(time.Duration) time.Duration
}

// CallbackDispatcher delivers SEP-6/24/31 transaction status callbacks to
// partner URLs from a durable outbox.
type CallbackDispatcher struct {
	store  CallbackStore
	alerts IncidentRaiser
	client *http.Client
	cfg    CallbackDispatcherConfig
}

func NewCallbackDispatcher(store CallbackStore, alerts IncidentRaiser, client *http.Client, cfg CallbackDispatcherConfig) (*CallbackDispatcher, error) {
	if len(cfg.EncryptionKey) != 32 {
		return nil, crypto.ErrKeyLength
	}
	if cfg.RetryWindow <= 0 {
		cfg.RetryWindow = 72 * time.Hour
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 30 * time.Second
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 6 * time.Hour
	}
	if cfg.QuarantineAfter <= 0 {
		cfg.QuarantineAfter = 20
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 16
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 2 * time.Minute
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Jitter == nil {
		cfg.Jitter = func(d time.Duration) time.Duration { return d/2 + rand.N(d/2+1) }
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	// Redirects are delivery errors: a partner must register the final URL.
	c := *client
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &CallbackDispatcher{store: store, alerts: alerts, client: &c, cfg: cfg}, nil
}

// RegisterEndpoint stores a partner callback URL with its shared secret,
// encrypted at rest. Only https URLs are accepted.
func (d *CallbackDispatcher) RegisterEndpoint(ctx context.Context, partner, rawURL, secret string) (*CallbackEndpoint, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("%w: url must be an absolute https URL", ErrInvalidCallbackEndpoint)
	}
	if partner == "" {
		return nil, fmt.Errorf("%w: partner is required", ErrInvalidCallbackEndpoint)
	}
	if len(secret) < minCallbackSecretBytes {
		return nil, fmt.Errorf("%w: secret must be at least %d bytes", ErrInvalidCallbackEndpoint, minCallbackSecretBytes)
	}
	ciphertext, err := crypto.Encrypt([]byte(secret), d.cfg.EncryptionKey)
	if err != nil {
		return nil, fmt.Errorf("encrypt callback secret: %w", err)
	}
	e := &CallbackEndpoint{Partner: partner, URL: u.String(), SecretCiphertext: ciphertext, CreatedAt: d.cfg.Now().UTC()}
	if err := d.store.CreateEndpoint(ctx, e); err != nil {
		return nil, err
	}
	return e, nil
}

// Notify queues a status callback. transaction is the SEP-6/24/31
// transaction object as returned by the transaction endpoint; it must carry
// "id" and "status". The body sent is {"transaction": transaction}.
func (d *CallbackDispatcher) Notify(ctx context.Context, endpointID string, sep int, transaction json.RawMessage) (*CallbackDelivery, error) {
	if sep != 6 && sep != 24 && sep != 31 {
		return nil, fmt.Errorf("%w: unsupported SEP %d", ErrInvalidCallback, sep)
	}
	var tx struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(transaction, &tx); err != nil || tx.ID == "" || tx.Status == "" {
		return nil, fmt.Errorf("%w: transaction object must include id and status", ErrInvalidCallback)
	}
	payload, err := json.Marshal(struct {
		Transaction json.RawMessage `json:"transaction"`
	}{transaction})
	if err != nil {
		return nil, fmt.Errorf("marshal callback payload: %w", err)
	}

	now := d.cfg.Now().UTC()
	delivery := &CallbackDelivery{
		EndpointID:        endpointID,
		SEP:               sep,
		TransactionID:     tx.ID,
		TransactionStatus: tx.Status,
		Payload:           payload,
		State:             CallbackPending,
		NextAttemptAt:     now,
		ExpiresAt:         now.Add(d.cfg.RetryWindow),
		CreatedAt:         now,
	}
	if err := d.store.Enqueue(ctx, delivery); err != nil {
		return nil, err
	}
	return delivery, nil
}

// Run delivers due callbacks until ctx is cancelled, polling every interval
// and draining back-to-back while full batches keep arriving.
func (d *CallbackDispatcher) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		n, err := d.ProcessDue(ctx)
		if err != nil {
			log.Error().Err(err).Msg("anchor callbacks: process due deliveries")
		}
		if n == d.cfg.BatchSize && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
}

// ProcessDue claims one batch and delivers it with at most Concurrency
// requests in flight, returning how many deliveries were attempted.
func (d *CallbackDispatcher) ProcessDue(ctx context.Context) (int, error) {
	now := d.cfg.Now().UTC()
	claimed, err := d.store.ClaimDue(ctx, now, now.Add(d.cfg.Lease), d.cfg.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("claim due callbacks: %w", err)
	}

	sem := make(chan struct{}, d.cfg.Concurrency)
	var wg sync.WaitGroup
	for i := range claimed {
		sem <- struct{}{}
		wg.Add(1)
		go func(c *ClaimedCallback) {
			defer func() { <-sem; wg.Done() }()
			if err := d.deliver(ctx, c); err != nil {
				log.Error().Err(err).Str("delivery_id", c.Delivery.ID).Msg("anchor callbacks: record outcome")
			}
		}(&claimed[i])
	}
	wg.Wait()
	return len(claimed), nil
}

func (d *CallbackDispatcher) deliver(ctx context.Context, c *ClaimedCallback) error {
	attempt := c.Delivery.Attempts + 1
	status, sendErr := d.send(ctx, c, attempt)
	now := d.cfg.Now().UTC()
	if sendErr == nil {
		return d.store.RecordSuccess(ctx, c.Delivery.ID, c.Endpoint.ID, attempt, status, now)
	}
	if ctx.Err() != nil {
		// Shutting down: not the partner's fault. The lease expires and the
		// delivery is retried without counting against the endpoint.
		return ctx.Err()
	}

	failure := CallbackFailure{
		DeliveryID:      c.Delivery.ID,
		EndpointID:      c.Endpoint.ID,
		Attempts:        attempt,
		StatusCode:      status,
		Error:           sendErr.Error(),
		QuarantineAfter: d.cfg.QuarantineAfter,
		At:              now,
	}
	if now.Before(c.Delivery.ExpiresAt) {
		next := now.Add(d.cfg.Jitter(d.backoff(attempt)))
		if next.After(c.Delivery.ExpiresAt) {
			next = c.Delivery.ExpiresAt // one final attempt at the end of the window
		}
		failure.NextAttemptAt = &next
	}
	quarantined, err := d.store.RecordFailure(ctx, failure)
	if err != nil {
		return err
	}
	if quarantined {
		return d.alerts.Raise(ctx, quarantineIncident(c.Endpoint, d.cfg.QuarantineAfter, failure.Error, now))
	}
	return nil
}

// send performs one signed POST and returns the HTTP status (0 when no
// response was received). Any non-2xx response is a delivery error.
func (d *CallbackDispatcher) send(ctx context.Context, c *ClaimedCallback, attempt int) (int, error) {
	secret, err := crypto.Decrypt(c.Endpoint.SecretCiphertext, d.cfg.EncryptionKey)
	if err != nil {
		return 0, fmt.Errorf("decrypt endpoint secret: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Endpoint.URL, bytes.NewReader(c.Delivery.Payload))
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}

	sentAt := d.cfg.Now()
	timestampMs := strconv.FormatInt(sentAt.UnixMilli(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderCallbackDeliveryID, c.Delivery.ID)
	req.Header.Set(HeaderCallbackAttempt, strconv.Itoa(attempt))
	req.Header.Set(HeaderCallbackTimestampMs, timestampMs)
	req.Header.Set(HeaderCallbackSignature, SignCallback(secret, timestampMs, c.Delivery.Payload))
	if d.cfg.SigningKey != nil {
		sig, err := SignSEPCallback(d.cfg.SigningKey, sentAt, req.URL.Host, c.Delivery.Payload)
		if err != nil {
			return 0, fmt.Errorf("sign SEP callback: %w", err)
		}
		req.Header.Set(HeaderSEPSignature, sig)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("post callback: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("partner returned HTTP %d", resp.StatusCode)
	}
	return resp.StatusCode, nil
}

// backoff returns min(BaseBackoff * 2^(attempt-1), MaxBackoff).
func (d *CallbackDispatcher) backoff(attempt int) time.Duration {
	delay := d.cfg.BaseBackoff
	for i := 1; i < attempt && delay < d.cfg.MaxBackoff; i++ {
		delay *= 2
	}
	return min(delay, d.cfg.MaxBackoff)
}

// ListQuarantined returns up to limit quarantined deliveries for an endpoint.
func (d *CallbackDispatcher) ListQuarantined(ctx context.Context, endpointID string, limit int) ([]CallbackDelivery, error) {
	return d.store.ListQuarantined(ctx, endpointID, limit)
}

// Replay lifts an endpoint's quarantine, resets its failure count and
// requeues every quarantined delivery with a fresh retry window.
func (d *CallbackDispatcher) Replay(ctx context.Context, endpointID string) (int, error) {
	now := d.cfg.Now().UTC()
	return d.store.ReleaseQuarantine(ctx, endpointID, now, now.Add(d.cfg.RetryWindow))
}

// SignCallback returns "sha256=" + hex HMAC-SHA256(secret, timestampMs + "." + body).
func SignCallback(secret []byte, timestampMs string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(timestampMs))
	mac.Write([]byte("."))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// SignSEPCallback builds the SEP-6/24/31 callback Signature header value
// "t=<unix seconds>, s=<base64 signature>" over "<t>.<host>.<body>".
func SignSEPCallback(kp *keypair.Full, at time.Time, host string, body []byte) (string, error) {
	t := strconv.FormatInt(at.Unix(), 10)
	sig, err := kp.Sign([]byte(t + "." + host + "." + string(body)))
	if err != nil {
		return "", err
	}
	return "t=" + t + ", s=" + base64.StdEncoding.EncodeToString(sig), nil
}

func quarantineIncident(e CallbackEndpoint, threshold int, lastErr string, at time.Time) alerting.Incident {
	return alerting.Incident{
		DedupKey: "anchor-callback-quarantine:" + e.ID,
		Summary:  fmt.Sprintf("Anchor callback endpoint for %s quarantined after %d consecutive delivery errors", e.Partner, threshold),
		Source:   "nexora-anchor-callbacks",
		Severity: alerting.SeverityCritical,
		Details: map[string]any{
			"endpoint_id": e.ID,
			"partner":     e.Partner,
			"url":         e.URL,
			"last_error":  lastErr,
		},
		Timestamp: at,
	}
}

// CallbackAdminHandler exposes quarantine inspection and replay for
// platform operators.
type CallbackAdminHandler struct {
	dispatcher *CallbackDispatcher
}

func NewCallbackAdminHandler(dispatcher *CallbackDispatcher) *CallbackAdminHandler {
	return &CallbackAdminHandler{dispatcher: dispatcher}
}

// AdminRoutes mounts, under the caller's operator-only prefix:
//
//	GET  /endpoints/{id}/quarantined
//	POST /endpoints/{id}/replay
func (h *CallbackAdminHandler) AdminRoutes() func(r chi.Router) {
	return func(r chi.Router) {
		r.Get("/endpoints/{id}/quarantined", h.listQuarantined)
		r.Post("/endpoints/{id}/replay", h.replay)
	}
}

type quarantinedCallbackResponse struct {
	ID                string `json:"id"`
	SEP               int    `json:"sep"`
	TransactionID     string `json:"transaction_id"`
	TransactionStatus string `json:"transaction_status"`
	Attempts          int    `json:"attempts"`
	LastError         string `json:"last_error,omitempty"`
	LastStatusCode    int    `json:"last_status_code,omitempty"`
	CreatedAt         string `json:"created_at"`
}

// endpointID returns the {id} URL parameter, writing 404 when it is not a UUID.
func endpointID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := chi.URLParam(r, "id")
	if _, err := uuid.Parse(id); err != nil {
		api.NotFound(w, "callback endpoint not found")
		return "", false
	}
	return id, true
}

func (h *CallbackAdminHandler) listQuarantined(w http.ResponseWriter, r *http.Request) {
	id, ok := endpointID(w, r)
	if !ok {
		return
	}
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 500 {
			api.BadRequest(w, "limit must be between 1 and 500")
			return
		}
		limit = n
	}
	deliveries, err := h.dispatcher.ListQuarantined(r.Context(), id, limit)
	if err != nil {
		api.InternalError(w, err)
		return
	}
	out := make([]quarantinedCallbackResponse, 0, len(deliveries))
	for _, dl := range deliveries {
		out = append(out, quarantinedCallbackResponse{
			ID:                dl.ID,
			SEP:               dl.SEP,
			TransactionID:     dl.TransactionID,
			TransactionStatus: dl.TransactionStatus,
			Attempts:          dl.Attempts,
			LastError:         dl.LastError,
			LastStatusCode:    dl.LastStatusCode,
			CreatedAt:         dl.CreatedAt.Format(time.RFC3339Nano),
		})
	}
	api.JSON(w, http.StatusOK, map[string]any{"deliveries": out})
}

func (h *CallbackAdminHandler) replay(w http.ResponseWriter, r *http.Request) {
	id, ok := endpointID(w, r)
	if !ok {
		return
	}
	n, err := h.dispatcher.Replay(r.Context(), id)
	if err != nil {
		if errors.Is(err, ErrCallbackEndpointNotFound) {
			api.NotFound(w, "callback endpoint not found")
			return
		}
		api.InternalError(w, err)
		return
	}
	api.JSON(w, http.StatusAccepted, map[string]int{"replayed": n})
}
