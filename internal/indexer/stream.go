package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/stellar"
	"github.com/rs/zerolog/log"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/protocols/horizon/operations"
)

// HorizonStreamer is satisfied by *stellar.HorizonStreamer.
type HorizonStreamer interface {
	Stream(ctx context.Context, path string, query url.Values, lastEventID string, onActivity func(), onEvent func(stellar.HorizonEvent) error) error
}

// StreamConfig tunes a ResilientStream. Zero values fall back to defaults.
type StreamConfig struct {
	// BaseBackoff and MaxBackoff bound the full-jitter reconnect delay
	// (defaults 1s and 60s).
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	// HeartbeatInterval is how often the stream emits its health metric
	// (default 10s).
	HeartbeatInterval time.Duration
	// randN returns a uniform value in [0, n); tests make it deterministic.
	randN func(n int64) int64
}

// StreamMetrics is a point-in-time health snapshot of one stream.
type StreamMetrics struct {
	Name           string
	Connected      bool
	Cursor         string
	Events         int64
	Reconnects     int64
	LastActivityAt time.Time
	LastEventAt    time.Time
}

// ResilientStream keeps one Horizon SSE stream alive. It resumes from the
// last committed cursor after every disconnect, waits a full-jitter
// exponential backoff between attempts, and emits a heartbeat metric so a
// silent connection is visible. Delivery is at-least-once: an event is
// re-delivered if the process stops between handling it and committing its
// cursor, so handlers must be idempotent.
type ResilientStream struct {
	name     string
	streamer HorizonStreamer
	path     string
	query    url.Values
	handle   func(ctx context.Context, ev stellar.HorizonEvent) error
	commit   func(ctx context.Context, cursor string) error
	cfg      StreamConfig

	mu           sync.Mutex
	cursor       string
	connected    atomic.Bool
	events       atomic.Int64
	reconnects   atomic.Int64
	lastActivity atomic.Int64 // unix nanoseconds
	lastEvent    atomic.Int64
}

// NewResilientStream builds a stream for a Horizon path. handle processes an
// event; commit persists the cursor after handle succeeds.
func NewResilientStream(
	name string,
	streamer HorizonStreamer,
	path string,
	query url.Values,
	cursor string,
	handle func(ctx context.Context, ev stellar.HorizonEvent) error,
	commit func(ctx context.Context, cursor string) error,
	cfg StreamConfig,
) *ResilientStream {
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = time.Second
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = time.Minute
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 10 * time.Second
	}
	if cfg.randN == nil {
		cfg.randN = rand.Int64N
	}
	return &ResilientStream{name: name, streamer: streamer, path: path, query: query, cursor: cursor, handle: handle, commit: commit, cfg: cfg}
}

// AccountPaymentsPath and AccountTransactionsPath are the Horizon streaming
// endpoints for one account.
func AccountPaymentsPath(accountID string) string {
	return "/accounts/" + url.PathEscape(accountID) + "/payments"
}

func AccountTransactionsPath(accountID string) string {
	return "/accounts/" + url.PathEscape(accountID) + "/transactions"
}

// NewWalletPaymentStream streams /accounts/{id}/payments for a wallet into
// the indexer's payment processing, committing the wallet's sync cursor after
// each payment. Payments are joined with their transactions for memo lookup.
func (idx *Indexer) NewWalletPaymentStream(streamer HorizonStreamer, w *domain.Wallet, cfg StreamConfig) *ResilientStream {
	return NewResilientStream(
		"payments:"+w.ID,
		streamer,
		AccountPaymentsPath(w.PublicKey),
		url.Values{"join": {"transactions"}},
		w.SyncCursor,
		func(ctx context.Context, ev stellar.HorizonEvent) error {
			op, err := DecodePayment(ev)
			if err != nil {
				return err
			}
			return idx.processPayment(ctx, w, op)
		},
		func(ctx context.Context, cursor string) error {
			if err := idx.walletRepo.UpdateSyncCursor(ctx, w.ID, cursor); err != nil {
				return err
			}
			w.SyncCursor = cursor
			return nil
		},
		cfg,
	)
}

// DecodePayment decodes a payments stream event into its concrete operation.
func DecodePayment(ev stellar.HorizonEvent) (operations.Operation, error) {
	var head struct {
		TypeI int32 `json:"type_i"`
	}
	if err := json.Unmarshal(ev.Data, &head); err != nil {
		return nil, fmt.Errorf("decode payment event %s: %w", ev.ID, err)
	}
	op, err := operations.UnmarshalOperation(head.TypeI, ev.Data)
	if err != nil {
		return nil, fmt.Errorf("decode payment event %s: %w", ev.ID, err)
	}
	return op, nil
}

// DecodeTransaction decodes a transactions stream event.
func DecodeTransaction(ev stellar.HorizonEvent) (horizon.Transaction, error) {
	var tx horizon.Transaction
	if err := json.Unmarshal(ev.Data, &tx); err != nil {
		return horizon.Transaction{}, fmt.Errorf("decode transaction event %s: %w", ev.ID, err)
	}
	return tx, nil
}

// Run streams until ctx is cancelled or the account does not exist (HTTP
// 404), which is returned. Every other failure reconnects.
func (s *ResilientStream) Run(ctx context.Context) error {
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	go s.heartbeat(heartbeatCtx)

	attempt := 0
	for {
		healthy := false
		err := s.streamer.Stream(ctx, s.path, s.query, s.Cursor(),
			func() {
				healthy = true
				s.connected.Store(true)
				s.lastActivity.Store(time.Now().UnixNano())
			},
			func(ev stellar.HorizonEvent) error { return s.apply(ctx, ev) },
		)
		s.connected.Store(false)
		if ctx.Err() != nil {
			return nil
		}
		if stellar.IsNotFound(err) {
			return fmt.Errorf("stream %s: %w", s.name, err)
		}
		if healthy {
			attempt = 0 // the connection was up; restart the backoff schedule
		}

		delay := time.Duration(0)
		if !errors.Is(err, stellar.ErrStreamClosed) {
			// Horizon's "close" event asks for an immediate reconnect; every
			// other failure backs off.
			delay = s.backoff(attempt)
			attempt++
			if status, ok := stellar.HTTPStatus(err); ok && status == http.StatusTooManyRequests {
				if retryAfter, ok := stellar.RetryAfter(err); ok && retryAfter > delay {
					delay = retryAfter
				}
			}
			log.Warn().Err(err).Str("stream", s.name).Str("cursor", s.Cursor()).Dur("retry_in", delay).
				Msg("indexer: horizon stream disconnected, reconnecting")
		}
		s.reconnects.Add(1)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
	}
}

// apply handles one event and commits its cursor. A failure ends the
// connection so the event is re-delivered from the last committed cursor.
func (s *ResilientStream) apply(ctx context.Context, ev stellar.HorizonEvent) error {
	if err := s.handle(ctx, ev); err != nil {
		return fmt.Errorf("handle event %s: %w", ev.ID, err)
	}
	if ev.ID != "" {
		if err := s.commit(ctx, ev.ID); err != nil {
			return fmt.Errorf("commit cursor %s: %w", ev.ID, err)
		}
		s.mu.Lock()
		s.cursor = ev.ID
		s.mu.Unlock()
	}
	s.events.Add(1)
	s.lastEvent.Store(time.Now().UnixNano())
	return nil
}

// backoff returns a full-jitter delay: uniform in [0, min(MaxBackoff,
// BaseBackoff * 2^attempt)].
func (s *ResilientStream) backoff(attempt int) time.Duration {
	ceiling := s.cfg.MaxBackoff
	if attempt < 62 {
		if exp := s.cfg.BaseBackoff << attempt; exp > 0 && exp < ceiling {
			ceiling = exp
		}
	}
	return time.Duration(s.cfg.randN(int64(ceiling) + 1))
}

func (s *ResilientStream) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m := s.Metrics()
			idle := time.Duration(0)
			if !m.LastActivityAt.IsZero() {
				idle = time.Since(m.LastActivityAt)
			}
			log.Info().Str("stream", m.Name).Bool("connected", m.Connected).Str("cursor", m.Cursor).
				Int64("events", m.Events).Int64("reconnects", m.Reconnects).
				Float64("idle_seconds", idle.Seconds()).Msg("indexer: horizon stream heartbeat")
		}
	}
}

// Cursor returns the last committed cursor.
func (s *ResilientStream) Cursor() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor
}

// Metrics returns the stream's current health snapshot.
func (s *ResilientStream) Metrics() StreamMetrics {
	return StreamMetrics{
		Name:           s.name,
		Connected:      s.connected.Load(),
		Cursor:         s.Cursor(),
		Events:         s.events.Load(),
		Reconnects:     s.reconnects.Load(),
		LastActivityAt: unixNano(s.lastActivity.Load()),
		LastEventAt:    unixNano(s.lastEvent.Load()),
	}
}

func unixNano(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}
