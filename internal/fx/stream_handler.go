package fx

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/api"
)

const (
	minStreamInterval = 100 * time.Millisecond
	maxStreamInterval = time.Hour
	streamPollResolution = 50 * time.Millisecond
)

// liveRateService is deliberately optional so existing Service implementations
// (including integrations) remain source compatible. Production service uses
// GetLiveRates while test and alternate implementations can use GetRates.
type liveRateService interface {
	GetLiveRates(context.Context, string, string) (*RateResponse, error)
}

type rateFetcher func(context.Context, string, string) (*RateResponse, error)

type StreamTick struct {
	Pair string        `json:"pair"`
	Rate *RateResponse `json:"rate"`
	At   time.Time     `json:"at"`
}

type streamSubscriber struct {
	id       uint64
	pairs    map[string]struct{}
	interval time.Duration
	lastSent time.Time
	updates  chan StreamTick // one slot: slow clients receive the newest tick.
}

type streamPair struct {
	from, to    string
	subscribers map[uint64]*streamSubscriber
	nextPoll    time.Time
}

// QuoteStream multiplexes subscriptions by currency pair. It uses one polling
// loop for the whole process, rather than a rate-fetching goroutine per SSE
// connection; the handler goroutine is released as soon as a client leaves.
type QuoteStream struct {
	mu          sync.Mutex
	fetch       rateFetcher
	pairs       map[string]*streamPair
	subscribers map[uint64]*streamSubscriber
	nextID      uint64
	done        chan struct{}
	closeOnce   sync.Once
}

func NewQuoteStream(svc Service) *QuoteStream {
	var fetch rateFetcher
	if svc != nil {
		fetch = svc.GetRates
		if live, ok := svc.(liveRateService); ok {
			fetch = live.GetLiveRates
		}
	}
	s := &QuoteStream{
		fetch:       fetch,
		pairs:       make(map[string]*streamPair),
		subscribers: make(map[uint64]*streamSubscriber),
		done:        make(chan struct{}),
	}
	go s.run()
	return s
}

func (s *QuoteStream) Close() {
	s.closeOnce.Do(func() { close(s.done) })
}

func (s *QuoteStream) Subscribe(pairs []string, interval time.Duration) (<-chan StreamTick, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	sub := &streamSubscriber{id: s.nextID, pairs: make(map[string]struct{}, len(pairs)), interval: interval, updates: make(chan StreamTick, 1)}
	for _, pair := range pairs {
		from, to, ok := splitPair(pair)
		if !ok {
			continue
		}
		key := from + ":" + to
		entry := s.pairs[key]
		if entry == nil {
			entry = &streamPair{from: from, to: to, subscribers: make(map[uint64]*streamSubscriber)}
			s.pairs[key] = entry
		}
		entry.subscribers[sub.id] = sub
		entry.nextPoll = time.Time{} // subscribe gets the next available tick.
		sub.pairs[key] = struct{}{}
	}
	s.subscribers[sub.id] = sub

	var once sync.Once
	return sub.updates, func() {
		once.Do(func() { s.unsubscribe(sub.id) })
	}
}

func (s *QuoteStream) unsubscribe(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub := s.subscribers[id]
	if sub == nil {
		return
	}
	for pair := range sub.pairs {
		entry := s.pairs[pair]
		if entry == nil {
			continue
		}
		delete(entry.subscribers, id)
		if len(entry.subscribers) == 0 {
			delete(s.pairs, pair)
		}
	}
	delete(s.subscribers, id)
}

func (s *QuoteStream) run() {
	ticker := time.NewTicker(streamPollResolution)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case now := <-ticker.C:
			s.pollDue(now)
		}
	}
}

type duePair struct{ key, from, to string }

func (s *QuoteStream) pollDue(now time.Time) {
	s.mu.Lock()
	due := make([]duePair, 0, len(s.pairs))
	for key, pair := range s.pairs {
		if pair.nextPoll.IsZero() || !now.Before(pair.nextPoll) {
			// Set a provisional value before I/O so an overloaded provider cannot
			// cause overlapping polls for this pair.
			pair.nextPoll = now.Add(minimumPairInterval(pair))
			due = append(due, duePair{key, pair.from, pair.to})
		}
	}
	s.mu.Unlock()

	for _, pair := range due {
		if s.fetch == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		rate, err := s.fetch(ctx, pair.from, pair.to)
		cancel()
		if err != nil {
			continue // A later poll retries without terminating subscribers.
		}
		s.publish(pair.key, rate, now)
	}
}

// Close releases the shared quote poller. Server shutdown calls this so the
// worker cannot outlive the HTTP server.
func (h *Handler) Close() {
	if h != nil && h.stream != nil {
		h.stream.Close()
	}
}

func minimumPairInterval(pair *streamPair) time.Duration {
	interval := maxStreamInterval
	for _, sub := range pair.subscribers {
		if sub.interval < interval {
			interval = sub.interval
		}
	}
	return interval
}

func (s *QuoteStream) publish(pair string, rate *RateResponse, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.pairs[pair]
	if entry == nil {
		return
	}
	for _, sub := range entry.subscribers {
		if !sub.lastSent.IsZero() && at.Sub(sub.lastSent) < sub.interval {
			continue
		}
		sub.lastSent = at
		tick := StreamTick{Pair: pair, Rate: rate, At: at.UTC()}
		select {
		case sub.updates <- tick:
		default:
			// Never let a slow socket delay other clients or provider polling.
			select { case <-sub.updates: default: }
			select { case sub.updates <- tick: default: }
		}
	}
}

func splitPair(raw string) (string, string, bool) {
	parts := strings.FieldsFunc(strings.ToUpper(strings.TrimSpace(raw)), func(r rune) bool { return r == ':' || r == '-' || r == '/' })
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func parseStreamPairs(r *http.Request) ([]string, error) {
	raw := r.URL.Query().Get("pairs")
	if raw == "" {
		from, to := r.URL.Query().Get("from"), r.URL.Query().Get("to")
		if from != "" && to != "" {
			raw = from + ":" + to
		}
	}
	if raw == "" {
		return nil, fmt.Errorf("pairs (or from and to) query parameter is required")
	}
	seen := make(map[string]struct{})
	pairs := make([]string, 0)
	for _, pair := range strings.Split(raw, ",") {
		from, to, ok := splitPair(pair)
		if !ok {
			return nil, fmt.Errorf("invalid currency pair %q", pair)
		}
		from, to, err := validateFXPair(from, to)
		if err != nil {
			return nil, fmt.Errorf("invalid currency pair %q", pair)
		}
		canonical := from + ":" + to
		if _, ok := seen[canonical]; !ok {
			seen[canonical] = struct{}{}
			pairs = append(pairs, canonical)
		}
	}
	return pairs, nil
}

func parseStreamDuration(raw string, fallback time.Duration) (time.Duration, error) {
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < minStreamInterval || d > maxStreamInterval {
		return 0, fmt.Errorf("must be between %s and %s", minStreamInterval, maxStreamInterval)
	}
	return d, nil
}

func (h *Handler) streamQuotes(w http.ResponseWriter, r *http.Request) {
	if h.stream == nil || h.stream.fetch == nil {
		api.Error(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "FX quote streaming is unavailable")
		return
	}
	pairs, err := parseStreamPairs(r)
	if err != nil {
		api.BadRequest(w, err.Error())
		return
	}
	interval, err := parseStreamDuration(r.URL.Query().Get("interval"), time.Second)
	if err != nil {
		api.BadRequest(w, "invalid interval: "+err.Error())
		return
	}
	heartbeat, err := parseStreamDuration(r.URL.Query().Get("heartbeat"), 15*time.Second)
	if err != nil {
		api.BadRequest(w, "invalid heartbeat: "+err.Error())
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	// The API server has a normal write deadline for JSON responses. Clear it
	// only for this long-lived response; heartbeat writes still surface dead
	// connections promptly.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{})

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	updates, unsubscribe := h.stream.Subscribe(pairs, interval)
	defer unsubscribe()
	heartbeats := time.NewTicker(heartbeat)
	defer heartbeats.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case tick := <-updates:
			data, err := json.Marshal(tick)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "event: rate\ndata: %s\n\n", data); err != nil {
				return
			}
			flusher.Flush()
		case <-heartbeats.C:
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
