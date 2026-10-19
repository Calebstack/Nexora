package anchor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/alerting"
	"github.com/Lumen-Nexora/Nexora/internal/crypto"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stellar/go/keypair"
)

// memCallbackStore mirrors the AnchorCallbackRepo semantics in memory.
type memCallbackStore struct {
	mu         sync.Mutex
	endpoints  map[string]*CallbackEndpoint
	deliveries map[string]*CallbackDelivery
}

func newMemCallbackStore() *memCallbackStore {
	return &memCallbackStore{endpoints: map[string]*CallbackEndpoint{}, deliveries: map[string]*CallbackDelivery{}}
}

func (s *memCallbackStore) CreateEndpoint(_ context.Context, e *CallbackEndpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e.ID = uuid.NewString()
	cp := *e
	s.endpoints[e.ID] = &cp
	return nil
}

func (s *memCallbackStore) Enqueue(_ context.Context, d *CallbackDelivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.endpoints[d.EndpointID]
	if !ok {
		return ErrCallbackEndpointNotFound
	}
	d.ID = uuid.NewString()
	d.State = CallbackPending
	if e.QuarantinedAt != nil {
		d.State = CallbackQuarantined
	}
	cp := *d
	s.deliveries[d.ID] = &cp
	return nil
}

func (s *memCallbackStore) ClaimDue(_ context.Context, now, leaseUntil time.Time, limit int) ([]ClaimedCallback, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var due []*CallbackDelivery
	for _, d := range s.deliveries {
		if d.State == CallbackPending && !d.NextAttemptAt.After(now) {
			due = append(due, d)
		}
	}
	sort.Slice(due, func(i, j int) bool { return due[i].NextAttemptAt.Before(due[j].NextAttemptAt) })
	if len(due) > limit {
		due = due[:limit]
	}
	out := make([]ClaimedCallback, 0, len(due))
	for _, d := range due {
		d.NextAttemptAt = leaseUntil
		out = append(out, ClaimedCallback{Delivery: *d, Endpoint: *s.endpoints[d.EndpointID]})
	}
	return out, nil
}

func (s *memCallbackStore) RecordSuccess(_ context.Context, deliveryID, endpointID string, attempts, statusCode int, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.deliveries[deliveryID]
	d.State, d.Attempts, d.LastStatusCode, d.LastError, d.DeliveredAt = CallbackDelivered, attempts, statusCode, "", &at
	if e := s.endpoints[endpointID]; e.QuarantinedAt == nil {
		e.ConsecutiveFailures = 0
	}
	return nil
}

func (s *memCallbackStore) RecordFailure(_ context.Context, f CallbackFailure) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.endpoints[f.EndpointID]
	e.ConsecutiveFailures++
	quarantined, just := e.QuarantinedAt != nil, false
	if !quarantined && e.ConsecutiveFailures >= f.QuarantineAfter {
		at := f.At
		e.QuarantinedAt = &at
		quarantined, just = true, true
	}
	d := s.deliveries[f.DeliveryID]
	d.Attempts, d.LastStatusCode, d.LastError = f.Attempts, f.StatusCode, f.Error
	switch {
	case quarantined:
		d.State = CallbackQuarantined
	case f.NextAttemptAt == nil:
		d.State = CallbackExpired
	default:
		d.State, d.NextAttemptAt = CallbackPending, *f.NextAttemptAt
	}
	if just {
		for _, other := range s.deliveries {
			if other.EndpointID == f.EndpointID && other.State == CallbackPending {
				other.State = CallbackQuarantined
			}
		}
	}
	return just, nil
}

func (s *memCallbackStore) ListQuarantined(_ context.Context, endpointID string, limit int) ([]CallbackDelivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []CallbackDelivery
	for _, d := range s.deliveries {
		if d.EndpointID == endpointID && d.State == CallbackQuarantined && len(out) < limit {
			out = append(out, *d)
		}
	}
	return out, nil
}

func (s *memCallbackStore) ReleaseQuarantine(_ context.Context, endpointID string, now, expiresAt time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.endpoints[endpointID]
	if !ok {
		return 0, ErrCallbackEndpointNotFound
	}
	e.QuarantinedAt, e.ConsecutiveFailures = nil, 0
	n := 0
	for _, d := range s.deliveries {
		if d.EndpointID == endpointID && d.State == CallbackQuarantined {
			d.State, d.Attempts, d.NextAttemptAt, d.ExpiresAt, d.LastError, d.LastStatusCode = CallbackPending, 0, now, expiresAt, "", 0
			n++
		}
	}
	return n, nil
}

func (s *memCallbackStore) delivery(id string) CallbackDelivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.deliveries[id]
}

func (s *memCallbackStore) endpoint(id string) CallbackEndpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.endpoints[id]
}

type countingRaiser struct {
	mu     sync.Mutex
	raised []alerting.Incident
}

func (c *countingRaiser) Raise(_ context.Context, inc alerting.Incident) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.raised = append(c.raised, inc)
	return nil
}

func (c *countingRaiser) Clear(context.Context, alerting.Incident) error { return nil }

// partnerServer records requests and answers with the current status.
type partnerServer struct {
	*httptest.Server
	status   atomic.Int32
	requests atomic.Int32
	mu       sync.Mutex
	last     *http.Request
	lastBody []byte
}

func newPartnerServer(t *testing.T, status int) *partnerServer {
	t.Helper()
	p := &partnerServer{}
	p.status.Store(int32(status))
	p.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		p.mu.Lock()
		p.last, p.lastBody = r, body
		p.mu.Unlock()
		p.requests.Add(1)
		w.WriteHeader(int(p.status.Load()))
	}))
	t.Cleanup(p.Close)
	return p
}

var (
	callbackKey    = []byte("0123456789abcdef0123456789abcdef")
	callbackSecret = "partner-shared-secret-0123456789abcdef"
)

type callbackFixture struct {
	now        time.Time
	store      *memCallbackStore
	alerts     *countingRaiser
	dispatcher *CallbackDispatcher
	partner    *partnerServer
	endpoint   *CallbackEndpoint
	signer     *keypair.Full
}

func newCallbackFixture(t *testing.T, status int, mutate func(*CallbackDispatcherConfig)) *callbackFixture {
	t.Helper()
	f := &callbackFixture{
		now:     time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC),
		store:   newMemCallbackStore(),
		alerts:  &countingRaiser{},
		partner: newPartnerServer(t, status),
		signer:  keypair.MustRandom(),
	}
	cfg := CallbackDispatcherConfig{
		EncryptionKey: callbackKey,
		SigningKey:    f.signer,
		Now:           func() time.Time { return f.now },
		Jitter:        func(d time.Duration) time.Duration { return d },
	}
	if mutate != nil {
		mutate(&cfg)
	}
	d, err := NewCallbackDispatcher(f.store, f.alerts, f.partner.Client(), cfg)
	if err != nil {
		t.Fatalf("NewCallbackDispatcher: %v", err)
	}
	f.dispatcher = d
	f.endpoint, err = d.RegisterEndpoint(context.Background(), "partner-a", f.partner.URL+"/callbacks", callbackSecret)
	if err != nil {
		t.Fatalf("RegisterEndpoint: %v", err)
	}
	return f
}

func (f *callbackFixture) notify(t *testing.T, txID, status string) *CallbackDelivery {
	t.Helper()
	d, err := f.dispatcher.Notify(context.Background(), f.endpoint.ID, 24,
		json.RawMessage(`{"id":"`+txID+`","kind":"withdrawal","status":"`+status+`"}`))
	if err != nil {
		t.Fatalf("Notify: %v", err)
	}
	return d
}

func (f *callbackFixture) process(t *testing.T) int {
	t.Helper()
	n, err := f.dispatcher.ProcessDue(context.Background())
	if err != nil {
		t.Fatalf("ProcessDue: %v", err)
	}
	return n
}

func TestCallbackDispatcher_SignedDelivery(t *testing.T) {
	f := newCallbackFixture(t, http.StatusNoContent, nil)
	d := f.notify(t, "tx-1", "pending_anchor")
	if f.process(t) != 1 {
		t.Fatal("expected one delivery")
	}

	got := f.store.delivery(d.ID)
	if got.State != CallbackDelivered || got.Attempts != 1 || got.LastStatusCode != http.StatusNoContent {
		t.Fatalf("delivery = %+v", got)
	}

	req, body := f.partner.last, f.partner.lastBody
	if string(body) != `{"transaction":{"id":"tx-1","kind":"withdrawal","status":"pending_anchor"}}` {
		t.Fatalf("body = %s", body)
	}
	ts := req.Header.Get(HeaderCallbackTimestampMs)
	if ts != strconv.FormatInt(f.now.UnixMilli(), 10) {
		t.Fatalf("timestamp = %s, want unix ms %d", ts, f.now.UnixMilli())
	}
	if want := SignCallback([]byte(callbackSecret), ts, body); req.Header.Get(HeaderCallbackSignature) != want {
		t.Fatalf("HMAC signature = %s, want %s", req.Header.Get(HeaderCallbackSignature), want)
	}
	if req.Header.Get(HeaderCallbackDeliveryID) != d.ID || req.Header.Get(HeaderCallbackAttempt) != "1" {
		t.Fatalf("delivery headers = %v", req.Header)
	}

	// SEP Signature: t=<unix seconds>, s=<base64 ed25519 over t.host.body>.
	parts := strings.SplitN(req.Header.Get(HeaderSEPSignature), ", ", 2)
	if len(parts) != 2 || parts[0] != "t="+strconv.FormatInt(f.now.Unix(), 10) || !strings.HasPrefix(parts[1], "s=") {
		t.Fatalf("Signature header = %q", req.Header.Get(HeaderSEPSignature))
	}
	sig, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(parts[1], "s="))
	if err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	host := mustHost(t, f.partner.URL)
	signed := []byte(strings.TrimPrefix(parts[0], "t=") + "." + host + "." + string(body))
	if err := f.signer.Verify(signed, sig); err != nil {
		t.Fatalf("SEP signature does not verify against SIGNING_KEY: %v", err)
	}
}

func mustHost(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u.Host
}

func TestCallbackDispatcher_ExponentialBackoffAcross72Hours(t *testing.T) {
	f := newCallbackFixture(t, http.StatusInternalServerError, func(c *CallbackDispatcherConfig) { c.QuarantineAfter = 1000 })
	d := f.notify(t, "tx-1", "completed")
	expires := d.ExpiresAt
	if !expires.Equal(f.now.Add(72 * time.Hour)) {
		t.Fatalf("expires = %s, want +72h", expires)
	}

	var gaps []time.Duration
	last := f.now
	for f.process(t) == 1 {
		got := f.store.delivery(d.ID)
		if got.State != CallbackPending {
			break
		}
		gaps = append(gaps, got.NextAttemptAt.Sub(f.now))
		last = got.NextAttemptAt
		f.now = got.NextAttemptAt
	}

	got := f.store.delivery(d.ID)
	if got.State != CallbackExpired {
		t.Fatalf("state = %s, want expired after the retry window", got.State)
	}
	if !last.Equal(expires) {
		t.Fatalf("final attempt at %s, want exactly at window end %s", last, expires)
	}
	if got.Attempts != 22 {
		t.Fatalf("attempts = %d, want 22 (10 doubling delays from 30s, 10 at the 6h cap, one final at 72h)", got.Attempts)
	}
	want := 30 * time.Second
	for i, g := range gaps[:10] {
		if g != want {
			t.Fatalf("gap %d = %s, want %s", i, g, want)
		}
		want *= 2
	}
	for i, g := range gaps[10:20] {
		if g != 6*time.Hour {
			t.Fatalf("gap %d = %s, want 6h cap", i+10, g)
		}
	}
}

func TestCallbackDispatcher_QuarantineAfter20ConsecutiveErrors(t *testing.T) {
	f := newCallbackFixture(t, http.StatusBadGateway, nil)
	var ids []string
	for i := 0; i < 25; i++ {
		ids = append(ids, f.notify(t, "tx-"+strconv.Itoa(i), "pending_external").ID)
	}

	// Each pass fails every claimed delivery; 25 pending exceed the threshold in one batch.
	f.process(t)

	e := f.store.endpoint(f.endpoint.ID)
	if e.QuarantinedAt == nil || e.ConsecutiveFailures < 20 {
		t.Fatalf("endpoint = %+v, want quarantined after 20 failures", e)
	}
	if len(f.alerts.raised) != 1 || f.alerts.raised[0].Severity != alerting.SeverityCritical {
		t.Fatalf("alerts = %+v, want exactly one critical alert", f.alerts.raised)
	}
	for _, id := range ids {
		if s := f.store.delivery(id).State; s != CallbackQuarantined {
			t.Fatalf("delivery %s state = %s, want quarantined", id, s)
		}
	}

	// New callbacks for a quarantined endpoint are held, not sent.
	held := f.notify(t, "tx-new", "completed")
	before := f.partner.requests.Load()
	f.now = f.now.Add(time.Hour)
	f.process(t)
	if f.store.delivery(held.ID).State != CallbackQuarantined || f.partner.requests.Load() != before {
		t.Fatal("quarantined endpoint must not receive deliveries")
	}
}

func TestCallbackDispatcher_SuccessResetsConsecutiveFailures(t *testing.T) {
	f := newCallbackFixture(t, http.StatusServiceUnavailable, func(c *CallbackDispatcherConfig) { c.BatchSize = 1 })
	for i := 0; i < 19; i++ {
		f.notify(t, "tx-"+strconv.Itoa(i), "pending_anchor")
		f.process(t)
	}
	if got := f.store.endpoint(f.endpoint.ID).ConsecutiveFailures; got != 19 {
		t.Fatalf("failures = %d, want 19", got)
	}
	f.partner.status.Store(http.StatusOK)
	f.notify(t, "tx-ok", "completed")
	f.process(t)
	e := f.store.endpoint(f.endpoint.ID)
	if e.ConsecutiveFailures != 0 || e.QuarantinedAt != nil {
		t.Fatalf("endpoint = %+v, want counter reset and no quarantine", e)
	}
}

func TestCallbackDispatcher_ReplayResendsQuarantined(t *testing.T) {
	f := newCallbackFixture(t, http.StatusInternalServerError, func(c *CallbackDispatcherConfig) { c.QuarantineAfter = 2 })
	a := f.notify(t, "tx-a", "completed")
	b := f.notify(t, "tx-b", "refunded")
	f.process(t)
	if f.store.delivery(a.ID).State != CallbackQuarantined || f.store.delivery(b.ID).State != CallbackQuarantined {
		t.Fatal("expected both deliveries quarantined")
	}

	f.partner.status.Store(http.StatusOK)
	f.now = f.now.Add(80 * time.Hour) // past the original window: replay grants a fresh one
	n, err := f.dispatcher.Replay(context.Background(), f.endpoint.ID)
	if err != nil || n != 2 {
		t.Fatalf("Replay = %d, %v; want 2", n, err)
	}
	if f.process(t) != 2 {
		t.Fatal("expected both replayed deliveries to be sent")
	}
	for _, id := range []string{a.ID, b.ID} {
		if d := f.store.delivery(id); d.State != CallbackDelivered || d.Attempts != 1 {
			t.Fatalf("delivery %s = %+v, want delivered on first replayed attempt", id, d)
		}
	}
	if _, err := f.dispatcher.Replay(context.Background(), uuid.NewString()); !errors.Is(err, ErrCallbackEndpointNotFound) {
		t.Fatalf("unknown endpoint err = %v", err)
	}
}

func TestCallbackDispatcher_RedirectIsADeliveryError(t *testing.T) {
	f := newCallbackFixture(t, http.StatusFound, nil)
	d := f.notify(t, "tx-1", "completed")
	f.process(t)
	got := f.store.delivery(d.ID)
	if got.State != CallbackPending || got.LastStatusCode != http.StatusFound {
		t.Fatalf("delivery = %+v, want a retried 302", got)
	}
}

func TestCallbackDispatcher_BoundedConcurrency(t *testing.T) {
	var inFlight, peak atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := inFlight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inFlight.Add(-1)
	}))
	defer srv.Close()

	store := newMemCallbackStore()
	d, err := NewCallbackDispatcher(store, &countingRaiser{}, srv.Client(), CallbackDispatcherConfig{EncryptionKey: callbackKey, Concurrency: 3})
	if err != nil {
		t.Fatal(err)
	}
	ep, err := d.RegisterEndpoint(context.Background(), "p", srv.URL, callbackSecret)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		if _, err := d.Notify(context.Background(), ep.ID, 31, json.RawMessage(`{"id":"t`+strconv.Itoa(i)+`","status":"pending_receiver"}`)); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := d.ProcessDue(context.Background()); err != nil || n != 12 {
		t.Fatalf("ProcessDue = %d, %v", n, err)
	}
	if peak.Load() > 3 {
		t.Fatalf("peak concurrency = %d, want <= 3", peak.Load())
	}
}

func TestCallbackDispatcher_Validation(t *testing.T) {
	f := newCallbackFixture(t, http.StatusOK, nil)
	ctx := context.Background()

	for _, tc := range []struct{ url, secret string }{
		{"http://partner.example/cb", callbackSecret},
		{"https://partner.example/cb", "short"},
		{"not a url", callbackSecret},
	} {
		if _, err := f.dispatcher.RegisterEndpoint(ctx, "p", tc.url, tc.secret); !errors.Is(err, ErrInvalidCallbackEndpoint) {
			t.Errorf("RegisterEndpoint(%q) err = %v", tc.url, err)
		}
	}

	stored := f.store.endpoint(f.endpoint.ID)
	if strings.Contains(string(stored.SecretCiphertext), callbackSecret) {
		t.Fatal("secret stored in plaintext")
	}
	if plain, err := crypto.Decrypt(stored.SecretCiphertext, callbackKey); err != nil || string(plain) != callbackSecret {
		t.Fatalf("decrypt stored secret: %q, %v", plain, err)
	}

	if _, err := f.dispatcher.Notify(ctx, f.endpoint.ID, 12, json.RawMessage(`{"id":"x","status":"completed"}`)); !errors.Is(err, ErrInvalidCallback) {
		t.Errorf("SEP-12 err = %v", err)
	}
	if _, err := f.dispatcher.Notify(ctx, f.endpoint.ID, 6, json.RawMessage(`{"id":"x"}`)); !errors.Is(err, ErrInvalidCallback) {
		t.Errorf("missing status err = %v", err)
	}
	if _, err := f.dispatcher.Notify(ctx, uuid.NewString(), 6, json.RawMessage(`{"id":"x","status":"completed"}`)); !errors.Is(err, ErrCallbackEndpointNotFound) {
		t.Errorf("unknown endpoint err = %v", err)
	}
	if _, err := NewCallbackDispatcher(f.store, f.alerts, nil, CallbackDispatcherConfig{EncryptionKey: []byte("short")}); !errors.Is(err, crypto.ErrKeyLength) {
		t.Errorf("short key err = %v", err)
	}
}

func TestCallbackAdminHandler(t *testing.T) {
	f := newCallbackFixture(t, http.StatusInternalServerError, func(c *CallbackDispatcherConfig) { c.QuarantineAfter = 1 })
	f.notify(t, "tx-1", "completed")
	f.process(t)

	r := chi.NewRouter()
	r.Route("/admin/anchor-callbacks", NewCallbackAdminHandler(f.dispatcher).AdminRoutes())
	do := func(method, path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
		return rec
	}

	rec := do(http.MethodGet, "/admin/anchor-callbacks/endpoints/"+f.endpoint.ID+"/quarantined")
	var list struct {
		Deliveries []quarantinedCallbackResponse `json:"deliveries"`
	}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &list) != nil || len(list.Deliveries) != 1 || list.Deliveries[0].TransactionID != "tx-1" {
		t.Fatalf("list: %d %s", rec.Code, rec.Body)
	}

	rec = do(http.MethodPost, "/admin/anchor-callbacks/endpoints/"+f.endpoint.ID+"/replay")
	if rec.Code != http.StatusAccepted || !strings.Contains(rec.Body.String(), `"replayed":1`) {
		t.Fatalf("replay: %d %s", rec.Code, rec.Body)
	}

	if rec = do(http.MethodPost, "/admin/anchor-callbacks/endpoints/not-a-uuid/replay"); rec.Code != http.StatusNotFound {
		t.Fatalf("bad id: %d", rec.Code)
	}
	if rec = do(http.MethodGet, "/admin/anchor-callbacks/endpoints/"+f.endpoint.ID+"/quarantined?limit=0"); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad limit: %d", rec.Code)
	}
}
