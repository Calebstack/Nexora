package indexer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/stellar"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stellar/go/protocols/horizon/operations"
)

const streamAccount = "GSTREAMACCOUNT"

// sseHorizon is a fake Horizon payments stream. Records have paging tokens
// 1..total; a connection serves records after its cursor, at most perConn of
// them, then ends the way the configured behaviour says.
type sseHorizon struct {
	mu       sync.Mutex
	total    int
	perConn  int
	ending   string // "drop" (abrupt TCP close), "close" (Horizon close event), "stall"
	requests []http.Header
	cursors  []string
	status   int // non-zero: answer every request with this status
}

func (h *sseHorizon) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.requests = append(h.requests, r.Header.Clone())
	cursor := r.URL.Query().Get("cursor")
	h.cursors = append(h.cursors, cursor)
	status, total, perConn, ending := h.status, h.total, h.perConn, h.ending
	h.mu.Unlock()

	if r.URL.Path != AccountPaymentsPath(streamAccount) || r.Header.Get("Accept") != "text/event-stream" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if status != 0 {
		w.Header().Set("Retry-After", "1")
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(status)
		fmt.Fprintf(w, `{"type":"x","title":"t","status":%d}`, status)
		return
	}

	start := 0
	if lastID := r.Header.Get("Last-Event-ID"); lastID != "" {
		start, _ = strconv.Atoi(lastID)
	} else if cursor != "now" {
		start, _ = strconv.Atoi(cursor)
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	flusher := w.(http.Flusher)
	// Horizon's preamble.
	fmt.Fprint(w, "retry: 1000\nevent: open\ndata: \"hello\"\n\n")
	flusher.Flush()

	sent := 0
	for id := start + 1; id <= total && sent < perConn; id++ {
		fmt.Fprintf(w, "id: %d\ndata: %s\n\n", id, paymentJSON(id))
		flusher.Flush()
		sent++
	}

	switch {
	case start+sent >= total || ending == "stall":
		<-r.Context().Done() // caught up (or stalled): stay open like Horizon
	case ending == "close":
		fmt.Fprint(w, "retry: 10\nevent: close\ndata: \"byebye\"\n\n")
		flusher.Flush()
	default: // "drop": kill the TCP connection without an SSE goodbye
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	}
}

func (h *sseHorizon) requestCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.requests)
}

func paymentJSON(id int) string {
	return fmt.Sprintf(`{"id":"%d","paging_token":"%d","transaction_successful":true,"source_account":"GSRC",`+
		`"type":"payment","type_i":1,"transaction_hash":"hash%d","asset_type":"native","from":"GSRC","to":"%s","amount":"1.0000000"}`,
		id, id, id, streamAccount)
}

// collector records handled payment IDs and committed cursors.
type collector struct {
	mu       sync.Mutex
	handled  []string
	commits  []string
	failOnce map[string]bool
	done     chan struct{}
	want     int
}

func newCollector(want int) *collector {
	return &collector{failOnce: map[string]bool{}, done: make(chan struct{}), want: want}
}

func (c *collector) handle(_ context.Context, ev stellar.HorizonEvent) error {
	op, err := DecodePayment(ev)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failOnce[op.GetID()] {
		delete(c.failOnce, op.GetID())
		return errors.New("database unavailable")
	}
	c.handled = append(c.handled, op.GetID())
	return nil
}

func (c *collector) commit(_ context.Context, cursor string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commits = append(c.commits, cursor)
	if len(c.commits) == c.want {
		close(c.done)
	}
	return nil
}

func fastStreamConfig() StreamConfig {
	return StreamConfig{BaseBackoff: 5 * time.Millisecond, MaxBackoff: 20 * time.Millisecond, HeartbeatInterval: time.Hour}
}

func runStream(t *testing.T, s *ResilientStream, c *collector, timeout time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx) }()
	select {
	case <-c.done:
	case <-time.After(timeout):
		t.Errorf("timed out: handled %v", c.handled)
	}
	cancel()
	return <-errc
}

func assertSequential(t *testing.T, got []string, n int) {
	t.Helper()
	if len(got) != n {
		t.Fatalf("handled %d events, want %d: %v", len(got), n, got)
	}
	for i, id := range got {
		if id != strconv.Itoa(i+1) {
			t.Fatalf("event %d = %s; want exactly-once, in order: %v", i, id, got)
		}
	}
}

func TestResilientStream_ResumesFromCursorAfterNetworkDrop(t *testing.T) {
	h := &sseHorizon{total: 10, perConn: 3, ending: "drop"}
	srv := httptest.NewServer(h)
	defer srv.Close()

	c := newCollector(10)
	s := NewResilientStream("test", stellar.NewHorizonStreamer(srv.URL, 0), AccountPaymentsPath(streamAccount), nil, "", c.handle, c.commit, fastStreamConfig())
	if err := runStream(t, s, c, 5*time.Second); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertSequential(t, c.handled, 10)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cursors[0] != "now" || h.requests[0].Get("Last-Event-ID") != "" {
		t.Fatalf("first connection cursor = %q, want now", h.cursors[0])
	}
	for i, want := range []string{"3", "6", "9"} {
		if got := h.requests[i+1].Get("Last-Event-ID"); got != want || h.cursors[i+1] != want {
			t.Fatalf("reconnect %d resumed from Last-Event-ID=%q cursor=%q, want %s", i+1, got, h.cursors[i+1], want)
		}
	}
	if m := s.Metrics(); m.Cursor != "10" || m.Events != 10 || m.Reconnects < 3 {
		t.Fatalf("metrics = %+v", m)
	}
}

func TestResilientStream_SurvivesProxyRestart(t *testing.T) {
	h := &sseHorizon{total: 6, perConn: 100}
	srv := httptest.NewServer(h)
	defer srv.Close()
	backend := strings.TrimPrefix(srv.URL, "http://")

	// A TCP proxy between the client and Horizon that can be restarted on
	// the same address, dropping every live connection.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	p := newTCPProxy(ln, backend)

	h.mu.Lock()
	h.total = 3 // first serve only 3 records
	h.mu.Unlock()

	c := newCollector(6)
	s := NewResilientStream("proxy", stellar.NewHorizonStreamer("http://"+addr, 0), AccountPaymentsPath(streamAccount), nil, "", c.handle, c.commit, fastStreamConfig())
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx) }()

	waitFor(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return len(c.commits) == 3 })
	p.stop() // proxy goes down mid-stream
	h.mu.Lock()
	h.total = 6 // new payments land while the proxy is down
	h.mu.Unlock()
	time.Sleep(50 * time.Millisecond)
	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("restart proxy: %v", err)
	}
	p2 := newTCPProxy(ln2, backend)
	defer p2.stop()

	select {
	case <-c.done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out after proxy restart: %v", c.handled)
	}
	cancel()
	if err := <-errc; err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertSequential(t, c.handled, 6)
	if s.Metrics().Reconnects < 1 {
		t.Fatal("expected a reconnect")
	}
}

func TestResilientStream_CloseEventReconnectsImmediately(t *testing.T) {
	h := &sseHorizon{total: 4, perConn: 2, ending: "close"}
	srv := httptest.NewServer(h)
	defer srv.Close()
	c := newCollector(4)
	cfg := fastStreamConfig()
	cfg.BaseBackoff, cfg.MaxBackoff = time.Hour, time.Hour // any backoff would time the test out
	s := NewResilientStream("close", stellar.NewHorizonStreamer(srv.URL, 0), AccountPaymentsPath(streamAccount), nil, "", c.handle, c.commit, cfg)
	if err := runStream(t, s, c, 3*time.Second); err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertSequential(t, c.handled, 4)
}

func TestResilientStream_IdleTimeoutReconnects(t *testing.T) {
	h := &sseHorizon{total: 2, perConn: 1, ending: "stall"}
	srv := httptest.NewServer(h)
	defer srv.Close()
	c := newCollector(2)
	s := NewResilientStream("idle", stellar.NewHorizonStreamer(srv.URL, 100*time.Millisecond), AccountPaymentsPath(streamAccount), nil, "", c.handle, c.commit, fastStreamConfig())
	if err := runStream(t, s, c, 3*time.Second); err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertSequential(t, c.handled, 2)
	if h.requestCount() < 2 {
		t.Fatal("a silent connection must be torn down and re-opened")
	}
}

func TestResilientStream_HandlerFailureRedelivers(t *testing.T) {
	h := &sseHorizon{total: 3, perConn: 100}
	srv := httptest.NewServer(h)
	defer srv.Close()
	c := newCollector(3)
	c.failOnce["2"] = true
	s := NewResilientStream("redeliver", stellar.NewHorizonStreamer(srv.URL, 0), AccountPaymentsPath(streamAccount), nil, "", c.handle, c.commit, fastStreamConfig())
	if err := runStream(t, s, c, 3*time.Second); err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertSequential(t, c.handled, 3)
	h.mu.Lock()
	defer h.mu.Unlock()
	if got := h.requests[1].Get("Last-Event-ID"); got != "1" {
		t.Fatalf("after a handler failure the stream must resume from the last committed cursor 1, got %q", got)
	}
}

func TestResilientStream_HonorsRetryAfterAndStopsOn404(t *testing.T) {
	h := &sseHorizon{status: http.StatusTooManyRequests}
	srv := httptest.NewServer(h)
	defer srv.Close()
	s := NewResilientStream("ratelimited", stellar.NewHorizonStreamer(srv.URL, 0), AccountPaymentsPath(streamAccount), nil, "", nil, nil, fastStreamConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	_ = s.Run(ctx)
	if n := h.requestCount(); n != 2 {
		t.Fatalf("requests = %d within 1.5s, want 2 (Retry-After: 1 must override the 20ms backoff)", n)
	}

	h.mu.Lock()
	h.status = http.StatusNotFound
	h.mu.Unlock()
	err := NewResilientStream("missing", stellar.NewHorizonStreamer(srv.URL, 0), AccountPaymentsPath(streamAccount), nil, "", nil, nil, fastStreamConfig()).Run(context.Background())
	if !stellar.IsNotFound(err) {
		t.Fatalf("404 err = %v, want a not-found error that stops the stream", err)
	}
}

func TestResilientStream_FullJitterBounds(t *testing.T) {
	var max int64
	cfg := StreamConfig{BaseBackoff: time.Second, MaxBackoff: 30 * time.Second, randN: func(n int64) int64 { max = n - 1; return n - 1 }}
	s := NewResilientStream("jitter", nil, "", nil, "", nil, nil, cfg)
	for attempt, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second} {
		if got := s.backoff(attempt); got != want || time.Duration(max) != want {
			t.Fatalf("attempt %d: ceiling %s, want %s", attempt, got, want)
		}
	}
	if got := s.backoff(200); got != 30*time.Second {
		t.Fatalf("huge attempt must not overflow: %s", got)
	}

	s.cfg.randN = func(n int64) int64 { return 0 }
	if s.backoff(5) != 0 {
		t.Fatal("full jitter's lower bound is 0")
	}
}

func TestResilientStream_EmitsHeartbeat(t *testing.T) {
	var buf safeBuffer
	prev := log.Logger
	log.Logger = zerolog.New(&buf)
	defer func() { log.Logger = prev }()

	h := &sseHorizon{total: 1, perConn: 1}
	srv := httptest.NewServer(h)
	defer srv.Close()
	cfg := fastStreamConfig()
	cfg.HeartbeatInterval = 20 * time.Millisecond
	c := newCollector(1)
	s := NewResilientStream("beat", stellar.NewHorizonStreamer(srv.URL, 0), AccountPaymentsPath(streamAccount), nil, "", c.handle, c.commit, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_ = s.Run(ctx)

	out := buf.String()
	if n := strings.Count(out, "horizon stream heartbeat"); n < 3 {
		t.Fatalf("heartbeats = %d in 150ms at 20ms interval, want >= 3:\n%s", n, out)
	}
	if !strings.Contains(out, `"connected":true`) || !strings.Contains(out, `"cursor":"1"`) || !strings.Contains(out, `"idle_seconds"`) {
		t.Fatalf("heartbeat fields missing:\n%s", out)
	}
}

func TestDecodePayment(t *testing.T) {
	op, err := DecodePayment(stellar.HorizonEvent{ID: "7", Data: []byte(paymentJSON(7))})
	if err != nil {
		t.Fatal(err)
	}
	p, ok := op.(operations.Payment)
	if !ok || p.To != streamAccount || p.Amount != "1.0000000" || op.PagingToken() != "7" {
		t.Fatalf("op = %#v", op)
	}
	if _, err := DecodePayment(stellar.HorizonEvent{Data: []byte("{")}); err == nil {
		t.Fatal("expected decode error")
	}
}

type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// tcpProxy forwards connections to backend until stopped.
type tcpProxy struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
	wg    sync.WaitGroup
}

func newTCPProxy(ln net.Listener, backend string) *tcpProxy {
	p := &tcpProxy{ln: ln}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			upstream, err := net.Dial("tcp", backend)
			if err != nil {
				client.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, client, upstream)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(upstream, client); upstream.Close() }()
			go func() { _, _ = io.Copy(client, upstream); client.Close() }()
		}
	}()
	return p
}

func (p *tcpProxy) stop() {
	p.ln.Close()
	p.mu.Lock()
	for _, c := range p.conns {
		c.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
