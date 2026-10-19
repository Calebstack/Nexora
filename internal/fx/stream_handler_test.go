package fx

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/shopspring/decimal"
)

func TestQuoteStream_GoAndEventSourceClients(t *testing.T) {
	var calls atomic.Int32
	svc := &mockFXService{getRatesFunc: func(context.Context, string, string) (*RateResponse, error) {
		calls.Add(1)
		return &RateResponse{Rate: decimal.RequireFromString("1.2345")}, nil
	}}
	h := NewHandler(svc)
	defer h.stream.Close()
	router := chi.NewRouter()
	h.Routes()(router)
	server := httptest.NewServer(router)
	defer server.Close()

	readEvent := func(client *http.Client, accept string) string {
		req, err := http.NewRequest(http.MethodGet, server.URL+"/quotes/stream?pairs=USDC-XLM&interval=100ms&heartbeat=100ms", nil)
		if err != nil { t.Fatal(err) }
		req.Header.Set("Accept", accept)
		resp, err := client.Do(req)
		if err != nil { t.Fatal(err) }
		defer resp.Body.Close()
		if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
			t.Fatalf("content type = %q", got)
		}
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				return strings.TrimPrefix(line, "data: ")
			}
		}
		if err := scanner.Err(); err != nil && err != io.EOF { t.Fatal(err) }
		return ""
	}

	// Standard Go HTTP client.
	goPayload := readEvent(server.Client(), "text/event-stream")
	if !strings.Contains(goPayload, `"pair":"USDC:XLM"`) { t.Fatalf("Go client payload = %s", goPayload) }
	// EventSource uses the same SSE wire format; this client sets its browser
	// accept header and verifies the event payload that EventSource consumes.
	browserPayload := readEvent(server.Client(), "text/event-stream, */*;q=0.9")
	if !strings.Contains(browserPayload, `"rate"`) { t.Fatalf("EventSource payload = %s", browserPayload) }
	if calls.Load() < 1 { t.Fatal("stream did not fetch a live rate") }
}

func TestQuoteStream_UnsubscribeRemovesConnectionState(t *testing.T) {
	stream := NewQuoteStream(&mockFXService{})
	defer stream.Close()
	_, unsubscribe := stream.Subscribe([]string{"USDC:XLM"}, minStreamInterval)
	unsubscribe()
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.subscribers) != 0 || len(stream.pairs) != 0 {
		t.Fatalf("subscriber state leaked: subscribers=%d pairs=%d", len(stream.subscribers), len(stream.pairs))
	}
}

func TestQuoteStream_NilServiceIsUnavailableWithoutPanicking(t *testing.T) {
	h := NewHandler(nil)
	defer h.stream.Close()
	w := httptest.NewRecorder()
	h.streamQuotes(w, httptest.NewRequest(http.MethodGet, "/quotes/stream?pairs=USDC-XLM", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}

func TestQuoteStream_MultiplexesTenThousandSubscribersByPair(t *testing.T) {
	var fetches atomic.Int32
	stream := &QuoteStream{
		fetch: func(context.Context, string, string) (*RateResponse, error) {
			fetches.Add(1)
			return &RateResponse{Rate: decimal.One}, nil
		},
		pairs:       make(map[string]*streamPair),
		subscribers: make(map[uint64]*streamSubscriber),
	}

	unsubscribes := make([]func(), 0, 10_000)
	for range 10_000 {
		_, unsubscribe := stream.Subscribe([]string{"USDC:XLM"}, time.Second)
		unsubscribes = append(unsubscribes, unsubscribe)
	}
	stream.pollDue(time.Now())
	if fetches.Load() != 1 {
		t.Fatalf("provider fetches = %d, want one shared pair poll", fetches.Load())
	}
	stream.mu.Lock()
	connections, pairs := len(stream.subscribers), len(stream.pairs)
	stream.mu.Unlock()
	if connections != 10_000 || pairs != 1 {
		t.Fatalf("connections=%d pairs=%d, want 10000 and 1", connections, pairs)
	}
	for _, unsubscribe := range unsubscribes {
		unsubscribe()
	}
}
