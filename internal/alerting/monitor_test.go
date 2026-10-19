package alerting

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordingServer struct {
	mu     sync.Mutex
	bodies []map[string]any
	status int
}

func newRecordingServer(t *testing.T, status int) (*recordingServer, *httptest.Server) {
	t.Helper()
	rs := &recordingServer{status: status}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("content-type = %q", r.Header.Get("Content-Type"))
		}
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Errorf("decode body: %v", err)
		}
		rs.mu.Lock()
		rs.bodies = append(rs.bodies, body)
		rs.mu.Unlock()
		w.WriteHeader(rs.status)
	}))
	t.Cleanup(srv.Close)
	return rs, srv
}

func testIncident() Incident {
	return Incident{
		DedupKey:  "anchor-reserve:USDC:GISSUER",
		Summary:   "USDC reserve ratio 1.007000 is outside 1.00 ± 0.5%",
		Source:    "nexora-test",
		Severity:  SeverityCritical,
		Details:   map[string]any{"reserve_ratio": "1.007000"},
		Timestamp: time.Date(2026, 10, 19, 10, 0, 0, 0, time.UTC),
	}
}

func TestPagerDutyNotifier_EventsV2Format(t *testing.T) {
	rec, srv := newRecordingServer(t, http.StatusAccepted)
	pd := NewPagerDutyNotifier("routing-key-32-characters-long-xx", srv.URL, srv.Client())

	if err := pd.Trigger(context.Background(), testIncident()); err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if err := pd.Resolve(context.Background(), testIncident()); err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	trigger := rec.bodies[0]
	if trigger["routing_key"] != "routing-key-32-characters-long-xx" || trigger["event_action"] != "trigger" || trigger["dedup_key"] != "anchor-reserve:USDC:GISSUER" {
		t.Fatalf("trigger envelope = %v", trigger)
	}
	payload := trigger["payload"].(map[string]any)
	if payload["severity"] != "critical" || payload["source"] != "nexora-test" || payload["timestamp"] != "2026-10-19T10:00:00Z" {
		t.Fatalf("trigger payload = %v", payload)
	}
	if payload["custom_details"].(map[string]any)["reserve_ratio"] != "1.007000" {
		t.Fatalf("custom_details = %v", payload["custom_details"])
	}

	resolve := rec.bodies[1]
	if resolve["event_action"] != "resolve" || resolve["dedup_key"] != "anchor-reserve:USDC:GISSUER" || resolve["payload"] != nil {
		t.Fatalf("resolve = %v", resolve)
	}
}

func TestPagerDutyNotifier_TruncatesSummary(t *testing.T) {
	rec, srv := newRecordingServer(t, http.StatusAccepted)
	inc := testIncident()
	inc.Summary = strings.Repeat("x", 2000)
	if err := NewPagerDutyNotifier("k", srv.URL, srv.Client()).Trigger(context.Background(), inc); err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if got := len(rec.bodies[0]["payload"].(map[string]any)["summary"].(string)); got != pagerDutySummaryLimit {
		t.Fatalf("summary length = %d, want %d", got, pagerDutySummaryLimit)
	}
}

func TestSlackNotifier_PostsText(t *testing.T) {
	rec, srv := newRecordingServer(t, http.StatusOK)
	if err := NewSlackNotifier(srv.URL, srv.Client()).Trigger(context.Background(), testIncident()); err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	text, _ := rec.bodies[0]["text"].(string)
	if !strings.Contains(text, "[CRITICAL]") || !strings.Contains(text, "reserve_ratio: 1.007000") {
		t.Fatalf("text = %q", text)
	}
}

func TestSlackNotifier_ErrorStatus(t *testing.T) {
	_, srv := newRecordingServer(t, http.StatusBadRequest)
	if err := NewSlackNotifier(srv.URL, srv.Client()).Trigger(context.Background(), testIncident()); err == nil {
		t.Fatal("expected error on HTTP 400")
	}
}

type countingNotifier struct {
	triggers, resolves int
	err                error
}

func (c *countingNotifier) Trigger(context.Context, Incident) error { c.triggers++; return c.err }
func (c *countingNotifier) Resolve(context.Context, Incident) error { c.resolves++; return c.err }

func TestMonitor_DedupesUntilResolved(t *testing.T) {
	a, b := &countingNotifier{}, &countingNotifier{}
	m := NewMonitor(a, b)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := m.Raise(ctx, testIncident()); err != nil {
			t.Fatalf("Raise: %v", err)
		}
	}
	if a.triggers != 1 || b.triggers != 1 {
		t.Fatalf("triggers = %d/%d, want 1/1", a.triggers, b.triggers)
	}
	_ = m.Clear(ctx, testIncident())
	_ = m.Clear(ctx, testIncident())
	if a.resolves != 1 {
		t.Fatalf("resolves = %d, want 1", a.resolves)
	}
	_ = m.Raise(ctx, testIncident())
	if a.triggers != 2 {
		t.Fatalf("re-raise after resolve: triggers = %d, want 2", a.triggers)
	}
}

func TestMonitor_RetriesWhenEveryNotifierFails(t *testing.T) {
	failing := &countingNotifier{err: errors.New("down")}
	m := NewMonitor(failing)
	ctx := context.Background()

	if err := m.Raise(ctx, testIncident()); err == nil {
		t.Fatal("expected error")
	}
	_ = m.Raise(ctx, testIncident())
	if failing.triggers != 2 {
		t.Fatalf("triggers = %d, want 2 (key must stay closed after total failure)", failing.triggers)
	}

	// One healthy channel is enough to mark the incident open.
	healthy := &countingNotifier{}
	m = NewMonitor(failing, healthy)
	if err := m.Raise(ctx, testIncident()); err == nil {
		t.Fatal("expected the failing channel's error to surface")
	}
	_ = m.Raise(ctx, testIncident())
	if healthy.triggers != 1 {
		t.Fatalf("healthy triggers = %d, want 1", healthy.triggers)
	}
}
