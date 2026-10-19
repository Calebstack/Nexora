package alerting

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Severity follows the PagerDuty Events API v2 payload.severity values.
type Severity string

const (
	SeverityCritical Severity = "critical"
	SeverityError    Severity = "error"
	SeverityWarning  Severity = "warning"
	SeverityInfo     Severity = "info"
)

// PagerDutyEventsURL is the Events API v2 enqueue endpoint.
const PagerDutyEventsURL = "https://events.pagerduty.com/v2/enqueue"

// pagerDutySummaryLimit is the maximum payload.summary length PagerDuty accepts.
const pagerDutySummaryLimit = 1024

// Incident is an operational condition paged out to on-call channels.
// DedupKey identifies the condition so repeated triggers collapse into one
// incident and a later resolve closes it.
type Incident struct {
	DedupKey  string
	Summary   string
	Source    string
	Severity  Severity
	Details   map[string]any
	Timestamp time.Time
}

// Notifier delivers incidents to one channel.
type Notifier interface {
	Trigger(ctx context.Context, inc Incident) error
	Resolve(ctx context.Context, inc Incident) error
}

// SlackNotifier posts to a Slack incoming webhook.
type SlackNotifier struct {
	webhookURL string
	httpClient *http.Client
}

func NewSlackNotifier(webhookURL string, httpClient *http.Client) *SlackNotifier {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &SlackNotifier{webhookURL: webhookURL, httpClient: httpClient}
}

func (n *SlackNotifier) Trigger(ctx context.Context, inc Incident) error {
	return n.post(ctx, fmt.Sprintf(":rotating_light: *[%s] %s*\n%s", strings.ToUpper(string(inc.Severity)), inc.Summary, formatDetails(inc.Details)))
}

func (n *SlackNotifier) Resolve(ctx context.Context, inc Incident) error {
	return n.post(ctx, fmt.Sprintf(":white_check_mark: *[RESOLVED] %s*\n%s", inc.Summary, formatDetails(inc.Details)))
}

func (n *SlackNotifier) post(ctx context.Context, text string) error {
	return postJSON(ctx, n.httpClient, n.webhookURL, map[string]string{"text": text})
}

// PagerDutyNotifier sends Events API v2 trigger/resolve events.
type PagerDutyNotifier struct {
	routingKey string
	endpoint   string
	httpClient *http.Client
}

// NewPagerDutyNotifier builds a notifier for an Events API v2 integration
// key. An empty endpoint defaults to PagerDutyEventsURL.
func NewPagerDutyNotifier(routingKey, endpoint string, httpClient *http.Client) *PagerDutyNotifier {
	if endpoint == "" {
		endpoint = PagerDutyEventsURL
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &PagerDutyNotifier{routingKey: routingKey, endpoint: endpoint, httpClient: httpClient}
}

type pagerDutyPayload struct {
	Summary       string         `json:"summary"`
	Source        string         `json:"source"`
	Severity      Severity       `json:"severity"`
	Timestamp     string         `json:"timestamp,omitempty"`
	CustomDetails map[string]any `json:"custom_details,omitempty"`
}

type pagerDutyEvent struct {
	RoutingKey  string            `json:"routing_key"`
	EventAction string            `json:"event_action"`
	DedupKey    string            `json:"dedup_key,omitempty"`
	Payload     *pagerDutyPayload `json:"payload,omitempty"`
}

func (n *PagerDutyNotifier) Trigger(ctx context.Context, inc Incident) error {
	summary := inc.Summary
	if len(summary) > pagerDutySummaryLimit {
		summary = strings.ToValidUTF8(summary[:pagerDutySummaryLimit], "")
	}
	return postJSON(ctx, n.httpClient, n.endpoint, pagerDutyEvent{
		RoutingKey:  n.routingKey,
		EventAction: "trigger",
		DedupKey:    inc.DedupKey,
		Payload: &pagerDutyPayload{
			Summary:       summary,
			Source:        inc.Source,
			Severity:      inc.Severity,
			Timestamp:     inc.Timestamp.UTC().Format(time.RFC3339Nano),
			CustomDetails: inc.Details,
		},
	})
}

func (n *PagerDutyNotifier) Resolve(ctx context.Context, inc Incident) error {
	return postJSON(ctx, n.httpClient, n.endpoint, pagerDutyEvent{
		RoutingKey:  n.routingKey,
		EventAction: "resolve",
		DedupKey:    inc.DedupKey,
	})
}

// Monitor fans incidents out to every notifier and tracks which dedup keys
// are open, so a condition that persists across evaluations pages once and
// a recovery resolves it once. Safe for concurrent use.
type Monitor struct {
	notifiers []Notifier
	mu        sync.Mutex
	open      map[string]struct{}
}

func NewMonitor(notifiers ...Notifier) *Monitor {
	return &Monitor{notifiers: notifiers, open: make(map[string]struct{})}
}

// Raise triggers inc on every notifier unless its dedup key is already
// open. A key stays closed when every notifier failed, so the next
// evaluation retries the page. O(n) in notifiers.
func (m *Monitor) Raise(ctx context.Context, inc Incident) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.open[inc.DedupKey]; ok {
		return nil
	}
	if inc.Timestamp.IsZero() {
		inc.Timestamp = time.Now()
	}
	delivered, err := m.fanOut(func(n Notifier) error { return n.Trigger(ctx, inc) })
	if delivered {
		m.open[inc.DedupKey] = struct{}{}
	}
	return err
}

// Clear resolves inc on every notifier when its dedup key is open.
func (m *Monitor) Clear(ctx context.Context, inc Incident) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.open[inc.DedupKey]; !ok {
		return nil
	}
	delivered, err := m.fanOut(func(n Notifier) error { return n.Resolve(ctx, inc) })
	if delivered {
		delete(m.open, inc.DedupKey)
	}
	return err
}

// fanOut reports whether at least one notifier accepted the call.
func (m *Monitor) fanOut(call func(Notifier) error) (bool, error) {
	var errs []error
	for _, n := range m.notifiers {
		if err := call(n); err != nil {
			errs = append(errs, err)
		}
	}
	return len(errs) < len(m.notifiers), errors.Join(errs...)
}

func postJSON(ctx context.Context, client *http.Client, url string, body any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("alerting: marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("alerting: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("alerting: send: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("alerting: %s returned HTTP %d", req.URL.Host, resp.StatusCode)
	}
	return nil
}

func formatDetails(details map[string]any) string {
	keys := make([]string, 0, len(details))
	for k := range details {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "• %s: %v\n", k, details[k])
	}
	return b.String()
}
