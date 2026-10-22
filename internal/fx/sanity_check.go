package fx

import (
	"fmt"
	"sync"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/shopspring/decimal"
)

// ViolationType classifies why a quote failed the sanity check. It travels into
// the emitted alert so an on-call engineer can triage without re-deriving the
// cause from the raw payload.
type ViolationType string

const (
	// ViolationNegativeSpread means ask <= bid: the spread is zero or negative.
	ViolationNegativeSpread ViolationType = "negative_spread"
	// ViolationInvertedRate means bid > ask, or bid/ask is non-positive: the
	// feed almost certainly transposed the pair or returned garbage.
	ViolationInvertedRate ViolationType = "inverted_rate"
	// ViolationOutOfCorridor means the mid-market rate fell outside the pair's
	// plausible historical bounds.
	ViolationOutOfCorridor ViolationType = "out_of_corridor"
)

// Corridor bounds the plausible mid-market rate for a pair. Both bounds are
// inclusive. A zero-value Max (Max.IsZero()) means "no upper bound"; a zero Min
// means "no lower bound", so a partially specified corridor is still useful.
type Corridor struct {
	Min decimal.Decimal
	Max decimal.Decimal
}

// QuoteSnapshot is the outgoing quote the sanity checker inspects. Bid and Ask
// are the prices as quoted to the caller (destination units per source unit);
// Mid is the mid-market reference the spread was applied to.
type QuoteSnapshot struct {
	FromAsset string
	ToAsset   string
	Bid       decimal.Decimal
	Ask       decimal.Decimal
	Mid       decimal.Decimal
	SpreadBps int
	Provider  string
	Timestamp time.Time
}

// SanityAlert is the detailed, machine-readable record of a rejected quote. It
// carries the full offending payload so the rejection can be reproduced from
// logs alone.
type SanityAlert struct {
	Pair      string        `json:"pair"`
	Violation ViolationType `json:"violation"`
	Bid       string        `json:"bid"`
	Ask       string        `json:"ask"`
	Mid       string        `json:"mid"`
	SpreadBps int           `json:"spread_bps"`
	Provider  string        `json:"provider"`
	Corridor  *Corridor     `json:"corridor,omitempty"`
	Message   string        `json:"message"`
	Timestamp time.Time     `json:"timestamp"`
}

// SanityChecker is a defensive guard over outgoing FX quotes. It asserts a
// strictly positive bid-ask spread, rejects inverted or non-positive rates, and
// confirms the mid-market rate sits inside a plausible historical corridor. The
// first violation on a pair suspends automated quoting for that pair until an
// operator calls Resume, so a stuck or corrupt feed cannot keep issuing bad
// prices.
//
// It is safe for concurrent use. Its design mirrors TriangulationDetector: a
// self-contained validator the FX service consults before a quote is returned.
type SanityChecker struct {
	mu           sync.RWMutex
	corridors    map[string]Corridor
	suspended    map[string]time.Time // pair key -> time suspended
	alerts       []SanityAlert
	maxAlerts    int
	alertHandler func(SanityAlert)
	now          func() time.Time
}

// NewSanityChecker builds a checker. corridors maps "FROM-TO" to the plausible
// mid-rate range for that pair (nil for none). alertHandler, if non-nil, is
// invoked synchronously for every rejected quote; keep it fast or dispatch
// asynchronously inside it.
func NewSanityChecker(corridors map[string]Corridor, alertHandler func(SanityAlert)) *SanityChecker {
	cp := make(map[string]Corridor, len(corridors))
	for k, v := range corridors {
		cp[k] = v
	}
	return &SanityChecker{
		corridors:    cp,
		suspended:    make(map[string]time.Time),
		maxAlerts:    256,
		alertHandler: alertHandler,
		now:          func() time.Time { return time.Now().UTC() },
	}
}

// DefaultHistoricalCorridors returns generous plausible mid-rate bounds for the
// pairs Nexora quotes. The bounds are intentionally wide: their job is to catch
// decimal-shift and transposition feed errors (orders of magnitude off), not to
// second-guess normal market moves.
func DefaultHistoricalCorridors() map[string]Corridor {
	d := func(s string) decimal.Decimal { return decimal.RequireFromString(s) }
	return map[string]Corridor{
		// XLM has historically traded roughly $0.01–$1; allow headroom.
		"XLM-USDC": {Min: d("0.005"), Max: d("5")},
		"USDC-XLM": {Min: d("0.2"), Max: d("200")},
		"XLM-EURC": {Min: d("0.005"), Max: d("5")},
		"EURC-XLM": {Min: d("0.2"), Max: d("200")},
	}
}

// SetCorridor sets or replaces the plausible mid-rate corridor for a pair.
func (c *SanityChecker) SetCorridor(from, to string, corridor Corridor) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.corridors[pairKey(from, to)] = corridor
}

// Check validates an outgoing quote. It returns nil when the quote is sound.
// On the first violation for a pair it records a detailed alert, invokes the
// alert handler, suspends automated quoting for the pair, and returns a typed
// domain error. A subsequent Check for an already-suspended pair short-circuits
// with ErrQuotingSuspended without re-alerting.
func (c *SanityChecker) Check(q QuoteSnapshot) error {
	key := pairKey(q.FromAsset, q.ToAsset)

	c.mu.RLock()
	_, suspended := c.suspended[key]
	corridor, hasCorridor := c.corridors[key]
	c.mu.RUnlock()

	if suspended {
		return fmt.Errorf("%w: %s", domain.ErrQuotingSuspended, key)
	}

	violation, sentinel, ok := classify(q, corridor, hasCorridor)
	if ok {
		return nil
	}

	alert := c.buildAlert(key, violation, q, corridor, hasCorridor)
	c.suspend(key, alert)
	return fmt.Errorf("%w: %s", sentinel, alert.Message)
}

// classify inspects a quote and returns the first violation found, together
// with the matching sentinel error. ok is true when the quote is sound.
func classify(q QuoteSnapshot, corridor Corridor, hasCorridor bool) (ViolationType, error, bool) {
	// Inverted or non-positive prices come first: a transposed feed is the most
	// dangerous failure and the spread test below is meaningless once a price
	// is non-positive.
	if q.Bid.Sign() <= 0 || q.Ask.Sign() <= 0 {
		return ViolationInvertedRate, domain.ErrInvertedRate, false
	}
	if q.Bid.GreaterThan(q.Ask) {
		return ViolationInvertedRate, domain.ErrInvertedRate, false
	}
	// Ask must be strictly greater than bid.
	if !q.Ask.GreaterThan(q.Bid) {
		return ViolationNegativeSpread, domain.ErrNegativeSpread, false
	}
	if hasCorridor {
		if corridor.Min.Sign() > 0 && q.Mid.LessThan(corridor.Min) {
			return ViolationOutOfCorridor, domain.ErrRateOutOfCorridor, false
		}
		if corridor.Max.Sign() > 0 && q.Mid.GreaterThan(corridor.Max) {
			return ViolationOutOfCorridor, domain.ErrRateOutOfCorridor, false
		}
	}
	return "", nil, true
}

func (c *SanityChecker) buildAlert(key string, v ViolationType, q QuoteSnapshot, corridor Corridor, hasCorridor bool) SanityAlert {
	ts := q.Timestamp
	if ts.IsZero() {
		ts = c.now()
	}
	alert := SanityAlert{
		Pair:      key,
		Violation: v,
		Bid:       q.Bid.String(),
		Ask:       q.Ask.String(),
		Mid:       q.Mid.String(),
		SpreadBps: q.SpreadBps,
		Provider:  q.Provider,
		Timestamp: ts,
	}
	if hasCorridor {
		cc := corridor
		alert.Corridor = &cc
	}
	switch v {
	case ViolationNegativeSpread:
		alert.Message = fmt.Sprintf("negative/zero bid-ask spread on %s: bid=%s ask=%s (provider %s)", key, q.Bid, q.Ask, q.Provider)
	case ViolationInvertedRate:
		alert.Message = fmt.Sprintf("inverted or non-positive rate on %s: bid=%s ask=%s (provider %s)", key, q.Bid, q.Ask, q.Provider)
	case ViolationOutOfCorridor:
		alert.Message = fmt.Sprintf("mid rate %s on %s outside plausible corridor [%s, %s] (provider %s)", q.Mid, key, corridor.Min, corridor.Max, q.Provider)
	}
	return alert
}

// suspend records the alert and marks the pair suspended. It is idempotent:
// concurrent violators race on the lock and only the first records state, so
// exactly one suspension and one alert are retained per crossing.
func (c *SanityChecker) suspend(key string, alert SanityAlert) {
	c.mu.Lock()
	if _, already := c.suspended[key]; already {
		c.mu.Unlock()
		return
	}
	c.suspended[key] = alert.Timestamp
	c.alerts = append(c.alerts, alert)
	if len(c.alerts) > c.maxAlerts {
		c.alerts = c.alerts[len(c.alerts)-c.maxAlerts:]
	}
	handler := c.alertHandler
	c.mu.Unlock()

	if handler != nil {
		handler(alert)
	}
}

// IsSuspended reports whether automated quoting for the pair is currently
// suspended.
func (c *SanityChecker) IsSuspended(from, to string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.suspended[pairKey(from, to)]
	return ok
}

// Resume clears a suspension so the pair can be quoted again. It reports
// whether the pair was in fact suspended.
func (c *SanityChecker) Resume(from, to string) bool {
	key := pairKey(from, to)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.suspended[key]; !ok {
		return false
	}
	delete(c.suspended, key)
	return true
}

// SuspendedPairs returns the pair keys currently suspended.
func (c *SanityChecker) SuspendedPairs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0, len(c.suspended))
	for k := range c.suspended {
		out = append(out, k)
	}
	return out
}

// GetRecentAlerts returns a copy of the retained rejection alerts, oldest first.
func (c *SanityChecker) GetRecentAlerts() []SanityAlert {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]SanityAlert, len(c.alerts))
	copy(out, c.alerts)
	return out
}

// SnapshotFromMid derives a symmetric bid/ask QuoteSnapshot from a mid-market
// rate and a half-spread expressed in basis points. It is a convenience for
// callers (such as the FX service) that price a single rate off a mid and a
// spread rather than tracking a two-sided book: bid = mid·(1 - bps/10000),
// ask = mid·(1 + bps/10000).
func SnapshotFromMid(from, to string, mid decimal.Decimal, spreadBps int, provider string) QuoteSnapshot {
	factor := decimal.NewFromInt(int64(spreadBps)).Div(decimal.NewFromInt(10000))
	one := decimal.NewFromInt(1)
	return QuoteSnapshot{
		FromAsset: from,
		ToAsset:   to,
		Bid:       mid.Mul(one.Sub(factor)),
		Ask:       mid.Mul(one.Add(factor)),
		Mid:       mid,
		SpreadBps: spreadBps,
		Provider:  provider,
		Timestamp: time.Now().UTC(),
	}
}
