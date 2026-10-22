package fx

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
)

const (
	AlertSyncChannel       = "fx:alerts:sync"
	AlertFiringRedisPrefix = "fx:alert:fired:"
	AlertFiringLockTTL     = 24 * time.Hour
)

// WebhookDispatcher defines the interface for dispatching rate alert webhook events.
type WebhookDispatcher interface {
	Dispatch(ctx context.Context, eventType string, payload interface{}) error
}

// AlertSyncMessage defines cross-node pub/sub payloads to synchronize in-memory alert indexes.
type AlertSyncMessage struct {
	Action  string            `json:"action"` // "create", "update", "delete"
	AlertID string            `json:"alert_id"`
	Pair    string            `json:"pair"`
	Alert   *domain.RateAlert `json:"alert,omitempty"`
}

// RateAlertWebhookPayload is the event payload dispatched on threshold crossing.
type RateAlertWebhookPayload struct {
	AlertID      string          `json:"alert_id"`
	TenantID     string          `json:"tenant_id"`
	FromAsset    string          `json:"from_asset"`
	ToAsset      string          `json:"to_asset"`
	TargetRate   decimal.Decimal `json:"target_rate"`
	CurrentRate  decimal.Decimal `json:"current_rate"`
	Direction    string          `json:"direction"`
	LastEvalRate *string         `json:"last_eval_rate,omitempty"`
	TriggeredAt  time.Time       `json:"triggered_at"`
}

// intervalNode represents a node in an augmented AVL interval tree.
// Time complexity: O(log N) insertion, deletion, and search.
type intervalNode struct {
	low      decimal.Decimal
	high     decimal.Decimal
	maxHigh  decimal.Decimal
	alert    *domain.RateAlert
	left     *intervalNode
	right    *intervalNode
	height   int
}

func nodeHeight(n *intervalNode) int {
	if n == nil {
		return 0
	}
	return n.height
}

func maxDecimal(a, b decimal.Decimal) decimal.Decimal {
	if a.GreaterThan(b) {
		return a
	}
	return b
}

func updateNode(n *intervalNode) {
	if n == nil {
		return
	}
	lh := nodeHeight(n.left)
	rh := nodeHeight(n.right)
	if lh > rh {
		n.height = lh + 1
	} else {
		n.height = rh + 1
	}

	n.maxHigh = n.high
	if n.left != nil && n.left.maxHigh.GreaterThan(n.maxHigh) {
		n.maxHigh = n.left.maxHigh
	}
	if n.right != nil && n.right.maxHigh.GreaterThan(n.maxHigh) {
		n.maxHigh = n.right.maxHigh
	}
}

func balanceFactor(n *intervalNode) int {
	if n == nil {
		return 0
	}
	return nodeHeight(n.left) - nodeHeight(n.right)
}

func rotateRight(y *intervalNode) *intervalNode {
	x := y.left
	t2 := x.right
	x.right = y
	y.left = t2
	updateNode(y)
	updateNode(x)
	return x
}

func rotateLeft(x *intervalNode) *intervalNode {
	y := x.right
	t2 := y.left
	y.left = x
	x.right = t2
	updateNode(x)
	updateNode(y)
	return y
}

func rebalance(n *intervalNode) *intervalNode {
	updateNode(n)
	bf := balanceFactor(n)
	if bf > 1 {
		if balanceFactor(n.left) < 0 {
			n.left = rotateLeft(n.left)
		}
		return rotateRight(n)
	}
	if bf < -1 {
		if balanceFactor(n.right) > 0 {
			n.right = rotateRight(n.right)
		}
		return rotateLeft(n)
	}
	return n
}

// IntervalTree stores active rate alert thresholds indexed as intervals.
type IntervalTree struct {
	root *intervalNode
	size int
}

func NewIntervalTree() *IntervalTree {
	return &IntervalTree{}
}

func (t *IntervalTree) Size() int {
	return t.size
}

// Insert adds an alert into the interval tree.
// Time complexity: O(log N).
func (t *IntervalTree) Insert(alert *domain.RateAlert) {
	t.root = t.insert(t.root, alert)
	t.size++
}

func (t *IntervalTree) insert(n *intervalNode, alert *domain.RateAlert) *intervalNode {
	low := alert.TargetRate
	high := alert.TargetRate

	if n == nil {
		return &intervalNode{
			low:     low,
			high:    high,
			maxHigh: high,
			alert:   alert,
			height:  1,
		}
	}

	if low.LessThan(n.low) || (low.Equal(n.low) && alert.ID < n.alert.ID) {
		n.left = t.insert(n.left, alert)
	} else {
		n.right = t.insert(n.right, alert)
	}

	return rebalance(n)
}

// Delete removes an alert by its ID and target rate.
// Time complexity: O(log N).
func (t *IntervalTree) Delete(alertID string, targetRate decimal.Decimal) bool {
	var deleted bool
	t.root, deleted = t.delete(t.root, alertID, targetRate)
	if deleted {
		t.size--
	}
	return deleted
}

func (t *IntervalTree) delete(n *intervalNode, alertID string, targetRate decimal.Decimal) (*intervalNode, bool) {
	if n == nil {
		return nil, false
	}

	var deleted bool
	if targetRate.LessThan(n.low) || (targetRate.Equal(n.low) && alertID < n.alert.ID) {
		n.left, deleted = t.delete(n.left, alertID, targetRate)
	} else if targetRate.GreaterThan(n.low) || (targetRate.Equal(n.low) && alertID > n.alert.ID) {
		n.right, deleted = t.delete(n.right, alertID, targetRate)
	} else {
		deleted = true
		if n.left == nil {
			return n.right, true
		} else if n.right == nil {
			return n.left, true
		}
		// Replace with in-order successor.
		succ := n.right
		for succ.left != nil {
			succ = succ.left
		}
		n.low = succ.low
		n.high = succ.high
		n.alert = succ.alert
		n.right, _ = t.delete(n.right, succ.alert.ID, succ.low)
	}

	if !deleted {
		return n, false
	}

	return rebalance(n), true
}

// QueryRange finds all active rate alerts whose target rates intersect [qLow, qHigh].
// Time complexity: O(log N + K) where K is number of matches.
func (t *IntervalTree) QueryRange(qLow, qHigh decimal.Decimal) []*domain.RateAlert {
	var results []*domain.RateAlert
	t.query(t.root, qLow, qHigh, &results)
	return results
}

func (t *IntervalTree) query(n *intervalNode, qLow, qHigh decimal.Decimal, results *[]*domain.RateAlert) {
	if n == nil {
		return
	}

	if n.left != nil && n.left.maxHigh.GreaterThanOrEqual(qLow) {
		t.query(n.left, qLow, qHigh, results)
	}

	if n.low.GreaterThanOrEqual(qLow) && n.high.LessThanOrEqual(qHigh) {
		*results = append(*results, n.alert)
	}

	if n.right != nil && n.low.LessThanOrEqual(qHigh) {
		t.query(n.right, qLow, qHigh, results)
	}
}

// atomicCASScript performs atomic check-and-set in Redis to enforce exactly-once firing.
var atomicCASScript = redis.NewScript(`
local val = redis.call('GET', KEYS[1])
if val and val == ARGV[1] then
    return 0
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return 1
`)

// AlertEvaluator maintains in-memory interval tree indices of active alerts per currency pair,
// evaluates incoming price ticks in sub-millisecond time, coordinates atomic CAS firing,
// and synchronizes cross-node state via Redis pub/sub.
type AlertEvaluator struct {
	mu         sync.RWMutex
	casMu      sync.Mutex
	trees      map[string]*IntervalTree
	alertsByID map[string]*domain.RateAlert
	lastPrices map[string]decimal.Decimal
	repo       AlertRepository
	redis      redis.UniversalClient
	dispatcher WebhookDispatcher
	subCancel  context.CancelFunc
}

func alertPairKey(from, to string) string {
	return from + ":" + to
}

// NewAlertEvaluator constructs a rate alert evaluation engine.
func NewAlertEvaluator(repo AlertRepository, redisClient redis.UniversalClient, dispatcher WebhookDispatcher) *AlertEvaluator {
	return &AlertEvaluator{
		trees:      make(map[string]*IntervalTree),
		alertsByID: make(map[string]*domain.RateAlert),
		lastPrices: make(map[string]decimal.Decimal),
		repo:       repo,
		redis:      redisClient,
		dispatcher: dispatcher,
	}
}

// LoadActiveAlerts populates in-memory interval trees from database storage at startup.
func (e *AlertEvaluator) LoadActiveAlerts(ctx context.Context) error {
	if e.repo == nil {
		return nil
	}
	alerts, err := e.repo.GetAllActiveAlerts(ctx)
	if err != nil {
		return fmt.Errorf("load active rate alerts: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	for _, alert := range alerts {
		e.indexAlertLocked(alert)
	}

	log.Info().Int("count", len(alerts)).Msg("rate alert in-memory interval trees initialized")
	return nil
}

func (e *AlertEvaluator) indexAlertLocked(alert *domain.RateAlert) {
	key := alertPairKey(alert.FromAsset, alert.ToAsset)
	tree := e.trees[key]
	if tree == nil {
		tree = NewIntervalTree()
		e.trees[key] = tree
	}
	tree.Insert(alert)
	e.alertsByID[alert.ID] = alert
}

// AddOrUpdateAlert updates in-memory index and optionally broadcasts to horizontal nodes.
func (e *AlertEvaluator) AddOrUpdateAlert(ctx context.Context, alert *domain.RateAlert, broadcast bool) error {
	e.mu.Lock()
	if existing, found := e.alertsByID[alert.ID]; found {
		oldKey := alertPairKey(existing.FromAsset, existing.ToAsset)
		if tree := e.trees[oldKey]; tree != nil {
			tree.Delete(existing.ID, existing.TargetRate)
		}
	}
	e.indexAlertLocked(alert)
	e.mu.Unlock()

	if broadcast && e.redis != nil {
		return e.publishSyncEvent(ctx, AlertSyncMessage{
			Action:  "update",
			AlertID: alert.ID,
			Pair:    alertPairKey(alert.FromAsset, alert.ToAsset),
			Alert:   alert,
		})
	}
	return nil
}

// RemoveAlert removes an alert from in-memory index and broadcasts deletion.
func (e *AlertEvaluator) RemoveAlert(ctx context.Context, alertID, pair string, broadcast bool) error {
	e.mu.Lock()
	if alert, found := e.alertsByID[alertID]; found {
		key := alertPairKey(alert.FromAsset, alert.ToAsset)
		if tree := e.trees[key]; tree != nil {
			tree.Delete(alertID, alert.TargetRate)
		}
		delete(e.alertsByID, alertID)
	}
	e.mu.Unlock()

	if broadcast && e.redis != nil {
		return e.publishSyncEvent(ctx, AlertSyncMessage{
			Action:  "delete",
			AlertID: alertID,
			Pair:    pair,
		})
	}
	return nil
}

func (e *AlertEvaluator) publishSyncEvent(ctx context.Context, msg AlertSyncMessage) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return e.redis.Publish(ctx, AlertSyncChannel, payload).Err()
}

// StartSyncSubscriber listens to horizontal synchronization events over Redis pub/sub.
func (e *AlertEvaluator) StartSyncSubscriber(ctx context.Context) {
	if e.redis == nil {
		return
	}
	subCtx, cancel := context.WithCancel(ctx)
	e.subCancel = cancel

	pubsub := e.redis.Subscribe(subCtx, AlertSyncChannel)
	go func() {
		defer pubsub.Close()
		ch := pubsub.Channel()
		for {
			select {
			case <-subCtx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var syncMsg AlertSyncMessage
				if err := json.Unmarshal([]byte(msg.Payload), &syncMsg); err != nil {
					continue
				}
				switch syncMsg.Action {
				case "create", "update":
					if syncMsg.Alert != nil {
						_ = e.AddOrUpdateAlert(subCtx, syncMsg.Alert, false)
					}
				case "delete":
					_ = e.RemoveAlert(subCtx, syncMsg.AlertID, syncMsg.Pair, false)
				}
			}
		}
	}()
}

// Stop closes pub/sub subscriber resources.
func (e *AlertEvaluator) Stop() {
	if e.subCancel != nil {
		e.subCancel()
	}
}

// ShouldFire determines whether a threshold crossing condition has occurred.
func ShouldFire(direction domain.AlertDirection, targetRate, currentPrice decimal.Decimal, lastEvalRate *decimal.Decimal) bool {
	switch direction {
	case domain.AlertDirectionAtOrAbove:
		if currentPrice.LessThan(targetRate) {
			return false
		}
		if lastEvalRate == nil {
			return true
		}
		return lastEvalRate.LessThan(targetRate)
	case domain.AlertDirectionAtOrBelow:
		if currentPrice.GreaterThan(targetRate) {
			return false
		}
		if lastEvalRate == nil {
			return true
		}
		return lastEvalRate.GreaterThan(targetRate)
	default:
		return false
	}
}

// EvaluatePriceTick processes a live market tick against in-memory interval trees.
// Returns matched alerts that fired.
// Time complexity: O(log N + K) tree query + O(K) atomic CAS executions.
func (e *AlertEvaluator) EvaluatePriceTick(ctx context.Context, pair string, currentPrice decimal.Decimal) ([]*domain.RateAlert, error) {
	if currentPrice.Sign() <= 0 {
		return nil, fmt.Errorf("current price must be positive")
	}

	e.mu.Lock()
	tree := e.trees[pair]
	prevPrice, hasPrev := e.lastPrices[pair]
	e.lastPrices[pair] = currentPrice
	e.mu.Unlock()

	if tree == nil || tree.Size() == 0 {
		return nil, nil
	}

	var qLow, qHigh decimal.Decimal
	if !hasPrev {
		qLow = decimal.Zero
		qHigh = decimal.NewFromInt(1_000_000_000)
	} else if currentPrice.LessThan(prevPrice) {
		qLow = currentPrice
		qHigh = prevPrice
	} else {
		qLow = prevPrice
		qHigh = currentPrice
	}

	e.mu.RLock()
	candidates := tree.QueryRange(qLow, qHigh)
	e.mu.RUnlock()

	var fired []*domain.RateAlert
	for _, alert := range candidates {
		if !alert.Active {
			continue
		}
		if alert.ExpiresAt != nil && !alert.ExpiresAt.After(time.Now().UTC()) {
			continue
		}

		var lastRate *decimal.Decimal
		if alert.LastEvalRate != nil {
			lastRate = alert.LastEvalRate
		} else if hasPrev {
			lastRate = &prevPrice
		}

		if !ShouldFire(alert.Direction, alert.TargetRate, currentPrice, lastRate) {
			continue
		}

		firedOnce, err := e.recordAtomicCAS(ctx, alert, currentPrice)
		if err != nil {
			log.Error().Err(err).Str("alert_id", alert.ID).Msg("failed atomic CAS for rate alert firing")
			continue
		}
		if !firedOnce {
			// Already fired by another worker or process.
			continue
		}

		// Update in-memory snapshot
		now := time.Now().UTC()
		alert.LastFiredAt = &now
		alert.LastEvalRate = &currentPrice

		e.dispatchWebhook(ctx, alert, currentPrice, lastRate)
		fired = append(fired, alert)
	}

	return fired, nil
}

// recordAtomicCAS guarantees exactly-once firing per crossing across horizontal nodes.
func (e *AlertEvaluator) recordAtomicCAS(ctx context.Context, alert *domain.RateAlert, currentPrice decimal.Decimal) (bool, error) {
	// Attempt Redis atomic CAS if Redis universal client is available.
	if e.redis != nil {
		key := fmt.Sprintf("%s%s", AlertFiringRedisPrefix, alert.ID)
		val := fmt.Sprintf("%s:%s", currentPrice.String(), alert.Direction)
		ttlSeconds := int(AlertFiringLockTTL.Seconds())

		res, err := atomicCASScript.Run(ctx, e.redis, []string{key}, val, ttlSeconds).Int()
		if err == nil && res == 0 {
			return false, nil
		}
	}

	// Persist atomic CAS to PostgreSQL if repo is configured.
	if e.repo != nil {
		return e.repo.RecordFiringAtomicCAS(ctx, alert.ID, currentPrice, alert.LastFiredAt)
	}

	// In-memory atomic CAS fallback when running without external Redis or PostgreSQL.
	e.casMu.Lock()
	defer e.casMu.Unlock()
	if alert.LastFiredAt != nil {
		return false, nil
	}
	now := time.Now().UTC()
	alert.LastFiredAt = &now
	alert.LastEvalRate = &currentPrice
	return true, nil
}

func (e *AlertEvaluator) dispatchWebhook(ctx context.Context, alert *domain.RateAlert, currentPrice decimal.Decimal, lastRate *decimal.Decimal) {
	if e.dispatcher == nil {
		return
	}
	var lastRateStr *string
	if lastRate != nil {
		s := lastRate.String()
		lastRateStr = &s
	}

	payload := RateAlertWebhookPayload{
		AlertID:      alert.ID,
		TenantID:     alert.TenantID,
		FromAsset:    alert.FromAsset,
		ToAsset:      alert.ToAsset,
		TargetRate:   alert.TargetRate,
		CurrentRate:  currentPrice,
		Direction:    string(alert.Direction),
		LastEvalRate: lastRateStr,
		TriggeredAt:  time.Now().UTC(),
	}

	if err := e.dispatcher.Dispatch(ctx, "fx.rate_alert.triggered", payload); err != nil {
		log.Error().Err(err).Str("alert_id", alert.ID).Msg("failed to dispatch rate alert webhook")
	}
}
