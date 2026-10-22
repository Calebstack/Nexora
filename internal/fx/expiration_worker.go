package fx

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/postgres"
	"github.com/Lumen-Nexora/Nexora/internal/queue"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog/log"
)

const (
	// DefaultRateLockBatchSize is the standard atomic batch size per requirement.
	DefaultRateLockBatchSize = 100
)

// ExpirationTelemetry tracks telemetry metrics for expired vs consumed rate locks.
type ExpirationTelemetry struct {
	ExpiredCount           uint64    `json:"expired_count"`
	ConsumedCount          uint64    `json:"consumed_count"`
	ActiveCount            uint64    `json:"active_count"`
	ExpiredToConsumedRatio float64   `json:"expired_to_consumed_ratio"`
	LastSweepAt            time.Time `json:"last_sweep_at"`
}

// RateLockExpirationWorker manages asynchronous identification, atomic status updates,
// and balance hold releases for expired rate locks.
type RateLockExpirationWorker interface {
	HandleExpireLocks(ctx context.Context, task *asynq.Task) error
	ExpireBatch(ctx context.Context, batchSize int) (int64, error)
	GetTelemetry(ctx context.Context) (ExpirationTelemetry, error)
}

type expirationWorker struct {
	db        postgres.DB
	memStore  *memoryLockRepository
	telemetry ExpirationTelemetry
	telMu     sync.RWMutex

	// Atomic counters for negligible overhead in-memory tracking
	cumExpired  uint64
	cumConsumed uint64
}

// NewExpirationWorker constructs an Asynq-powered rate lock cleanup worker.
func NewExpirationWorker(db postgres.DB) RateLockExpirationWorker {
	return &expirationWorker{
		db: db,
	}
}

// NewMemoryExpirationWorker constructs an in-memory expiration worker for test isolation.
func NewMemoryExpirationWorker(mem *memoryLockRepository) RateLockExpirationWorker {
	return &expirationWorker{
		memStore: mem,
	}
}

// HandleExpireLocks processes scheduled or enqueued rate lock expiration tasks.
// Drains the expired backlog in atomic batches of 100 with negligible CPU load.
func (w *expirationWorker) HandleExpireLocks(ctx context.Context, task *asynq.Task) error {
	ctx = queue.ContextFromTask(ctx, task)

	batchSize := DefaultRateLockBatchSize
	if task != nil && len(task.Payload()) > 0 {
		var payload queue.ExpireRateLocksPayload
		if err := json.Unmarshal(task.Payload(), &payload); err == nil && payload.BatchSize > 0 {
			batchSize = payload.BatchSize
		}
	}

	totalExpired := int64(0)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := w.ExpireBatch(ctx, batchSize)
		if err != nil {
			log.Error().Err(err).Msg("rate lock expiration batch failed")
			return err
		}

		totalExpired += n
		if n < int64(batchSize) {
			// Backlog drained
			break
		}
	}

	// Emit telemetry metrics
	telemetry, err := w.GetTelemetry(ctx)
	if err == nil {
		log.Info().
			Int64("swept_in_run", totalExpired).
			Uint64("total_expired", telemetry.ExpiredCount).
			Uint64("total_consumed", telemetry.ConsumedCount).
			Uint64("total_active", telemetry.ActiveCount).
			Float64("expired_to_consumed_ratio", telemetry.ExpiredToConsumedRatio).
			Msg("rate lock expiration sweep telemetry emitted")
	}

	return nil
}

// ExpireBatch executes an atomic transaction that identifies expired locks,
// updates status to 'expired', and releases any associated balance holds.
func (w *expirationWorker) ExpireBatch(ctx context.Context, batchSize int) (int64, error) {
	if batchSize <= 0 {
		batchSize = DefaultRateLockBatchSize
	}

	if w.memStore != nil {
		return w.expireBatchMemory(ctx, batchSize)
	}

	if w.db == nil {
		return 0, nil
	}

	var expiredCount int64

	err := postgres.RunInTx(ctx, w.db, func(txCtx context.Context) error {
		tx := postgres.TxFromContext(txCtx, w.db)

		// 1. Query database for locks where expires_at < NOW() AND consumed_at IS NULL AND status = 'active'
		// 2. Updates status to expired in atomic batches of 100
		// 3. Releases balance holds on custodial accounts synchronously within database transaction
		// Uses CTE with FOR UPDATE SKIP LOCKED to prevent lock contention across multiple workers.
		row := tx.QueryRow(txCtx, `
			WITH target_locks AS (
				SELECT id, wallet_id
				FROM fx_rate_locks
				WHERE expires_at < NOW()
				  AND consumed_at IS NULL
				  AND status = 'active'
				ORDER BY expires_at ASC
				LIMIT $1
				FOR UPDATE SKIP LOCKED
			),
			updated_locks AS (
				UPDATE fx_rate_locks l
				SET status = 'expired'
				FROM target_locks tl
				WHERE l.id = tl.id
				RETURNING l.id, tl.wallet_id
			),
			released_holds AS (
				UPDATE fx_rate_lock_balance_holds h
				SET released = TRUE, released_at = NOW()
				FROM updated_locks ul
				WHERE h.lock_id = ul.id AND h.released = FALSE
				RETURNING h.id, h.wallet_id, h.asset_code, h.issuer, h.amount
			),
			custodial_balance_releases AS (
				UPDATE anchor_payout_balance_holds b
				SET held = GREATEST(0, b.held - rh.amount), updated_at = NOW()
				FROM released_holds rh
				WHERE b.wallet_id = rh.wallet_id 
				  AND b.asset_code = rh.asset_code 
				  AND b.issuer = rh.issuer
				RETURNING b.wallet_id
			)
			SELECT COUNT(*) FROM updated_locks;
		`, batchSize)

		if err := row.Scan(&expiredCount); err != nil {
			return fmt.Errorf("execute atomic rate lock expiration and balance release: %w", err)
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	if expiredCount > 0 {
		atomic.AddUint64(&w.cumExpired, uint64(expiredCount))
	}

	return expiredCount, nil
}

func (w *expirationWorker) expireBatchMemory(_ context.Context, batchSize int) (int64, error) {
	w.memStore.mu.Lock()
	defer w.memStore.mu.Unlock()

	now := time.Now().UTC()
	var count int64
	for _, lock := range w.memStore.locks {
		if count >= int64(batchSize) {
			break
		}
		if lock.ConsumedAt == nil && lock.ExpiresAt.Before(now) {
			delete(w.memStore.locks, lock.ID)
			count++
		}
	}
	atomic.AddUint64(&w.cumExpired, uint64(count))
	return count, nil
}

// GetTelemetry computes the point-in-time ratio of expired locks vs consumed locks.
func (w *expirationWorker) GetTelemetry(ctx context.Context) (ExpirationTelemetry, error) {
	if w.db == nil {
		w.telMu.RLock()
		defer w.telMu.RUnlock()
		exp := atomic.LoadUint64(&w.cumExpired)
		cons := atomic.LoadUint64(&w.cumConsumed)
		ratio := 0.0
		if cons > 0 {
			ratio = float64(exp) / float64(cons)
		} else if exp > 0 {
			ratio = 1.0
		}
		return ExpirationTelemetry{
			ExpiredCount:           exp,
			ConsumedCount:          cons,
			ExpiredToConsumedRatio: ratio,
			LastSweepAt:            time.Now().UTC(),
		}, nil
	}

	var expired, consumed, active uint64
	row := w.db.QueryRow(ctx, `
		SELECT 
			COALESCE(COUNT(*) FILTER (WHERE status = 'expired'), 0),
			COALESCE(COUNT(*) FILTER (WHERE status = 'consumed'), 0),
			COALESCE(COUNT(*) FILTER (WHERE status = 'active'), 0)
		FROM fx_rate_locks
	`)

	if err := row.Scan(&expired, &consumed, &active); err != nil {
		return ExpirationTelemetry{}, fmt.Errorf("query rate lock telemetry metrics: %w", err)
	}

	ratio := 0.0
	if consumed > 0 {
		ratio = float64(expired) / float64(consumed)
	} else if expired > 0 {
		ratio = 1.0
	}
	// Round to 4 decimal places
	ratio = math.Round(ratio*10000) / 10000

	tel := ExpirationTelemetry{
		ExpiredCount:           expired,
		ConsumedCount:          consumed,
		ActiveCount:            active,
		ExpiredToConsumedRatio: ratio,
		LastSweepAt:            time.Now().UTC(),
	}

	w.telMu.Lock()
	w.telemetry = tel
	w.telMu.Unlock()

	return tel, nil
}
