package fx

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// RateLockEventType represents state transition events for rate locks.
type RateLockEventType string

const (
	LockEventCreated  RateLockEventType = "CREATED"
	LockEventReserved RateLockEventType = "RESERVED"
	LockEventConsumed RateLockEventType = "CONSUMED"
	LockEventExpired  RateLockEventType = "EXPIRED"
)

// RateLockEvent records lifecycle events with microsecond timestamps.
type RateLockEvent struct {
	LockID    string            `json:"lock_id"`
	Type      RateLockEventType `json:"type"`
	Timestamp time.Time         `json:"timestamp"`
	Details   string            `json:"details,omitempty"`
}

// RateLock holds a quoted rate for a bounded window, guaranteed against slippage.
type RateLock struct {
	ID           string                `json:"id"`
	TenantID     string                `json:"tenant_id,omitempty"`
	FromAsset    string                `json:"from_asset,omitempty"`
	ToAsset      string                `json:"to_asset,omitempty"`
	LockedRate   decimal.Decimal       `json:"locked_rate,omitempty"`
	Amount       decimal.Decimal       `json:"amount,omitempty"`
	MinAmountOut float64               `json:"min_amount_out,omitempty"`
	ExpiresAt    time.Time             `json:"expires_at"`
	ConsumedAt   *time.Time            `json:"consumed_at,omitempty"`
	ConsumedBy   *string               `json:"consumed_by,omitempty"`
	Status       domain.RateLockStatus `json:"status,omitempty"`
	WalletID     *string               `json:"wallet_id,omitempty"`
	CreatedAt    time.Time             `json:"created_at"`
}

// LockRepository handles persistence and atomic state transitions of FX rate locks.
type LockRepository interface {
	Create(ctx context.Context, lock *domain.RateLock) error
	Get(ctx context.Context, id, tenantID string) (*domain.RateLock, error)
	Reserve(ctx context.Context, lockID, tenantID string) error
	ConsumeLock(ctx context.Context, lockID, conversionID string) error
	CleanupExpired(ctx context.Context) (int64, error)
	Put(lock RateLock)
	Consume(lockID string, amount float64, now time.Time) error
	RecordEvent(event RateLockEvent)
	GetEvents(lockID string) []RateLockEvent
}

type memoryLockRepository struct {
	mu     sync.Mutex
	locks  map[string]*RateLock
	events map[string][]RateLockEvent
}

type sqlLockRepository struct {
	db     *sql.DB
	mem    *memoryLockRepository
}

// NewLockRepository creates a rate lock repository. When db is nil or omitted,
// returns a high-concurrency in-memory lock state machine.
func NewLockRepository(db ...*sql.DB) LockRepository {
	mem := &memoryLockRepository{
		locks:  make(map[string]*RateLock),
		events: make(map[string][]RateLockEvent),
	}
	if len(db) > 0 && db[0] != nil {
		return &sqlLockRepository{
			db:  db[0],
			mem: mem,
		}
	}
	return mem
}

func (m *memoryLockRepository) Put(lock RateLock) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if lock.CreatedAt.IsZero() {
		lock.CreatedAt = time.Now().UTC().Truncate(time.Microsecond)
	}
	cp := lock
	m.locks[lock.ID] = &cp

	event := RateLockEvent{
		LockID:    lock.ID,
		Type:      LockEventCreated,
		Timestamp: lock.CreatedAt,
		Details:   fmt.Sprintf("lock created with expires_at %s", lock.ExpiresAt.Format(time.RFC3339Nano)),
	}
	m.events[lock.ID] = append(m.events[lock.ID], event)
}

func (m *memoryLockRepository) Consume(lockID string, amount float64, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	lock, ok := m.locks[lockID]
	if !ok {
		return domain.ErrQuoteExpired
	}

	evalTime := now.UTC().Truncate(time.Microsecond)

	// Sub-second precision check: even 1 microsecond past deadline is expired
	if !evalTime.Before(lock.ExpiresAt.UTC().Truncate(time.Microsecond)) {
		m.events[lockID] = append(m.events[lockID], RateLockEvent{
			LockID:    lockID,
			Type:      LockEventExpired,
			Timestamp: evalTime,
			Details:   "attempted consumption after expires_at",
		})
		return domain.ErrQuoteExpired
	}

	if lock.ConsumedAt != nil {
		return domain.ErrQuoteExpired
	}

	// Slippage protection: validate min_amount_out
	if lock.MinAmountOut > 0 && amount < lock.MinAmountOut {
		return domain.ErrSlippageExceeded
	}

	consumedAt := evalTime
	lock.ConsumedAt = &consumedAt

	m.events[lockID] = append(m.events[lockID], RateLockEvent{
		LockID:    lockID,
		Type:      LockEventConsumed,
		Timestamp: evalTime,
		Details:   fmt.Sprintf("consumed with amount %f", amount),
	})

	return nil
}

func (m *memoryLockRepository) RecordEvent(event RateLockEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC().Truncate(time.Microsecond)
	}
	m.events[event.LockID] = append(m.events[event.LockID], event)
}

func (m *memoryLockRepository) GetEvents(lockID string) []RateLockEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	evs := m.events[lockID]
	res := make([]RateLockEvent, len(evs))
	copy(res, evs)
	return res
}

func (m *memoryLockRepository) Create(_ context.Context, lock *domain.RateLock) error {
	if lock.ID == "" {
		lock.ID = uuid.New().String()
	}
	if lock.CreatedAt.IsZero() {
		lock.CreatedAt = time.Now().UTC().Truncate(time.Microsecond)
	}
	m.Put(RateLock{
		ID:         lock.ID,
		TenantID:   lock.TenantID,
		FromAsset:  lock.FromAsset,
		ToAsset:    lock.ToAsset,
		LockedRate: lock.LockedRate,
		Amount:     lock.Amount,
		ExpiresAt:  lock.ExpiresAt,
		ConsumedAt: lock.ConsumedAt,
		ConsumedBy: lock.ConsumedBy,
		CreatedAt:  lock.CreatedAt,
	})
	return nil
}

func (m *memoryLockRepository) Get(_ context.Context, id, tenantID string) (*domain.RateLock, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	lock, ok := m.locks[id]
	if !ok || (tenantID != "" && lock.TenantID != tenantID) {
		return nil, domain.ErrWalletNotFound
	}
	return &domain.RateLock{
		ID:         lock.ID,
		TenantID:   lock.TenantID,
		FromAsset:  lock.FromAsset,
		ToAsset:    lock.ToAsset,
		LockedRate: lock.LockedRate,
		Amount:     lock.Amount,
		ExpiresAt:  lock.ExpiresAt,
		ConsumedAt: lock.ConsumedAt,
		ConsumedBy: lock.ConsumedBy,
		CreatedAt:  lock.CreatedAt,
	}, nil
}

func (m *memoryLockRepository) Reserve(_ context.Context, lockID, tenantID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	lock, ok := m.locks[lockID]
	if !ok || (tenantID != "" && lock.TenantID != "" && lock.TenantID != tenantID) {
		return domain.ErrWalletNotFound
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	if !now.Before(lock.ExpiresAt.UTC().Truncate(time.Microsecond)) {
		m.events[lockID] = append(m.events[lockID], RateLockEvent{
			LockID:    lockID,
			Type:      LockEventExpired,
			Timestamp: now,
			Details:   "reservation rejected after expiration",
		})
		return domain.ErrQuoteExpired
	}

	if lock.ConsumedAt != nil {
		return domain.ErrQuoteAlreadyUsed
	}

	m.events[lockID] = append(m.events[lockID], RateLockEvent{
		LockID:    lockID,
		Type:      LockEventReserved,
		Timestamp: now,
		Details:   fmt.Sprintf("reserved by tenant %s", tenantID),
	})
	return nil
}

func (m *memoryLockRepository) ConsumeLock(_ context.Context, lockID, conversionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	lock, ok := m.locks[lockID]
	if !ok {
		return domain.ErrQuoteExpired
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	if !now.Before(lock.ExpiresAt.UTC().Truncate(time.Microsecond)) {
		m.events[lockID] = append(m.events[lockID], RateLockEvent{
			LockID:    lockID,
			Type:      LockEventExpired,
			Timestamp: now,
			Details:   "consumption attempt after expiration deadline",
		})
		return domain.ErrQuoteExpired
	}

	if lock.ConsumedAt != nil {
		return domain.ErrQuoteAlreadyUsed
	}

	lock.ConsumedAt = &now
	lock.ConsumedBy = &conversionID

	m.events[lockID] = append(m.events[lockID], RateLockEvent{
		LockID:    lockID,
		Type:      LockEventConsumed,
		Timestamp: now,
		Details:   fmt.Sprintf("consumed by conversion %s", conversionID),
	})

	return nil
}

func (m *memoryLockRepository) CleanupExpired(_ context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UTC().Truncate(time.Microsecond)
	var count int64
	for id, lock := range m.locks {
		if lock.ConsumedAt == nil && !lock.ExpiresAt.After(now) {
			delete(m.locks, id)
			count++
		}
	}
	return count, nil
}

// SQL implementation delegating and executing atomic DB statements

func (r *sqlLockRepository) Put(lock RateLock) {
	r.mem.Put(lock)
}

func (r *sqlLockRepository) Consume(lockID string, amount float64, now time.Time) error {
	return r.mem.Consume(lockID, amount, now)
}

func (r *sqlLockRepository) RecordEvent(event RateLockEvent) {
	r.mem.RecordEvent(event)
}

func (r *sqlLockRepository) GetEvents(lockID string) []RateLockEvent {
	return r.mem.GetEvents(lockID)
}

func (r *sqlLockRepository) Create(ctx context.Context, lock *domain.RateLock) error {
	if lock.ID == "" {
		lock.ID = uuid.New().String()
	}
	if lock.Status == "" {
		lock.Status = domain.RateLockStatusActive
	}
	lock.CreatedAt = time.Now().UTC().Truncate(time.Microsecond)

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO fx_rate_locks 
		(id, tenant_id, from_asset, to_asset, locked_rate, amount, expires_at, status, wallet_id, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, lock.ID, lock.TenantID, lock.FromAsset, lock.ToAsset, lock.LockedRate,
		lock.Amount, lock.ExpiresAt, string(lock.Status), lock.WalletID, lock.CreatedAt)

	if err == nil {
		r.RecordEvent(RateLockEvent{
			LockID:    lock.ID,
			Type:      LockEventCreated,
			Timestamp: lock.CreatedAt,
			Details:   "persisted in database",
		})
	}

	return err
}

func (r *sqlLockRepository) Get(ctx context.Context, id, tenantID string) (*domain.RateLock, error) {
	lock := &domain.RateLock{}
	var statusStr string
	err := r.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, from_asset, to_asset, locked_rate, amount,
		       expires_at, consumed_at, consumed_by, COALESCE(status, 'active'), wallet_id, created_at
		FROM fx_rate_locks
		WHERE id = $1 AND tenant_id = $2
	`, id, tenantID).Scan(
		&lock.ID, &lock.TenantID, &lock.FromAsset, &lock.ToAsset, &lock.LockedRate,
		&lock.Amount, &lock.ExpiresAt, &lock.ConsumedAt, &lock.ConsumedBy, &statusStr, &lock.WalletID, &lock.CreatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, domain.ErrWalletNotFound
	}
	if err == nil {
		lock.Status = domain.RateLockStatus(statusStr)
	}
	return lock, err
}

func (r *sqlLockRepository) Reserve(ctx context.Context, lockID, tenantID string) error {
	lock, err := r.Get(ctx, lockID, tenantID)
	if err != nil {
		return err
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	if !now.Before(lock.ExpiresAt.UTC().Truncate(time.Microsecond)) {
		r.RecordEvent(RateLockEvent{
			LockID:    lockID,
			Type:      LockEventExpired,
			Timestamp: now,
			Details:   "reservation rejected after expiration",
		})
		return domain.ErrQuoteExpired
	}

	if lock.ConsumedAt != nil {
		return domain.ErrQuoteAlreadyUsed
	}

	r.RecordEvent(RateLockEvent{
		LockID:    lockID,
		Type:      LockEventReserved,
		Timestamp: now,
		Details:   fmt.Sprintf("reserved by tenant %s", tenantID),
	})
	return nil
}

func (r *sqlLockRepository) ConsumeLock(ctx context.Context, lockID, conversionID string) error {
	now := time.Now().UTC().Truncate(time.Microsecond)

	result, err := r.db.ExecContext(ctx, `
		UPDATE fx_rate_locks
		SET consumed_at = $1, consumed_by = $2, status = 'consumed'
		WHERE id = $3 AND consumed_at IS NULL AND expires_at > $1
	`, now, conversionID, lockID)

	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		var expiresAt time.Time
		var consumedAt *time.Time
		qErr := r.db.QueryRowContext(ctx, `SELECT expires_at, consumed_at FROM fx_rate_locks WHERE id = $1`, lockID).Scan(&expiresAt, &consumedAt)
		if qErr != nil {
			return domain.ErrQuoteExpired
		}
		if consumedAt != nil {
			return domain.ErrQuoteAlreadyUsed
		}
		if !now.Before(expiresAt) {
			r.RecordEvent(RateLockEvent{
				LockID:    lockID,
				Type:      LockEventExpired,
				Timestamp: now,
				Details:   "db consumption rejected after expiry",
			})
			return domain.ErrQuoteExpired
		}
		return domain.ErrQuoteExpired
	}

	r.RecordEvent(RateLockEvent{
		LockID:    lockID,
		Type:      LockEventConsumed,
		Timestamp: now,
		Details:   fmt.Sprintf("db consumed by conversion %s", conversionID),
	})

	return nil
}

func (r *sqlLockRepository) CleanupExpired(ctx context.Context) (int64, error) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	result, err := r.db.ExecContext(ctx, `
		DELETE FROM fx_rate_locks
		WHERE consumed_at IS NULL AND expires_at <= $1
	`, now)

	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

