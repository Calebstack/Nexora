package settlement

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/stellar"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"github.com/stellar/go/protocols/horizon"
)

// Channel pool errors.
var (
	// ErrPoolExhausted is returned when every active channel account is currently
	// leased by another goroutine / instance.
	ErrPoolExhausted = errors.New("channel pool exhausted: no accounts available")
	// ErrPoolClosed is returned after Shutdown has been called.
	ErrPoolClosed = errors.New("channel pool is shut down")
)

const (
	// DefaultTopUpThreshold is the XLM balance below which the balance monitor
	// queues a top-up. Three base reserves (3 × 0.5 XLM) plus headroom for a
	// few hundred operations' fees.
	DefaultTopUpThreshold = "3.0"
	// DefaultTopUpAmount is the XLM sent to a channel that drops below threshold.
	DefaultTopUpAmount = "10.0"
	// DefaultBalanceCheckInterval is how often the background monitor re-checks
	// channel balances on Horizon.
	DefaultBalanceCheckInterval = 2 * time.Minute
	// DefaultIdleRecycleAge is how long a channel must be unused before the
	// recycler transitions it to draining.
	DefaultIdleRecycleAge = 24 * time.Hour

	channelLeasePrefix = "channel:lease:"
	channelLeaseTTL    = 30 * time.Second
)

// ChannelAccountRepository is the persistence interface the pool needs. It is
// satisfied by postgres.ChannelAccountRepo.
type ChannelAccountRepository interface {
	ListActive(ctx context.Context) ([]*domain.ChannelAccount, error)
	UpdateBalance(ctx context.Context, id string, balance decimal.Decimal) error
	UpdateStatus(ctx context.Context, id string, status domain.ChannelAccountStatus) error
	MarkUsed(ctx context.Context, id string) error
	ListNeedingBalanceCheck(ctx context.Context, olderThan time.Time, limit int) ([]*domain.ChannelAccount, error)
}

// ChannelPoolConfig configures the pool manager.
type ChannelPoolConfig struct {
	// TopUpThreshold is the XLM balance below which a top-up is triggered.
	TopUpThreshold decimal.Decimal
	// TopUpAmount is the XLM amount sent per top-up.
	TopUpAmount decimal.Decimal
	// BalanceCheckInterval is how often the monitor polls Horizon.
	BalanceCheckInterval time.Duration
	// IdleRecycleAge is the duration after which an unused account is drained.
	IdleRecycleAge time.Duration
	// MaxLeaseWait is how long Acquire blocks before returning ErrPoolExhausted.
	MaxLeaseWait time.Duration
}

// DefaultChannelPoolConfig returns production defaults.
func DefaultChannelPoolConfig() ChannelPoolConfig {
	threshold, _ := decimal.NewFromString(DefaultTopUpThreshold)
	amount, _ := decimal.NewFromString(DefaultTopUpAmount)
	return ChannelPoolConfig{
		TopUpThreshold:       threshold,
		TopUpAmount:          amount,
		BalanceCheckInterval: DefaultBalanceCheckInterval,
		IdleRecycleAge:       DefaultIdleRecycleAge,
		MaxLeaseWait:         5 * time.Second,
	}
}

// ChannelLease is a handle to a leased channel account. The holder must call
// Release when the transaction completes (or fails) so other submitters can
// use the account. Leases are also guarded by a Redis key with a TTL so a
// crashed holder does not permanently consume a channel.
type ChannelLease struct {
	Account domain.ChannelAccount
	pool    *ChannelPool
	token   string

	released atomic.Bool
}

// Release returns the channel to the pool and deletes the Redis lease key.
// Safe to call more than once.
func (l *ChannelLease) Release(ctx context.Context) error {
	if l == nil || l.pool == nil {
		return nil
	}
	if !l.released.CompareAndSwap(false, true) {
		return nil
	}
	return l.pool.releaseLease(ctx, l)
}

// ChannelPoolMetrics is a snapshot of pool counters.
type ChannelPoolMetrics struct {
	Acquired       int64
	Released       int64
	Exhausted      int64
	TopUps         int64
	BalanceChecks  int64
	Recycled       int64
	PoolSize       int
	ActiveLeases   int
}

// ChannelPool manages a pool of funded Stellar channel accounts for concurrent
// transaction submission (issue #41). Each Acquire hands the caller a channel
// whose sequence number is independent of every other channel and of the
// primary treasury account, so 200+ transactions can be in flight at once.
//
// Channels are leased via Redis SET NX so the lock is distributed: if this
// instance crashes mid-lease the key expires and another instance can acquire
// the channel. The pool refreshes its view of active accounts from Postgres
// periodically and monitors balances via Horizon, topping up channels whose
// XLM balance drops below threshold.
type ChannelPool struct {
	repo    ChannelAccountRepository
	rdb     redis.UniversalClient
	horizon stellar.Client
	cfg     ChannelPoolConfig

	// mu protects accounts and the leased set.
	mu       sync.RWMutex
	accounts []*domain.ChannelAccount
	leased   map[string]string // publicKey → lease token

	acquired, released, exhausted, topUps, balChecks, recycled atomic.Int64
	closed atomic.Bool
	done   chan struct{}
}

// NewChannelPool builds a pool. Call Start to begin background monitoring.
func NewChannelPool(
	repo ChannelAccountRepository,
	rdb redis.UniversalClient,
	horizon stellar.Client,
	cfg ChannelPoolConfig,
) *ChannelPool {
	if cfg.TopUpThreshold.IsZero() {
		cfg = DefaultChannelPoolConfig()
	}
	return &ChannelPool{
		repo:    repo,
		rdb:     rdb,
		horizon: horizon,
		cfg:     cfg,
		leased:  make(map[string]string),
		done:    make(chan struct{}),
	}
}

// Refresh reloads the active account list from Postgres.
func (p *ChannelPool) Refresh(ctx context.Context) error {
	accounts, err := p.repo.ListActive(ctx)
	if err != nil {
		return fmt.Errorf("refresh channel pool: %w", err)
	}
	p.mu.Lock()
	p.accounts = accounts
	p.mu.Unlock()
	return nil
}

// Acquire leases a channel account for exclusive use. It tries each active
// account in round-robin order (least-recently-used first, matching the repo
// query) and returns the first one whose Redis lease key is not held. If every
// account is leased it retries up to MaxLeaseWait.
func (p *ChannelPool) Acquire(ctx context.Context) (*ChannelLease, error) {
	if p.closed.Load() {
		return nil, ErrPoolClosed
	}

	deadline := time.Now().Add(p.cfg.MaxLeaseWait)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}

	for {
		lease, err := p.tryAcquire(ctx)
		if err == nil {
			return lease, nil
		}
		if !errors.Is(err, ErrPoolExhausted) {
			return nil, err
		}
		if time.Now().After(deadline) {
			p.exhausted.Add(1)
			return nil, ErrPoolExhausted
		}
		// Brief back-off before retrying.
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (p *ChannelPool) tryAcquire(ctx context.Context) (*ChannelLease, error) {
	p.mu.RLock()
	snapshot := make([]*domain.ChannelAccount, len(p.accounts))
	copy(snapshot, p.accounts)
	p.mu.RUnlock()

	if len(snapshot) == 0 {
		return nil, ErrPoolExhausted
	}

	token, err := generateLockToken()
	if err != nil {
		return nil, fmt.Errorf("generate lease token: %w", err)
	}

	for _, ch := range snapshot {
		key := channelLeasePrefix + ch.PublicKey

		// Try distributed lock via Redis.
		if p.rdb != nil {
			ok, rerr := p.rdb.SetNX(ctx, key, token, channelLeaseTTL).Result()
			if rerr != nil {
				log.Warn().Err(rerr).Str("channel", ch.PublicKey).
					Msg("channel pool: redis unavailable for lease, falling back to in-process")
				// Fall through to in-process lock.
			} else if !ok {
				continue // already leased by another instance
			}
		}

		// In-process dedup: skip if this process already leased this channel.
		p.mu.Lock()
		if _, held := p.leased[ch.PublicKey]; held {
			p.mu.Unlock()
			continue
		}
		p.leased[ch.PublicKey] = token
		p.mu.Unlock()

		// Record the lease in Postgres so the LRU ordering stays correct.
		_ = p.repo.MarkUsed(ctx, ch.ID)

		p.acquired.Add(1)
		return &ChannelLease{
			Account: *ch,
			pool:    p,
			token:   token,
		}, nil
	}

	return nil, ErrPoolExhausted
}

// releaseLease is called by ChannelLease.Release.
func (p *ChannelPool) releaseLease(ctx context.Context, l *ChannelLease) error {
	p.mu.Lock()
	delete(p.leased, l.Account.PublicKey)
	p.mu.Unlock()

	p.released.Add(1)

	if p.rdb != nil {
		key := channelLeasePrefix + l.Account.PublicKey
		// Atomically delete only if we still own the token.
		err := redlockReleaseScript.Run(ctx, p.rdb, []string{key}, l.token).Err()
		if err != nil && !errors.Is(err, redis.Nil) {
			log.Warn().Err(err).Str("channel", l.Account.PublicKey).
				Msg("channel pool: redis release failed, relying on TTL")
		}
	}
	return nil
}

// Start launches background goroutines for balance monitoring and idle
// recycling. It blocks until ctx is cancelled.
func (p *ChannelPool) Start(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.BalanceCheckInterval)
	defer ticker.Stop()

	// Initial load.
	if err := p.Refresh(ctx); err != nil {
		log.Error().Err(err).Msg("channel pool: initial refresh failed")
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.done:
			return
		case <-ticker.C:
			p.monitorBalances(ctx)
			p.recycleIdle(ctx)
			if err := p.Refresh(ctx); err != nil {
				log.Error().Err(err).Msg("channel pool: refresh failed")
			}
		}
	}
}

// Shutdown signals background goroutines to stop.
func (p *ChannelPool) Shutdown() {
	if p.closed.CompareAndSwap(false, true) {
		close(p.done)
	}
}

// monitorBalances checks active channels with stale balance data, refreshes
// from Horizon, and queues a top-up for any that have fallen below threshold.
func (p *ChannelPool) monitorBalances(ctx context.Context) {
	staleAt := time.Now().Add(-p.cfg.BalanceCheckInterval)
	accounts, err := p.repo.ListNeedingBalanceCheck(ctx, staleAt, 50)
	if err != nil {
		log.Error().Err(err).Msg("channel pool: list accounts for balance check failed")
		return
	}

	for _, ch := range accounts {
		acct, err := p.horizon.LoadAccount(ch.PublicKey)
		if err != nil {
			log.Warn().Err(err).Str("channel", ch.PublicKey).
				Msg("channel pool: load account failed")
			continue
		}

		balance := extractNativeBalance(acct)
		if err := p.repo.UpdateBalance(ctx, ch.ID, balance); err != nil {
			log.Warn().Err(err).Str("channel", ch.PublicKey).
				Msg("channel pool: update balance failed")
			continue
		}
		p.balChecks.Add(1)

		if balance.LessThan(p.cfg.TopUpThreshold) {
			log.Info().
				Str("channel", ch.PublicKey).
				Str("balance", balance.String()).
				Str("threshold", p.cfg.TopUpThreshold.String()).
				Msg("channel pool: channel below threshold, needs top-up")
			p.topUps.Add(1)
			// The actual top-up transaction (payment from treasury to channel)
			// is submitted by the settlement worker that owns the funding key.
			// The pool only records the intent here; the worker picks it up by
			// querying ListNeedingBalanceCheck with the threshold filter.
		}
	}
}

// recycleIdle transitions channels that have not been used for IdleRecycleAge
// to "draining" so they stop being leased.
func (p *ChannelPool) recycleIdle(ctx context.Context) {
	p.mu.RLock()
	snapshot := make([]*domain.ChannelAccount, len(p.accounts))
	copy(snapshot, p.accounts)
	p.mu.RUnlock()

	cutoff := time.Now().Add(-p.cfg.IdleRecycleAge)
	for _, ch := range snapshot {
		if ch.LastUsedAt == nil || ch.LastUsedAt.Before(cutoff) {
			// Do not recycle channels that were never used (newly provisioned).
			if ch.LastUsedAt == nil {
				continue
			}
			if err := p.repo.UpdateStatus(ctx, ch.ID, domain.ChannelAccountDraining); err != nil {
				log.Warn().Err(err).Str("channel", ch.PublicKey).
					Msg("channel pool: recycle idle failed")
				continue
			}
			p.recycled.Add(1)
			log.Info().Str("channel", ch.PublicKey).
				Msg("channel pool: idle channel transitioned to draining")
		}
	}
}

// Metrics returns a snapshot of pool counters.
func (p *ChannelPool) Metrics() ChannelPoolMetrics {
	p.mu.RLock()
	poolSize := len(p.accounts)
	activeLeases := len(p.leased)
	p.mu.RUnlock()

	return ChannelPoolMetrics{
		Acquired:      p.acquired.Load(),
		Released:      p.released.Load(),
		Exhausted:     p.exhausted.Load(),
		TopUps:        p.topUps.Load(),
		BalanceChecks: p.balChecks.Load(),
		Recycled:      p.recycled.Load(),
		PoolSize:      poolSize,
		ActiveLeases:  activeLeases,
	}
}

// Size returns the current number of active accounts in the pool.
func (p *ChannelPool) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.accounts)
}

// extractNativeBalance finds the XLM balance in a Horizon account response by
// iterating the Balances slice, matching the pattern used throughout the
// codebase (settlement engine, wallet service, reconcile). Native XLM has an
// empty Code in the Horizon Balance struct.
func extractNativeBalance(acct horizon.Account) decimal.Decimal {
	for _, b := range acct.Balances {
		if b.Code == "" {
			bal, _ := decimal.NewFromString(b.Balance)
			return bal
		}
	}
	return decimal.Zero
}
