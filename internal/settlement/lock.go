package settlement

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const (
	// DefaultLockTTL is the default millisecond-granularity lock TTL held during
	// Horizon broadcast and confirmation.
	DefaultLockTTL = 15000 * time.Millisecond

	// DefaultClockDriftFactor is the Redlock clock drift factor (1%).
	DefaultClockDriftFactor = 0.01

	// DefaultBaseClockDrift is the constant clock drift safety margin (2ms).
	DefaultBaseClockDrift = 2 * time.Millisecond

	// DefaultPerNodeTimeout is the maximum time allowed per Redis node during lock acquisition
	// so dead or partitioned nodes fail fast without blocking quorum.
	DefaultPerNodeTimeout = 50 * time.Millisecond

	// DefaultLockRetryDelay is the base delay between Redlock acquisition attempts.
	DefaultLockRetryDelay = 25 * time.Millisecond

	// DefaultLockRetryJitter is the maximum random jitter added to retry backoff.
	DefaultLockRetryJitter = 25 * time.Millisecond

	// DefaultLockKeyPrefix namespaces transfer submission locks in Redis.
	DefaultLockKeyPrefix = "settlement:tx:lock:"
)

var (
	// ErrLockNotAcquired is returned when a worker cannot obtain quorum across the Redis cluster.
	ErrLockNotAcquired = errors.New("distributed submission lock not acquired: quorum not reached")

	// ErrInvalidRedlockCluster is returned when creating a RedlockManager with no Redis nodes.
	ErrInvalidRedlockCluster = errors.New("redlock cluster requires at least one redis node")

	// ErrInvalidTransferID is returned when attempting to lock an empty transfer ID.
	ErrInvalidTransferID = errors.New("transfer ID must not be empty")

	// ErrInvalidLockTTL is returned when the requested lock TTL is less than 1 millisecond.
	ErrInvalidLockTTL = errors.New("lock TTL must be at least 1ms")
)

// redlockReleaseScript atomically deletes the lock key only if its value matches the
// caller's unique random token, preventing a worker whose lock expired from removing
// another worker's subsequently acquired lock.
var redlockReleaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end
`)

// redlockExtendScript atomically refreshes the millisecond TTL on a lock key only if
// its value still matches the caller's unique random token.
var redlockExtendScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
	return 0
end
`)

// SubmissionLocker coordinates distributed mutual exclusion for transfer submissions.
type SubmissionLocker interface {
	AcquireTransferLock(ctx context.Context, transferID string) (*TransferLock, error)
}

// RedlockConfig configures the multi-node Redis Redlock manager.
type RedlockConfig struct {
	// TTL is the lock expiration duration with millisecond granularity (default 15000ms).
	TTL time.Duration
	// ClockDriftFactor is the proportional clock drift allowance across nodes (default 0.01).
	ClockDriftFactor float64
	// BaseClockDrift is the additive clock drift margin (default 2ms).
	BaseClockDrift time.Duration
	// PerNodeTimeout bounds the RPC latency to any single Redis node (default 50ms).
	PerNodeTimeout time.Duration
	// RetryCount is the number of additional attempts after an initial failed lock acquisition (default 0 for fail-fast submission).
	RetryCount int
	// RetryDelay is the base wait duration between retry attempts (default 25ms).
	RetryDelay time.Duration
	// RetryJitter is the maximum random jitter added to RetryDelay (default 25ms).
	RetryJitter time.Duration
	// KeyPrefix is the Redis key prefix for transfer locks (default "settlement:tx:lock:").
	KeyPrefix string
}

// DefaultRedlockConfig returns standard Redlock settings for settlement workers.
func DefaultRedlockConfig() RedlockConfig {
	return RedlockConfig{
		TTL:              DefaultLockTTL,
		ClockDriftFactor: DefaultClockDriftFactor,
		BaseClockDrift:   DefaultBaseClockDrift,
		PerNodeTimeout:   DefaultPerNodeTimeout,
		RetryCount:       0,
		RetryDelay:       DefaultLockRetryDelay,
		RetryJitter:      DefaultLockRetryJitter,
		KeyPrefix:        DefaultLockKeyPrefix,
	}
}

// TransferLock represents an active distributed lock held across a quorum of Redis nodes
// for a single transfer ID.
type TransferLock struct {
	TransferID    string
	Key           string
	Token         string
	TTL           time.Duration
	ValidityTime  time.Duration
	AcquiredNodes int
	Quorum        int
	AcquiredAt    time.Time

	manager  *RedlockManager
	released atomic.Bool
}

// Released reports whether Release has already been invoked on this lock handle.
func (l *TransferLock) Released() bool {
	if l == nil {
		return true
	}
	return l.released.Load()
}

// Release atomically unlocks the transfer across all Redis cluster nodes using the
// compare-and-delete Lua script. Safe to call multiple times.
func (l *TransferLock) Release(ctx context.Context) error {
	if l == nil || l.manager == nil {
		return nil
	}
	if !l.released.CompareAndSwap(false, true) {
		return nil
	}
	return l.manager.releaseKey(ctx, l.Key, l.Token)
}

// Extend refreshes the lock's millisecond TTL across a quorum of Redis nodes if the
// caller still owns the lock token.
func (l *TransferLock) Extend(ctx context.Context, ttl time.Duration) error {
	if l == nil || l.manager == nil {
		return ErrLockNotAcquired
	}
	if l.released.Load() {
		return ErrLockNotAcquired
	}
	if ttl < time.Millisecond {
		return ErrInvalidLockTTL
	}
	return l.manager.extendLock(ctx, l, ttl)
}

// RedlockManager implements the Redis Redlock algorithm across N independent Redis nodes.
type RedlockManager struct {
	nodes []redis.UniversalClient
	cfg   RedlockConfig
}

// NewRedlockManager constructs a RedlockManager over a slice of independent Redis nodes.
func NewRedlockManager(nodes []redis.UniversalClient, cfg RedlockConfig) (*RedlockManager, error) {
	filtered := make([]redis.UniversalClient, 0, len(nodes))
	for _, n := range nodes {
		if n != nil {
			filtered = append(filtered, n)
		}
	}
	if len(filtered) == 0 {
		return nil, ErrInvalidRedlockCluster
	}
	if cfg.TTL <= 0 {
		cfg.TTL = DefaultLockTTL
	}
	if cfg.TTL < time.Millisecond {
		return nil, ErrInvalidLockTTL
	}
	if cfg.ClockDriftFactor <= 0 {
		cfg.ClockDriftFactor = DefaultClockDriftFactor
	}
	if cfg.BaseClockDrift <= 0 {
		cfg.BaseClockDrift = DefaultBaseClockDrift
	}
	if cfg.PerNodeTimeout <= 0 {
		cfg.PerNodeTimeout = DefaultPerNodeTimeout
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = DefaultLockRetryDelay
	}
	if cfg.RetryJitter < 0 {
		cfg.RetryJitter = DefaultLockRetryJitter
	}
	if strings.TrimSpace(cfg.KeyPrefix) == "" {
		cfg.KeyPrefix = DefaultLockKeyPrefix
	}

	return &RedlockManager{
		nodes: filtered,
		cfg:   cfg,
	}, nil
}

// NewRedlockManagerFromClients is a convenience constructor accepting concrete *redis.Client instances.
func NewRedlockManagerFromClients(clients []*redis.Client, cfg RedlockConfig) (*RedlockManager, error) {
	universal := make([]redis.UniversalClient, 0, len(clients))
	for _, c := range clients {
		if c != nil {
			universal = append(universal, c)
		}
	}
	return NewRedlockManager(universal, cfg)
}

// Quorum returns the majority node count (floor(N/2) + 1) required to hold a valid lock.
func (m *RedlockManager) Quorum() int {
	return len(m.nodes)/2 + 1
}

// NodeCount returns the total number of Redis nodes configured in the Redlock cluster.
func (m *RedlockManager) NodeCount() int {
	return len(m.nodes)
}

// LockKey returns the Redis key used to lock a given transfer ID.
func (m *RedlockManager) LockKey(transferID string) string {
	return m.cfg.KeyPrefix + strings.TrimSpace(transferID)
}

// AcquireTransferLock acquires a distributed Redlock for transferID using the manager's configured millisecond TTL.
func (m *RedlockManager) AcquireTransferLock(ctx context.Context, transferID string) (*TransferLock, error) {
	return m.AcquireTransferLockWithTTL(ctx, transferID, m.cfg.TTL)
}

// AcquireTransferLockWithTTL acquires a distributed Redlock for transferID with an explicit millisecond TTL.
func (m *RedlockManager) AcquireTransferLockWithTTL(ctx context.Context, transferID string, ttl time.Duration) (*TransferLock, error) {
	transferID = strings.TrimSpace(transferID)
	if transferID == "" {
		return nil, ErrInvalidTransferID
	}
	// Truncate TTL to millisecond precision and ensure at least 1ms
	ttlMs := ttl.Milliseconds()
	if ttlMs < 1 {
		return nil, ErrInvalidLockTTL
	}
	normalizedTTL := time.Duration(ttlMs) * time.Millisecond

	token, err := generateLockToken()
	if err != nil {
		return nil, fmt.Errorf("generate redlock token: %w", err)
	}

	key := m.LockKey(transferID)
	quorum := m.Quorum()
	attempts := m.cfg.RetryCount + 1
	if attempts < 1 {
		attempts = 1
	}

	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		start := time.Now()
		acquiredNodes := m.lockAllNodes(ctx, key, token, normalizedTTL)
		elapsed := time.Since(start)

		// Compute Redlock clock drift allowance: drift = TTL * ClockDriftFactor + BaseClockDrift
		drift := time.Duration(float64(normalizedTTL)*m.cfg.ClockDriftFactor) + m.cfg.BaseClockDrift
		validityTime := normalizedTTL - elapsed - drift

		if acquiredNodes >= quorum && validityTime > 0 {
			return &TransferLock{
				TransferID:    transferID,
				Key:           key,
				Token:         token,
				TTL:           normalizedTTL,
				ValidityTime:  validityTime,
				AcquiredNodes: acquiredNodes,
				Quorum:        quorum,
				AcquiredAt:    start.UTC(),
				manager:       m,
			}, nil
		}

		// Per Redlock specification: immediately unlock all instances (even those we believe
		// failed) so partial locks do not linger across the cluster.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), m.cfg.PerNodeTimeout*2)
		_ = m.releaseKey(cleanupCtx, key, token)
		cancel()

		if attempt+1 < attempts {
			waitDur := m.retryWaitDuration()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(waitDur):
			}
		}
	}

	return nil, fmt.Errorf("%w for transfer %s", ErrLockNotAcquired, transferID)
}

func (m *RedlockManager) lockAllNodes(ctx context.Context, key, token string, ttl time.Duration) int {
	var acquired atomic.Int32
	var wg sync.WaitGroup
	wg.Add(len(m.nodes))

	for _, node := range m.nodes {
		node := node
		go func() {
			defer wg.Done()
			nodeCtx, cancel := context.WithTimeout(ctx, m.cfg.PerNodeTimeout)
			defer cancel()

			// SET key token NX PX <milliseconds>
			ok, err := node.SetNX(nodeCtx, key, token, ttl).Result()
			if err == nil && ok {
				acquired.Add(1)
			}
		}()
	}

	wg.Wait()
	return int(acquired.Load())
}

func (m *RedlockManager) extendLock(ctx context.Context, lock *TransferLock, ttl time.Duration) error {
	ttlMs := ttl.Milliseconds()
	if ttlMs < 1 {
		return ErrInvalidLockTTL
	}
	normalizedTTL := time.Duration(ttlMs) * time.Millisecond

	start := time.Now()
	var extended atomic.Int32
	var wg sync.WaitGroup
	wg.Add(len(m.nodes))

	for _, node := range m.nodes {
		node := node
		go func() {
			defer wg.Done()
			nodeCtx, cancel := context.WithTimeout(ctx, m.cfg.PerNodeTimeout)
			defer cancel()

			res, err := redlockExtendScript.Run(nodeCtx, node, []string{lock.Key}, lock.Token, ttlMs).Int64()
			if err == nil && res == 1 {
				extended.Add(1)
			}
		}()
	}
	wg.Wait()

	elapsed := time.Since(start)
	drift := time.Duration(float64(normalizedTTL)*m.cfg.ClockDriftFactor) + m.cfg.BaseClockDrift
	validityTime := normalizedTTL - elapsed - drift

	quorum := m.Quorum()
	count := int(extended.Load())
	if count >= quorum && validityTime > 0 {
		lock.TTL = normalizedTTL
		lock.ValidityTime = validityTime
		lock.AcquiredNodes = count
		return nil
	}

	return fmt.Errorf("%w: extend failed for transfer %s (extended %d/%d nodes)", ErrLockNotAcquired, lock.TransferID, count, quorum)
}

func (m *RedlockManager) releaseKey(ctx context.Context, key, token string) error {
	var wg sync.WaitGroup
	wg.Add(len(m.nodes))

	var releaseErrors atomic.Int32
	for _, node := range m.nodes {
		node := node
		go func() {
			defer wg.Done()
			nodeCtx, cancel := context.WithTimeout(ctx, m.cfg.PerNodeTimeout)
			defer cancel()

			if err := redlockReleaseScript.Run(nodeCtx, node, []string{key}, token).Err(); err != nil && !errors.Is(err, redis.Nil) {
				releaseErrors.Add(1)
			}
		}()
	}

	wg.Wait()

	// If more than (N - Quorum) nodes failed to process the release RPC, report an error
	// so callers can log it while still allowing TTL expiration to clean up failed nodes.
	maxFailures := len(m.nodes) - m.Quorum()
	if int(releaseErrors.Load()) > maxFailures {
		err := fmt.Errorf("redlock release failed on %d/%d redis nodes for key %s", releaseErrors.Load(), len(m.nodes), key)
		log.Warn().Err(err).Str("key", key).Msg("settlement redlock: majority release failed, relying on millisecond TTL")
		return err
	}
	return nil
}

func (m *RedlockManager) retryWaitDuration() time.Duration {
	delay := m.cfg.RetryDelay
	if m.cfg.RetryJitter > 0 {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(m.cfg.RetryJitter)+1))
		if err == nil {
			delay += time.Duration(n.Int64())
		}
	}
	return delay
}

// generateLockToken generates a 20-byte (160-bit) cryptographically random hex string
// as specified by the Redis Redlock algorithm.
func generateLockToken() (string, error) {
	var buf [20]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}
