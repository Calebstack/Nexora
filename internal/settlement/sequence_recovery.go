package settlement

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Lumen-Nexora/Nexora/internal/stellar"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/txnbuild"
)

var (
	// ErrSequenceRecoveryExhausted means every resynchronised attempt was
	// still rejected with tx_bad_seq.
	ErrSequenceRecoveryExhausted = errors.New("sequence recovery exhausted")
	// ErrUnboundedEnvelope rejects envelopes without a max time. A superseded
	// envelope whose sequence was too high could otherwise become valid later
	// and pay twice.
	ErrUnboundedEnvelope = errors.New("envelope must set a finite max time")
)

// SequenceStore allocates sequence numbers per signing account. The stored
// value is the last sequence number handed out.
type SequenceStore interface {
	// Next reserves and returns the next sequence number; ok is false when
	// the account has not been initialised.
	Next(ctx context.Context, account string) (seq int64, ok bool, err error)
	// Init stores seq only if the account has no counter yet.
	Init(ctx context.Context, account string, seq int64) error
	// Reset overwrites the counter with an authoritative on-chain value.
	Reset(ctx context.Context, account string, seq int64) error
}

// MemorySequenceStore is a process-local SequenceStore.
type MemorySequenceStore struct {
	mu   sync.Mutex
	last map[string]int64
}

func NewMemorySequenceStore() *MemorySequenceStore {
	return &MemorySequenceStore{last: make(map[string]int64)}
}

func (s *MemorySequenceStore) Next(_ context.Context, account string) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq, ok := s.last[account]
	if !ok {
		return 0, false, nil
	}
	seq++
	s.last[account] = seq
	return seq, true, nil
}

func (s *MemorySequenceStore) Init(_ context.Context, account string, seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.last[account]; !ok {
		s.last[account] = seq
	}
	return nil
}

func (s *MemorySequenceStore) Reset(_ context.Context, account string, seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last[account] = seq
	return nil
}

// incrIfExists increments the counter atomically, or returns nil when it is
// missing so the caller initialises it from Horizon first.
var incrIfExists = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
  return redis.call('INCR', KEYS[1])
end
return false`)

// RedisSequenceStore shares counters across processes; INCR makes each
// allocation atomic.
type RedisSequenceStore struct {
	rdb    redis.UniversalClient
	prefix string
}

func NewRedisSequenceStore(rdb redis.UniversalClient, prefix string) *RedisSequenceStore {
	if prefix == "" {
		prefix = "nexora:stellar:seq:"
	}
	return &RedisSequenceStore{rdb: rdb, prefix: prefix}
}

func (s *RedisSequenceStore) Next(ctx context.Context, account string) (int64, bool, error) {
	seq, err := incrIfExists.Run(ctx, s.rdb, []string{s.prefix + account}).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("allocate sequence for %s: %w", account, err)
	}
	return seq, true, nil
}

func (s *RedisSequenceStore) Init(ctx context.Context, account string, seq int64) error {
	if err := s.rdb.SetNX(ctx, s.prefix+account, seq, 0).Err(); err != nil {
		return fmt.Errorf("init sequence for %s: %w", account, err)
	}
	return nil
}

func (s *RedisSequenceStore) Reset(ctx context.Context, account string, seq int64) error {
	if err := s.rdb.Set(ctx, s.prefix+account, seq, 0).Err(); err != nil {
		return fmt.Errorf("reset sequence for %s: %w", account, err)
	}
	return nil
}

// EnvelopeBuilder builds and signs the transaction with exactly sequence
// number seq (for example SimpleAccount{Sequence: seq - 1} with
// IncrementSequenceNum) and a finite max time. It is called again with a new
// sequence number after every resynchronisation.
type EnvelopeBuilder func(seq int64) (*txnbuild.Transaction, error)

// SequenceMetrics counts recovery activity.
type SequenceMetrics struct {
	Submitted      int64
	BadSequence    int64
	Resyncs        int64
	AlreadyOnChain int64
}

// SequenceManager allocates sequence numbers and recovers from tx_bad_seq by
// resynchronising from Horizon and re-signing the payment with a fresh
// sequence number. Stellar consumes at most one transaction per account per
// ledger, so throughput scales with the number of signing (channel) accounts.
type SequenceManager struct {
	store      SequenceStore
	horizon    stellar.Client
	passphrase string
	maxResyncs int
	// accountLocks serialises submissions per signing account in this
	// process so envelopes reach Horizon in sequence order.
	accountLocks sync.Map // account -> *sync.Mutex

	submitted, badSeq, resyncs, onChain atomic.Int64
}

// NewSequenceManager builds a manager. passphrase is the network passphrase
// used to hash envelopes; maxResyncs (default 5) bounds recovery per payment.
func NewSequenceManager(store SequenceStore, h stellar.Client, passphrase string, maxResyncs int) *SequenceManager {
	if maxResyncs <= 0 {
		maxResyncs = 5
	}
	return &SequenceManager{store: store, horizon: h, passphrase: passphrase, maxResyncs: maxResyncs}
}

// SubmitError carries the hash of the envelope whose submission failed, so
// an ambiguous outcome can later be resolved by hash lookup.
type SubmitError struct {
	Hash string
	Err  error
}

func (e *SubmitError) Error() string { return fmt.Sprintf("submit %s: %v", e.Hash, e.Err) }
func (e *SubmitError) Unwrap() error { return e.Err }

// Submit signs and submits one payment from account. priorHashes are
// envelopes of the same payment submitted earlier with an unknown outcome;
// if any of them is on chain its result is returned without submitting.
//
// On tx_bad_seq it first checks every known envelope of this payment on
// Horizon: tx_bad_seq is also returned when the sequence was consumed by an
// earlier envelope that did land, and that envelope's result is returned
// instead of paying twice. Only when none landed does it reload the
// account's on-chain sequence, reset the counter and re-sign. Any other
// error is returned as a *SubmitError without retry, because only a
// definite rejection proves the envelope was not applied.
func (m *SequenceManager) Submit(ctx context.Context, account string, priorHashes []string, build EnvelopeBuilder) (horizon.Transaction, error) {
	hashes := append([]string(nil), priorHashes...)
	if landed, found, err := m.findOnChain(ctx, hashes); err != nil || found {
		return landed, err
	}

	lock, _ := m.accountLocks.LoadOrStore(account, &sync.Mutex{})
	lock.(*sync.Mutex).Lock()
	defer lock.(*sync.Mutex).Unlock()

	for resync := 0; ; resync++ {
		seq, err := m.next(ctx, account)
		if err != nil {
			return horizon.Transaction{}, err
		}
		tx, err := build(seq)
		if err != nil {
			return horizon.Transaction{}, fmt.Errorf("build envelope: %w", err)
		}
		if err := m.checkEnvelope(tx, account, seq); err != nil {
			return horizon.Transaction{}, err
		}
		hash, err := tx.HashHex(m.passphrase)
		if err != nil {
			return horizon.Transaction{}, fmt.Errorf("hash envelope: %w", err)
		}

		m.submitted.Add(1)
		resp, err := stellar.SubmitTransactionWithContext(ctx, m.horizon, tx)
		if err == nil {
			return resp, nil
		}
		if !stellar.IsBadSequence(err) {
			return horizon.Transaction{}, &SubmitError{Hash: hash, Err: err}
		}
		m.badSeq.Add(1)
		hashes = append(hashes, hash)

		if landed, found, err := m.findOnChain(ctx, hashes); err != nil || found {
			return landed, err
		}
		if resync == m.maxResyncs {
			return horizon.Transaction{}, fmt.Errorf("%w: account %s after %d resyncs", ErrSequenceRecoveryExhausted, account, resync)
		}
		if err := m.resync(ctx, account); err != nil {
			return horizon.Transaction{}, err
		}
	}
}

// next allocates a sequence number, initialising the counter from Horizon
// on first use. Init only writes when no counter exists, so concurrent
// first callers never rewind each other.
func (m *SequenceManager) next(ctx context.Context, account string) (int64, error) {
	seq, ok, err := m.store.Next(ctx, account)
	if err != nil || ok {
		return seq, err
	}
	onChain, err := m.onChainSequence(ctx, account)
	if err != nil {
		return 0, err
	}
	if err := m.store.Init(ctx, account, onChain); err != nil {
		return 0, err
	}
	seq, ok, err = m.store.Next(ctx, account)
	if err == nil && !ok {
		err = fmt.Errorf("sequence counter for %s vanished after init", account)
	}
	return seq, err
}

func (m *SequenceManager) resync(ctx context.Context, account string) error {
	onChain, err := m.onChainSequence(ctx, account)
	if err != nil {
		return err
	}
	if err := m.store.Reset(ctx, account, onChain); err != nil {
		return err
	}
	m.resyncs.Add(1)
	log.Warn().Str("account", account).Int64("on_chain_sequence", onChain).
		Msg("settlement: tx_bad_seq, resynchronised sequence from Horizon")
	return nil
}

func (m *SequenceManager) onChainSequence(ctx context.Context, account string) (int64, error) {
	acct, err := stellar.LoadAccountWithContext(ctx, m.horizon, account)
	if err != nil {
		return 0, fmt.Errorf("load sequence for %s: %w", account, err)
	}
	return acct.Sequence, nil
}

// findOnChain reports the first of hashes Horizon knows. A lookup error other
// than 404 is returned, since a payment that cannot be ruled out must not be
// re-signed. O(h) Horizon calls for h envelopes of this payment.
func (m *SequenceManager) findOnChain(ctx context.Context, hashes []string) (horizon.Transaction, bool, error) {
	for _, h := range hashes {
		tx, err := stellar.TransactionDetailWithContext(ctx, m.horizon, h)
		if err == nil {
			m.onChain.Add(1)
			return tx, true, nil
		}
		if !stellar.IsNotFound(err) {
			return horizon.Transaction{}, false, fmt.Errorf("check envelope %s on chain: %w", h, err)
		}
	}
	return horizon.Transaction{}, false, nil
}

func (m *SequenceManager) checkEnvelope(tx *txnbuild.Transaction, account string, seq int64) error {
	src := tx.SourceAccount()
	if !strings.EqualFold(src.AccountID, account) {
		return fmt.Errorf("envelope source %s does not match signing account %s", src.AccountID, account)
	}
	if src.Sequence != seq {
		return fmt.Errorf("envelope sequence %d does not match allocated %d", src.Sequence, seq)
	}
	if tx.Timebounds().MaxTime == txnbuild.TimeoutInfinite {
		return ErrUnboundedEnvelope
	}
	return nil
}

// Metrics returns a snapshot of the manager's counters.
func (m *SequenceManager) Metrics() SequenceMetrics {
	return SequenceMetrics{
		Submitted:      m.submitted.Load(),
		BadSequence:    m.badSeq.Load(),
		Resyncs:        m.resyncs.Load(),
		AlreadyOnChain: m.onChain.Load(),
	}
}
