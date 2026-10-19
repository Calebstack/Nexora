package settlement

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/alerting"
	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/stellar"
	"github.com/Lumen-Nexora/Nexora/internal/tenant"
	"github.com/Lumen-Nexora/Nexora/internal/transfer"
	"github.com/Lumen-Nexora/Nexora/internal/wallet"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"github.com/stellar/go/protocols/horizon"
)

// FallbackPollerConfig configures the asynchronous Horizon fallback poller.
type FallbackPollerConfig struct {
	StuckThreshold      time.Duration
	PollInterval        time.Duration
	MaxTTL              time.Duration
	InitialBackoff      time.Duration
	MaxBackoff          time.Duration
	Timeout504Threshold int
	TimeoutWindow       time.Duration
	BatchSize           int
}

// DefaultFallbackPollerConfig returns production defaults for transaction fallback polling.
func DefaultFallbackPollerConfig() FallbackPollerConfig {
	return FallbackPollerConfig{
		StuckThreshold:      60 * time.Second,
		PollInterval:        5 * time.Second,
		MaxTTL:              300 * time.Second,
		InitialBackoff:      1 * time.Second,
		MaxBackoff:          30 * time.Second,
		Timeout504Threshold: 3,
		TimeoutWindow:       5 * time.Minute,
		BatchSize:           50,
	}
}

// StuckTransactionProvider fetches stuck pending or submitted transactions.
type StuckTransactionProvider interface {
	GetStuckPendingTransactions(ctx context.Context, olderThan time.Duration, limit int) ([]*domain.Transaction, error)
}

// ReconcilePendingProvider matches postgres.TransactionRepo.GetPendingTxesForReconciliation.
type ReconcilePendingProvider interface {
	GetPendingTxesForReconciliation(ctx context.Context, olderThan time.Duration, limit int) ([]*domain.Transaction, error)
}

// WalletBalanceReleaser provides an explicit interface for releasing reserved wallet balances.
type WalletBalanceReleaser interface {
	ReleaseReservedBalance(ctx context.Context, walletID string, asset string, amount decimal.Decimal) error
}

// SettlementDetailsRecorder provides an interface for persisting ledger sequence and fee charged.
type SettlementDetailsRecorder interface {
	RecordSettlementDetails(ctx context.Context, txID, txHash string, ledgerSequence int32, feeCharged int64) error
}

// ConfirmedRecord stores on-chain confirmation details recorded by the fallback poller.
type ConfirmedRecord struct {
	TransactionID  string    `json:"transaction_id"`
	TxHash         string    `json:"tx_hash"`
	LedgerSequence int32     `json:"ledger_sequence"`
	FeeCharged     int64     `json:"fee_charged"`
	ConfirmedAt    time.Time `json:"confirmed_at"`
}

// PollerMetrics holds atomic telemetry counters for transaction submissions and fallback polling.
type PollerMetrics struct {
	SubmissionSuccessCount uint64 `json:"submission_success_count"`
	SubmissionFailureCount uint64 `json:"submission_failure_count"`
	PollingRecoveryCount   uint64 `json:"polling_recovery_count"`
	PollingDropCount       uint64 `json:"polling_drop_count"`
	Timeout504Count        uint64 `json:"timeout_504_count"`
	ActiveStuckCount       uint64 `json:"active_stuck_count"`
}

type txPollState struct {
	attempts    int
	lastPolled  time.Time
	nextAllowed time.Time
}

type timeoutTracker struct {
	mu         sync.Mutex
	timestamps []time.Time
}

func (t *timeoutTracker) record(now time.Time, window time.Duration) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	cutoff := now.Add(-window)
	filtered := make([]time.Time, 0, len(t.timestamps)+1)
	for _, ts := range t.timestamps {
		if ts.After(cutoff) {
			filtered = append(filtered, ts)
		}
	}
	filtered = append(filtered, now)
	t.timestamps = filtered
	return len(t.timestamps)
}

// FallbackPoller asynchronously polls Horizon for transactions stuck in submitted/pending status.
type FallbackPoller struct {
	txRepo          transfer.Repository
	walletRepo      wallet.Repository
	stellar         stellar.Client
	alerting        *alerting.Client
	config          FallbackPollerConfig
	walletReleaser  WalletBalanceReleaser
	detailsRecorder SettlementDetailsRecorder

	// Internal state
	pollStatesMu sync.RWMutex
	pollStates   map[string]*txPollState

	confirmedMu      sync.RWMutex
	confirmedRecords map[string]*ConfirmedRecord

	timeouts504 timeoutTracker

	// Telemetry counters
	subSuccessCount atomic.Uint64
	subFailureCount atomic.Uint64
	pollRecovCount  atomic.Uint64
	pollDropCount   atomic.Uint64
	timeout504Count atomic.Uint64

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup

	rngMu sync.Mutex
	rng   *rand.Rand
}

// NewFallbackPoller creates a new asynchronous Horizon transaction polling fallback worker.
func NewFallbackPoller(
	txRepo transfer.Repository,
	walletRepo wallet.Repository,
	stellarClient stellar.Client,
	alertClient *alerting.Client,
	config FallbackPollerConfig,
) *FallbackPoller {
	if config.StuckThreshold <= 0 {
		config.StuckThreshold = 60 * time.Second
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 5 * time.Second
	}
	if config.MaxTTL <= 0 {
		config.MaxTTL = 300 * time.Second
	}
	if config.InitialBackoff <= 0 {
		config.InitialBackoff = 1 * time.Second
	}
	if config.MaxBackoff <= 0 {
		config.MaxBackoff = 30 * time.Second
	}
	if config.Timeout504Threshold <= 0 {
		config.Timeout504Threshold = 3
	}
	if config.TimeoutWindow <= 0 {
		config.TimeoutWindow = 5 * time.Minute
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 50
	}

	return &FallbackPoller{
		txRepo:           txRepo,
		walletRepo:       walletRepo,
		stellar:          stellarClient,
		alerting:         alertClient,
		config:           config,
		pollStates:       make(map[string]*txPollState),
		confirmedRecords: make(map[string]*ConfirmedRecord),
		stopCh:           make(chan struct{}),
		rng:              rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// WithWalletReleaser attaches a custom wallet balance releaser.
func (p *FallbackPoller) WithWalletReleaser(releaser WalletBalanceReleaser) *FallbackPoller {
	p.walletReleaser = releaser
	return p
}

// WithDetailsRecorder attaches a settlement details recorder.
func (p *FallbackPoller) WithDetailsRecorder(recorder SettlementDetailsRecorder) *FallbackPoller {
	p.detailsRecorder = recorder
	return p
}

// RecordSubmissionSuccess increments telemetry for successful direct submissions.
func (p *FallbackPoller) RecordSubmissionSuccess() {
	p.subSuccessCount.Add(1)
}

// RecordSubmissionFailure tracks submission failure and analyzes 504 timeouts.
func (p *FallbackPoller) RecordSubmissionFailure(err error) {
	p.subFailureCount.Add(1)
	if err == nil {
		return
	}
	if is504Timeout(err) {
		p.handle504Timeout(context.Background(), err)
	}
}

func is504Timeout(err error) bool {
	if err == nil {
		return false
	}
	if status, ok := stellar.HTTPStatus(err); ok && status == http.StatusGatewayTimeout {
		return true
	}
	return errors.Is(err, context.DeadlineExceeded)
}

func (p *FallbackPoller) handle504Timeout(ctx context.Context, err error) {
	p.timeout504Count.Add(1)
	count := p.timeouts504.record(time.Now().UTC(), p.config.TimeoutWindow)

	log.Warn().
		Err(err).
		Int("window_504_count", count).
		Int("threshold", p.config.Timeout504Threshold).
		Msg("settlement poller: detected 504 Gateway Timeout from Horizon")

	if count >= p.config.Timeout504Threshold && p.alerting != nil {
		p.alerting.Warning(ctx,
			"Elevated Horizon 504 Timeouts",
			fmt.Sprintf("Detected %d Horizon 504 Gateway Timeouts within %s window (threshold: %d). Polling fallback active.",
				count, p.config.TimeoutWindow.Round(time.Second), p.config.Timeout504Threshold),
		)
	}
}

// GetMetrics returns a point-in-time snapshot of poller and submission telemetry.
func (p *FallbackPoller) GetMetrics() PollerMetrics {
	p.pollStatesMu.RLock()
	activeStuck := uint64(len(p.pollStates))
	p.pollStatesMu.RUnlock()

	return PollerMetrics{
		SubmissionSuccessCount: p.subSuccessCount.Load(),
		SubmissionFailureCount: p.subFailureCount.Load(),
		PollingRecoveryCount:   p.pollRecovCount.Load(),
		PollingDropCount:       p.pollDropCount.Load(),
		Timeout504Count:        p.timeout504Count.Load(),
		ActiveStuckCount:       activeStuck,
	}
}

// GetConfirmedRecord returns confirmed on-chain details if recovered by the poller.
func (p *FallbackPoller) GetConfirmedRecord(txID string) (*ConfirmedRecord, bool) {
	p.confirmedMu.RLock()
	defer p.confirmedMu.RUnlock()
	rec, ok := p.confirmedRecords[txID]
	return rec, ok
}

// Start spawns the background polling worker routine.
func (p *FallbackPoller) Start(ctx context.Context) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		ticker := time.NewTicker(p.config.PollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-p.stopCh:
				return
			case <-ticker.C:
				if err := p.PollOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
					log.Error().Err(err).Msg("settlement poller: error during polling sweep")
				}
			}
		}
	}()
}

// Stop gracefully stops the background poller worker.
func (p *FallbackPoller) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
		p.wg.Wait()
	})
}

// PollOnce executes a single polling pass over stuck transactions.
func (p *FallbackPoller) PollOnce(ctx context.Context) error {
	stuckTxs, err := p.fetchStuckTransactions(ctx)
	if err != nil {
		return fmt.Errorf("fetch stuck transactions: %w", err)
	}

	now := time.Now().UTC()
	for _, tx := range stuckTxs {
		if tx == nil {
			continue
		}
		if tx.TenantID != nil {
			ctx = tenant.WithID(ctx, *tx.TenantID)
		}
		ctx = tenant.WithMode(ctx, tx.Mode)

		if err := p.processStuckTransaction(ctx, tx, now); err != nil {
			log.Error().Err(err).Str("tx_id", tx.ID).Str("tx_hash", tx.TxHash).
				Msg("settlement poller: failed processing stuck transaction")
		}
	}

	return nil
}

func (p *FallbackPoller) fetchStuckTransactions(ctx context.Context) ([]*domain.Transaction, error) {
	// 1. Try StuckTransactionProvider if repo implements it
	if provider, ok := p.txRepo.(StuckTransactionProvider); ok {
		return provider.GetStuckPendingTransactions(ctx, p.config.StuckThreshold, p.config.BatchSize)
	}

	// 2. Try ReconcilePendingProvider (e.g. postgres.TransactionRepo)
	if provider, ok := p.txRepo.(ReconcilePendingProvider); ok {
		return provider.GetPendingTxesForReconciliation(ctx, p.config.StuckThreshold, p.config.BatchSize)
	}

	return nil, nil
}

func (p *FallbackPoller) processStuckTransaction(ctx context.Context, tx *domain.Transaction, now time.Time) error {
	// A transaction without a hash cannot be queried on Horizon
	if tx.TxHash == "" {
		if now.Sub(tx.CreatedAt) > p.config.MaxTTL {
			return p.handleDroppedTransaction(ctx, tx, "no transaction hash generated within max TTL")
		}
		return nil
	}

	// Check backoff and rate-limiting
	p.pollStatesMu.Lock()
	state, exists := p.pollStates[tx.ID]
	if !exists {
		state = &txPollState{
			attempts:    0,
			lastPolled:  time.Time{},
			nextAllowed: time.Time{},
		}
		p.pollStates[tx.ID] = state
	}

	if now.Before(state.nextAllowed) {
		p.pollStatesMu.Unlock()
		return nil
	}

	state.attempts++
	state.lastPolled = now
	state.nextAllowed = now.Add(p.calculateBackoffWithJitter(state.attempts))
	p.pollStatesMu.Unlock()

	// Query Horizon for transaction details
	horizonTx, err := p.stellar.TransactionDetail(tx.TxHash)
	if err != nil {
		if is504Timeout(err) {
			p.handle504Timeout(ctx, err)
		}

		if stellar.IsNotFound(err) {
			// Transaction not found on Horizon
			if now.Sub(tx.CreatedAt) > p.config.MaxTTL {
				return p.handleDroppedTransaction(ctx, tx, "transaction dropped from mempool after max TTL")
			}
			// Still within TTL, wait for next backoff cycle
			return nil
		}

		return fmt.Errorf("horizon transaction detail for %s: %w", tx.TxHash, err)
	}

	// Transaction found on Horizon!
	if horizonTx.Successful {
		return p.handleConfirmedTransaction(ctx, tx, horizonTx)
	}

	// Transaction recorded on chain as failed
	return p.handleDroppedTransaction(ctx, tx, "transaction failed on-chain")
}

func (p *FallbackPoller) handleConfirmedTransaction(
	ctx context.Context,
	tx *domain.Transaction,
	horizonTx horizon.Transaction,
) error {
	// 1. Update status to Confirmed
	if err := p.txRepo.UpdateStatus(ctx, tx.ID, domain.StatusConfirmed, tx.TxHash); err != nil {
		if !errors.Is(err, domain.ErrConcurrentUpdate) {
			return fmt.Errorf("update status confirmed: %w", err)
		}
	}

	// 2. Record ledger sequence and fee charged
	record := &ConfirmedRecord{
		TransactionID:  tx.ID,
		TxHash:         tx.TxHash,
		LedgerSequence: horizonTx.Ledger,
		FeeCharged:     horizonTx.FeeCharged,
		ConfirmedAt:    time.Now().UTC(),
	}

	p.confirmedMu.Lock()
	p.confirmedRecords[tx.ID] = record
	p.confirmedMu.Unlock()

	if p.detailsRecorder != nil {
		if err := p.detailsRecorder.RecordSettlementDetails(ctx, tx.ID, tx.TxHash, horizonTx.Ledger, horizonTx.FeeCharged); err != nil {
			log.Warn().Err(err).Str("tx_id", tx.ID).Msg("settlement poller: failed recording settlement details")
		}
	}

	// 3. Sync wallet balances
	p.syncWalletBalances(ctx, tx.FromWallet)
	p.syncWalletBalances(ctx, tx.ToWallet)

	// 4. Update metrics & clean tracking
	p.pollRecovCount.Add(1)
	p.pollStatesMu.Lock()
	delete(p.pollStates, tx.ID)
	p.pollStatesMu.Unlock()

	log.Info().
		Str("tx_id", tx.ID).
		Str("tx_hash", tx.TxHash).
		Int32("ledger", horizonTx.Ledger).
		Int64("fee_charged", horizonTx.FeeCharged).
		Msg("settlement poller: confirmed stuck transaction via Horizon fallback")

	return nil
}

func (p *FallbackPoller) handleDroppedTransaction(ctx context.Context, tx *domain.Transaction, reason string) error {
	// 1. Mark transaction failed
	if err := p.txRepo.UpdateStatus(ctx, tx.ID, domain.StatusFailed, tx.TxHash); err != nil {
		if !errors.Is(err, domain.ErrConcurrentUpdate) {
			log.Error().Err(err).Str("tx_id", tx.ID).Msg("settlement poller: update failed status")
		}
	}

	// 2. Release reserved wallet balance
	if p.walletReleaser != nil {
		if err := p.walletReleaser.ReleaseReservedBalance(ctx, tx.FromWallet, tx.Asset, tx.Amount); err != nil {
			log.Error().Err(err).Str("wallet_id", tx.FromWallet).Msg("settlement poller: failed to release reserved balance")
		}
	}
	// Sync current on-chain balance to source wallet to ensure consistency
	p.syncWalletBalances(ctx, tx.FromWallet)

	// 3. Telemetry and state cleanup
	p.pollDropCount.Add(1)
	p.pollStatesMu.Lock()
	delete(p.pollStates, tx.ID)
	p.pollStatesMu.Unlock()

	log.Warn().
		Str("tx_id", tx.ID).
		Str("tx_hash", tx.TxHash).
		Str("reason", reason).
		Msg("settlement poller: transaction dropped/failed after max TTL, released reserved balance")

	return nil
}

func (p *FallbackPoller) syncWalletBalances(ctx context.Context, walletID string) {
	if walletID == "" || p.walletRepo == nil || p.stellar == nil {
		return
	}

	w, err := p.walletRepo.GetByID(ctx, walletID)
	if err != nil || w == nil {
		return
	}

	acct, err := stellar.LoadAccountWithContext(ctx, p.stellar, w.PublicKey)
	if err != nil {
		return
	}

	for _, b := range acct.Balances {
		code := b.Code
		if code == "" {
			code = "XLM"
		}
		amt, err := decimal.NewFromString(b.Balance)
		if err == nil {
			_ = p.walletRepo.UpsertBalance(ctx, w.ID, code, b.Issuer, amt)
		}
	}
}

func (p *FallbackPoller) calculateBackoffWithJitter(attempt int) time.Duration {
	if attempt <= 0 {
		return p.config.InitialBackoff
	}

	factor := math.Pow(2, float64(attempt-1))
	backoff := float64(p.config.InitialBackoff) * factor
	if backoff > float64(p.config.MaxBackoff) {
		backoff = float64(p.config.MaxBackoff)
	}

	p.rngMu.Lock()
	r := p.rng.Float64()
	p.rngMu.Unlock()

	// Jitter: uniformly randomized in [0.5 * backoff, 1.0 * backoff]
	jitter := (backoff * 0.5) + (backoff * 0.5 * r)
	return time.Duration(jitter)
}
