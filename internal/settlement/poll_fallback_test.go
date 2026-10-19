package settlement

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/alerting"
	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/queue"
	"github.com/hibiken/asynq"
	"github.com/shopspring/decimal"
	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/support/render/problem"
)

type pollerTestRepo struct {
	*fakeTxRepo
	stuckTxs []*domain.Transaction
}

func newPollerTestRepo(stuckTxs ...*domain.Transaction) *pollerTestRepo {
	baseMap := make(map[string]*domain.Transaction)
	for _, tx := range stuckTxs {
		cp := *tx
		baseMap[tx.ID] = &cp
	}
	return &pollerTestRepo{
		fakeTxRepo: &fakeTxRepo{rows: baseMap},
		stuckTxs:   stuckTxs,
	}
}

func (r *pollerTestRepo) GetStuckPendingTransactions(_ context.Context, _ time.Duration, _ int) ([]*domain.Transaction, error) {
	return r.stuckTxs, nil
}

func newFakeWalletRepo(wallets ...*domain.Wallet) *fakeWalletRepo {
	m := make(map[string]*domain.Wallet)
	for _, w := range wallets {
		m[w.ID] = w
	}
	return &fakeWalletRepo{wallets: m}
}

type testBalanceReleaser struct {
	mu           sync.Mutex
	released     bool
	walletID     string
	asset        string
	amount       decimal.Decimal
	releaseCalls int
}

func (r *testBalanceReleaser) ReleaseReservedBalance(_ context.Context, walletID string, asset string, amount decimal.Decimal) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.released = true
	r.walletID = walletID
	r.asset = asset
	r.amount = amount
	r.releaseCalls++
	return nil
}

type testDetailsRecorder struct {
	mu             sync.Mutex
	txID           string
	txHash         string
	ledgerSequence int32
	feeCharged     int64
	recorded       bool
}

func (r *testDetailsRecorder) RecordSettlementDetails(_ context.Context, txID, txHash string, ledgerSequence int32, feeCharged int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.txID = txID
	r.txHash = txHash
	r.ledgerSequence = ledgerSequence
	r.feeCharged = feeCharged
	r.recorded = true
	return nil
}

func TestFallbackPoller_ConfirmsStuckTransaction(t *testing.T) {
	tx := &domain.Transaction{
		ID:         "tx-stuck-confirm",
		Status:     domain.StatusSubmitted,
		TxHash:     "hash-confirm-123",
		FromWallet: "w-src",
		ToWallet:   "w-dst",
		Asset:      "USDC",
		Amount:     decimal.NewFromInt(50),
		CreatedAt:  time.Now().UTC().Add(-90 * time.Second),
	}

	repo := newPollerTestRepo(tx)
	wallets := newFakeWalletRepo(
		&domain.Wallet{ID: "w-src", PublicKey: "G_SRC"},
		&domain.Wallet{ID: "w-dst", PublicKey: "G_DST"},
	)

	stellarClient := &fakeStellarClient{
		txDetailFunc: func(hash string) (horizon.Transaction, error) {
			if hash != "hash-confirm-123" {
				return horizon.Transaction{}, errors.New("not found")
			}
			return horizon.Transaction{
				Hash:       hash,
				Successful: true,
				Ledger:     456789,
				FeeCharged: 100,
			}, nil
		},
	}

	config := DefaultFallbackPollerConfig()
	config.StuckThreshold = 60 * time.Second
	poller := NewFallbackPoller(repo, wallets, stellarClient, nil, config)

	detailsRec := &testDetailsRecorder{}
	poller.WithDetailsRecorder(detailsRec)

	// Execute single polling sweep
	err := poller.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected PollOnce error: %v", err)
	}

	// 1. Transaction status must be StatusConfirmed
	gotStatus := repo.status("tx-stuck-confirm")
	if gotStatus != domain.StatusConfirmed {
		t.Fatalf("expected status %s, got %s", domain.StatusConfirmed, gotStatus)
	}

	// 2. Ledger sequence and fee charged recorded in poller
	rec, ok := poller.GetConfirmedRecord("tx-stuck-confirm")
	if !ok {
		t.Fatalf("expected confirmed record stored in poller")
	}
	if rec.LedgerSequence != 456789 {
		t.Fatalf("expected ledger sequence 456789, got %d", rec.LedgerSequence)
	}
	if rec.FeeCharged != 100 {
		t.Fatalf("expected fee charged 100, got %d", rec.FeeCharged)
	}

	// 3. Details recorder was called
	detailsRec.mu.Lock()
	if !detailsRec.recorded || detailsRec.ledgerSequence != 456789 || detailsRec.feeCharged != 100 {
		t.Fatalf("expected details recorded properly, got %+v", detailsRec)
	}
	detailsRec.mu.Unlock()

	// 4. Balances were synced via UpsertBalance
	wallets.mu.Lock()
	if wallets.upsertBalCalls < 1 {
		t.Fatalf("expected upsert balance calls for confirmed tx, got %d", wallets.upsertBalCalls)
	}
	wallets.mu.Unlock()

	// 5. Telemetry
	metrics := poller.GetMetrics()
	if metrics.PollingRecoveryCount != 1 {
		t.Fatalf("expected PollingRecoveryCount=1, got %d", metrics.PollingRecoveryCount)
	}
}

func TestFallbackPoller_DropsTransactionAfterMaxTTL(t *testing.T) {
	tx := &domain.Transaction{
		ID:         "tx-stuck-drop",
		Status:     domain.StatusSubmitted,
		TxHash:     "hash-drop-456",
		FromWallet: "w-src-drop",
		ToWallet:   "w-dst-drop",
		Asset:      "USDC",
		Amount:     decimal.NewFromInt(100),
		CreatedAt:  time.Now().UTC().Add(-400 * time.Second), // Older than 300s MaxTTL
	}

	repo := newPollerTestRepo(tx)
	wallets := newFakeWalletRepo(
		&domain.Wallet{ID: "w-src-drop", PublicKey: "G_SRC_DROP"},
	)

	stellarClient := &fakeStellarClient{
		txDetailFunc: func(hash string) (horizon.Transaction, error) {
			// Simulate 404 Not Found from Horizon
			return horizon.Transaction{}, &horizonclient.Error{
				Problem: problem.P{
					Status: http.StatusNotFound,
					Title:  "Resource Missing",
				},
			}
		},
	}

	config := DefaultFallbackPollerConfig()
	config.MaxTTL = 300 * time.Second
	poller := NewFallbackPoller(repo, wallets, stellarClient, nil, config)

	releaser := &testBalanceReleaser{}
	poller.WithWalletReleaser(releaser)

	err := poller.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected PollOnce error: %v", err)
	}

	// 1. Transaction marked StatusFailed
	gotStatus := repo.status("tx-stuck-drop")
	if gotStatus != domain.StatusFailed {
		t.Fatalf("expected status %s, got %s", domain.StatusFailed, gotStatus)
	}

	// 2. Reserved wallet balance was released
	releaser.mu.Lock()
	if !releaser.released {
		t.Fatalf("expected reserved balance to be released")
	}
	if releaser.walletID != "w-src-drop" || !releaser.amount.Equal(decimal.NewFromInt(100)) {
		t.Fatalf("unexpected release call params: wallet=%s, amount=%s", releaser.walletID, releaser.amount)
	}
	releaser.mu.Unlock()

	// 3. Telemetry
	metrics := poller.GetMetrics()
	if metrics.PollingDropCount != 1 {
		t.Fatalf("expected PollingDropCount=1, got %d", metrics.PollingDropCount)
	}
}

func TestFallbackPoller_BackoffAndJitter(t *testing.T) {
	config := FallbackPollerConfig{
		InitialBackoff: 2 * time.Second,
		MaxBackoff:     30 * time.Second,
	}
	poller := NewFallbackPoller(nil, nil, nil, nil, config)

	// Attempt 1: backoff is in [0.5*2s, 1.0*2s] = [1s, 2s]
	d1 := poller.calculateBackoffWithJitter(1)
	if d1 < 1*time.Second || d1 > 2*time.Second {
		t.Fatalf("expected attempt 1 backoff in [1s, 2s], got %v", d1)
	}

	// Attempt 2: factor is 2^(2-1)=2, backoff=4s, jitter in [2s, 4s]
	d2 := poller.calculateBackoffWithJitter(2)
	if d2 < 2*time.Second || d2 > 4*time.Second {
		t.Fatalf("expected attempt 2 backoff in [2s, 4s], got %v", d2)
	}

	// Attempt 10: capped at MaxBackoff (30s), jitter in [15s, 30s]
	d10 := poller.calculateBackoffWithJitter(10)
	if d10 < 15*time.Second || d10 > 30*time.Second {
		t.Fatalf("expected attempt 10 backoff in [15s, 30s], got %v", d10)
	}
}

func TestFallbackPoller_504TimeoutPatternAndAlerting(t *testing.T) {
	var alertMu sync.Mutex
	var receivedAlerts []alerting.Alert

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var a alerting.Alert
		if err := json.NewDecoder(r.Body).Decode(&a); err == nil {
			alertMu.Lock()
			receivedAlerts = append(receivedAlerts, a)
			alertMu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	alertClient := alerting.NewClient(server.URL, "settlement-test")

	config := DefaultFallbackPollerConfig()
	config.Timeout504Threshold = 3
	config.TimeoutWindow = 2 * time.Minute

	poller := NewFallbackPoller(nil, nil, nil, alertClient, config)

	timeoutErr := &horizonclient.Error{
		Problem: problem.P{
			Status: http.StatusGatewayTimeout,
			Title:  "Gateway Timeout",
		},
	}

	// Trigger 3 consecutive 504 timeouts
	poller.RecordSubmissionFailure(timeoutErr)
	poller.RecordSubmissionFailure(timeoutErr)
	poller.RecordSubmissionFailure(timeoutErr)

	metrics := poller.GetMetrics()
	if metrics.Timeout504Count != 3 {
		t.Fatalf("expected Timeout504Count=3, got %d", metrics.Timeout504Count)
	}

	// Verify alert was dispatched
	alertMu.Lock()
	if len(receivedAlerts) == 0 {
		t.Fatalf("expected alert to be sent on reaching 504 threshold")
	}
	alert := receivedAlerts[0]
	alertMu.Unlock()

	if alert.Level != alerting.LevelWarning {
		t.Fatalf("expected alert level WARNING, got %s", alert.Level)
	}
	if alert.Title != "Elevated Horizon 504 Timeouts" {
		t.Fatalf("unexpected alert title: %s", alert.Title)
	}
}

func TestFallbackPoller_MetricsPublication(t *testing.T) {
	config := DefaultFallbackPollerConfig()
	poller := NewFallbackPoller(nil, nil, nil, nil, config)

	poller.RecordSubmissionSuccess()
	poller.RecordSubmissionSuccess()
	poller.RecordSubmissionFailure(errors.New("transient error"))

	metrics := poller.GetMetrics()
	if metrics.SubmissionSuccessCount != 2 {
		t.Fatalf("expected SubmissionSuccessCount=2, got %d", metrics.SubmissionSuccessCount)
	}
	if metrics.SubmissionFailureCount != 1 {
		t.Fatalf("expected SubmissionFailureCount=1, got %d", metrics.SubmissionFailureCount)
	}
}

func TestWorker_IntegrationWithPoller(t *testing.T) {
	tx := &domain.Transaction{
		ID:     "tx-work-1",
		Status: domain.StatusPending,
	}
	repo := newFakeTxRepo(tx)
	wallets := newFakeWalletRepo()
	engine := &Engine{txRepo: repo, walletRepo: wallets}
	worker := NewWorker(engine)

	poller := NewFallbackPoller(repo, nil, nil, nil, DefaultFallbackPollerConfig())
	worker.WithPoller(poller)

	// Simulate transfer task
	payloadBytes, _ := json.Marshal(queue.ProcessTransferPayload{TransactionID: "tx-work-1"})
	task := asynq.NewTask(queue.TypeProcessTransfer, payloadBytes)

	// Engine will fail because srcWallet is not found, exercising the failure path
	_ = worker.HandleProcessTransfer(context.Background(), task)

	metrics := poller.GetMetrics()
	if metrics.SubmissionFailureCount != 1 {
		t.Fatalf("expected SubmissionFailureCount=1 on engine failure, got %d", metrics.SubmissionFailureCount)
	}
}
