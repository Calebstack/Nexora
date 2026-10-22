package reconcile

import (
	"context"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/alerting"
	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/shopspring/decimal"
)

// holdSkipRepo embeds mockRepo and overrides only the two methods
// RecoverPending touches before it would reach the settlement queue.
type holdSkipRepo struct {
	mockRepo
	stuck    []*domain.Transaction
	requeued []string
	failed   []string
	reset    []string
}

func (m *holdSkipRepo) GetStuckPendingTxes(_ context.Context, _ time.Duration, _ int) ([]*domain.Transaction, error) {
	return m.stuck, nil
}

func (m *holdSkipRepo) IncrementRequeueCount(_ context.Context, id string) (int, error) {
	m.requeued = append(m.requeued, id)
	// Over maxRequeues, so the transaction takes the mark-as-failed branch and
	// never reaches the queue client, which these tests do not construct.
	return 99, nil
}

func (m *holdSkipRepo) UpdateReconciliationStatus(_ context.Context, id string, _ domain.TransactionStatus) error {
	m.failed = append(m.failed, id)
	return nil
}

func (m *holdSkipRepo) ResetStuckSubmittedToPending(_ context.Context, id string, _ time.Duration) error {
	m.reset = append(m.reset, id)
	return nil
}

func newRecoveryService(repo Repository) *Service {
	return NewService(repo, nil, nil, nil, alerting.NewClient("", "test"), nil, nil,
		"test", decimal.Zero, nil, "")
}

// A transfer held for compliance is waiting on a human, not stuck. Recovery
// must never re-enqueue one — that would release a payment compliance
// deliberately stopped.
func TestRecoverPendingSkipsComplianceHolds(t *testing.T) {
	repo := &holdSkipRepo{stuck: []*domain.Transaction{
		{ID: "tx-held", Status: domain.StatusComplianceHold, CreatedAt: time.Now().Add(-time.Hour)},
	}}

	if err := newRecoveryService(repo).RecoverPending(context.Background()); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}

	if len(repo.requeued) != 0 {
		t.Fatalf("a held transfer was put through recovery: %v", repo.requeued)
	}
	if len(repo.failed) != 0 {
		t.Fatalf("a held transfer was marked failed by recovery: %v", repo.failed)
	}
}

// The skip must be specific to held rows — genuinely stuck pending transfers
// still get recovered.
func TestRecoverPendingStillProcessesPendingTransactions(t *testing.T) {
	repo := &holdSkipRepo{stuck: []*domain.Transaction{
		{ID: "tx-held", Status: domain.StatusComplianceHold, CreatedAt: time.Now().Add(-time.Hour)},
		{ID: "tx-stuck", Status: domain.StatusPending, CreatedAt: time.Now().Add(-time.Hour)},
	}}

	if err := newRecoveryService(repo).RecoverPending(context.Background()); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}

	if len(repo.requeued) != 1 || repo.requeued[0] != "tx-stuck" {
		t.Fatalf("recovered %v, want only [tx-stuck]", repo.requeued)
	}
	for _, id := range repo.failed {
		if id == "tx-held" {
			t.Fatal("the held transfer was marked failed")
		}
	}
}

func TestRecoverPendingDoesNotRequeueTransactionsWithHashes(t *testing.T) {
	repo := &holdSkipRepo{stuck: []*domain.Transaction{
		{ID: "tx-pending-hash", Status: domain.StatusPending, TxHash: "hash-pending", CreatedAt: time.Now().Add(-time.Hour)},
		{ID: "tx-submitted-hash", Status: domain.StatusSubmitted, TxHash: "hash-submitted", CreatedAt: time.Now().Add(-time.Hour)},
	}}

	if err := newRecoveryService(repo).RecoverPending(context.Background()); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}

	if len(repo.requeued) != 0 {
		t.Fatalf("transactions with recorded hashes were requeued: %v", repo.requeued)
	}
	if len(repo.failed) != 0 {
		t.Fatalf("transactions with recorded hashes were marked failed: %v", repo.failed)
	}
}

func TestRecoverPendingResetsStaleSubmittedWithoutHash(t *testing.T) {
	repo := &holdSkipRepo{stuck: []*domain.Transaction{
		{ID: "tx-stale-submitted", Status: domain.StatusSubmitted, CreatedAt: time.Now().Add(-time.Hour)},
	}}

	if err := newRecoveryService(repo).RecoverPending(context.Background()); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}

	if len(repo.reset) != 1 || repo.reset[0] != "tx-stale-submitted" {
		t.Fatalf("reset transactions %v, want [tx-stale-submitted]", repo.reset)
	}
}

func TestRecoverPendingLeavesLegacySubmittedWithoutHashForReview(t *testing.T) {
	repo := &holdSkipRepo{stuck: []*domain.Transaction{
		{
			ID: "tx-legacy-uncertain", Status: domain.StatusSubmitted,
			CreatedAt: time.Now().Add(-time.Hour), BroadcastOutcomeUnknown: true,
		},
	}}

	if err := newRecoveryService(repo).RecoverPending(context.Background()); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}

	if len(repo.reset) != 0 || len(repo.requeued) != 0 {
		t.Fatalf("uncertain legacy transaction was reset or requeued: reset=%v requeued=%v", repo.reset, repo.requeued)
	}
}
