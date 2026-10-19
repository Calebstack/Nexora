package reconcile

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/anchor"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog/log"
)

// AnchorAccountProvider supplies active anchor distribution accounts for reconciliation.
type AnchorAccountProvider interface {
	ListAnchorDistributionAccounts(ctx context.Context) ([]string, error)
}

// StaticAnchorAccountProvider provides a static list of anchor distribution accounts.
type StaticAnchorAccountProvider struct {
	accounts []string
}

// NewStaticAnchorAccountProvider creates a new StaticAnchorAccountProvider.
func NewStaticAnchorAccountProvider(accounts ...string) *StaticAnchorAccountProvider {
	return &StaticAnchorAccountProvider{accounts: accounts}
}

func (p *StaticAnchorAccountProvider) ListAnchorDistributionAccounts(_ context.Context) ([]string, error) {
	copied := make([]string, len(p.accounts))
	copy(copied, p.accounts)
	return copied, nil
}

// AnchorSyncWorker coordinates periodic or on-demand background reconciliation of anchor distribution accounts.
type AnchorSyncWorker struct {
	reconciler      *anchor.AnchorReconciler
	accountProvider AnchorAccountProvider
	mu              sync.Mutex
	lastRunReports  []*anchor.ReconciliationVarianceReport
	lastRunTime     time.Time
}

// NewAnchorSyncWorker constructs a new AnchorSyncWorker.
func NewAnchorSyncWorker(
	reconciler *anchor.AnchorReconciler,
	accountProvider AnchorAccountProvider,
) *AnchorSyncWorker {
	return &AnchorSyncWorker{
		reconciler:      reconciler,
		accountProvider: accountProvider,
		lastRunReports:  make([]*anchor.ReconciliationVarianceReport, 0),
	}
}

// SyncAllAnchors runs reconciliation audits across all active anchor accounts.
func (w *AnchorSyncWorker) SyncAllAnchors(ctx context.Context) ([]*anchor.ReconciliationVarianceReport, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	accounts, err := w.accountProvider.ListAnchorDistributionAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("list anchor accounts: %w", err)
	}

	reports := make([]*anchor.ReconciliationVarianceReport, 0, len(accounts))

	for _, account := range accounts {
		report, err := w.reconciler.ReconcileAccount(ctx, account)
		if err != nil {
			log.Error().Err(err).Str("anchor_account", account).Msg("anchor reconciliation run failed for account")
			continue
		}
		reports = append(reports, report)
	}

	w.lastRunReports = reports
	w.lastRunTime = time.Now().UTC()

	return reports, nil
}

// HandleAnchorReconcileTask is an Asynq handler that executes background anchor reconciliation.
func (w *AnchorSyncWorker) HandleAnchorReconcileTask(ctx context.Context, _ *asynq.Task) error {
	log.Info().Msg("starting automated anchor balance discrepancy reconciliation run")
	reports, err := w.SyncAllAnchors(ctx)
	if err != nil {
		return err
	}

	criticalCount := 0
	varianceCount := 0
	for _, r := range reports {
		if r.Status == "CRITICAL_ALERT" {
			criticalCount++
		} else if r.Status == "VARIANCE_DETECTED" {
			varianceCount++
		}
	}

	log.Info().
		Int("total_accounts", len(reports)).
		Int("critical_alerts", criticalCount).
		Int("variances_detected", varianceCount).
		Msg("anchor reconciliation run complete")

	return nil
}

// LastRunSummary returns the results from the most recent reconciliation run.
func (w *AnchorSyncWorker) LastRunSummary() (time.Time, []*anchor.ReconciliationVarianceReport) {
	w.mu.Lock()
	defer w.mu.Unlock()
	copied := make([]*anchor.ReconciliationVarianceReport, len(w.lastRunReports))
	copy(copied, w.lastRunReports)
	return w.lastRunTime, copied
}
