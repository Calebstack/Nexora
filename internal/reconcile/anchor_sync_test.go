package reconcile

import (
	"context"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/anchor"
	"github.com/shopspring/decimal"
	"github.com/stellar/go/protocols/horizon/operations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type dummyExtractor struct{}

func (d *dummyExtractor) Payments(_ string, _ string, _ uint) ([]operations.Operation, error) {
	return nil, nil
}

type dummyInternalRepo struct{}

func (d *dummyInternalRepo) GetAnchorTransactions(_ context.Context, _ []string, _ time.Time) ([]anchor.AnchorTransactionRecord, error) {
	return nil, nil
}

func TestAnchorSyncWorker_SyncAllAnchors(t *testing.T) {
	accounts := []string{
		"GAAZI4TCR3TY5OJHCTJC2A4QSY6CJWJH5IAJTGKIN2ER7LBNVKOCCWN7",
		"GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
	}

	provider := NewStaticAnchorAccountProvider(accounts...)

	reconciler := anchor.NewAnchorReconciler(
		&dummyExtractor{},
		&dummyInternalRepo{},
		anchor.NewInMemAuditLogPublisher(),
		anchor.NewInMemAlertEmitter(),
		anchor.AnchorReconcilerConfig{
			WindowDuration:       24 * time.Hour,
			MaxVarianceThreshold: decimal.NewFromFloat(0.01),
		},
	)

	worker := NewAnchorSyncWorker(reconciler, provider)

	reports, err := worker.SyncAllAnchors(context.Background())
	require.NoError(t, err)
	assert.Len(t, reports, 2)
	assert.Equal(t, "BALANCED", reports[0].Status)
	assert.Equal(t, "BALANCED", reports[1].Status)

	lastTime, lastReports := worker.LastRunSummary()
	assert.False(t, lastTime.IsZero())
	assert.Len(t, lastReports, 2)
}
