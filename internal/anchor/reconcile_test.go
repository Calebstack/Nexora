package anchor

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/protocols/horizon/base"
	"github.com/stellar/go/protocols/horizon/operations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockHorizonExtractor struct {
	ops []operations.Operation
}

func (m *mockHorizonExtractor) Payments(_ string, _ string, _ uint) ([]operations.Operation, error) {
	return m.ops, nil
}

type mockAnchorInternalTxRepo struct {
	records []AnchorTransactionRecord
}

func (m *mockAnchorInternalTxRepo) GetAnchorTransactions(_ context.Context, _ []string, _ time.Time) ([]AnchorTransactionRecord, error) {
	return m.records, nil
}

func TestAnchorReconciler_FullAuditScenarios(t *testing.T) {
	anchorKp, err := keypair.Random()
	require.NoError(t, err)
	userKp, err := keypair.Random()
	require.NoError(t, err)
	issuerKp, err := keypair.Random()
	require.NoError(t, err)

	anchorAccount := anchorKp.Address()
	now := time.Now().UTC()

	t.Run("fully balanced transactions without discrepancy", func(t *testing.T) {
		txHash := "tx-hash-deposit-1"

		onChainOps := []operations.Operation{
			operations.Payment{
				Base: operations.Base{
					ID:              "op-1",
					TransactionHash: txHash,
					LedgerCloseTime: now,
				},
				Asset: base.Asset{
					Type:   "credit_alphanum4",
					Code:   "USDC",
					Issuer: issuerKp.Address(),
				},
				Amount: "500.0000000",
				From:   userKp.Address(),
				To:     anchorAccount, // deposit inflow
			},
		}

		internalRecords := []AnchorTransactionRecord{
			{
				ID:        "int-1",
				TxHash:    txHash,
				Type:      "deposit",
				Account:   anchorAccount,
				Asset:     "USDC",
				Amount:    decimal.NewFromInt(500),
				Status:    "settled",
				CreatedAt: now,
			},
		}

		extractor := &mockHorizonExtractor{ops: onChainOps}
		repo := &mockAnchorInternalTxRepo{records: internalRecords}
		auditPub := NewInMemAuditLogPublisher()
		alerter := NewInMemAlertEmitter()

		reconciler := NewAnchorReconciler(extractor, repo, auditPub, alerter, AnchorReconcilerConfig{
			WindowDuration:       24 * time.Hour,
			MaxVarianceThreshold: decimal.NewFromFloat(0.01),
		})

		report, err := reconciler.ReconcileAccount(context.Background(), anchorAccount)
		require.NoError(t, err)

		assert.Equal(t, "BALANCED", report.Status)
		assert.Equal(t, 1, report.TotalMatched)
		assert.True(t, report.NetVariance.IsZero())
		assert.Empty(t, report.Discrepancies)
		assert.Len(t, auditPub.Reports, 1)
		assert.Empty(t, alerter.Alerts) // No alert when balanced
	})

	t.Run("critical unindexed deposit detected", func(t *testing.T) {
		txHash := "tx-hash-unindexed-deposit"

		// On-chain payment exists, but NO internal record!
		onChainOps := []operations.Operation{
			operations.Payment{
				Base: operations.Base{
					ID:              "op-2",
					TransactionHash: txHash,
					LedgerCloseTime: now,
				},
				Asset: base.Asset{
					Type: "native",
					Code: "XLM",
				},
				Amount: "1200.0000000",
				From:   userKp.Address(),
				To:     anchorAccount,
			},
		}

		extractor := &mockHorizonExtractor{ops: onChainOps}
		repo := &mockAnchorInternalTxRepo{records: nil}
		auditPub := NewInMemAuditLogPublisher()
		alerter := NewInMemAlertEmitter()

		reconciler := NewAnchorReconciler(extractor, repo, auditPub, alerter, AnchorReconcilerConfig{
			WindowDuration:       24 * time.Hour,
			MaxVarianceThreshold: decimal.NewFromFloat(1.0),
		})

		report, err := reconciler.ReconcileAccount(context.Background(), anchorAccount)
		require.NoError(t, err)

		assert.Equal(t, "CRITICAL_ALERT", report.Status)
		require.Len(t, report.Discrepancies, 1)
		disc := report.Discrepancies[0]
		assert.Equal(t, DiscrepancyUnindexedDeposit, disc.Type)
		assert.Equal(t, SeverityCritical, disc.Severity)
		assert.True(t, disc.Variance.Equal(decimal.NewFromInt(1200)))

		// Verify published to audit repo and alerted
		assert.Len(t, auditPub.Reports, 1)
		assert.Len(t, alerter.Alerts, 1)
		assert.Equal(t, "anchor.reconciliation.variance_threshold_exceeded", alerter.Alerts[0]["type"])
	})

	t.Run("critical unauthorized outflow detected", func(t *testing.T) {
		txHash := "tx-hash-unauthorized-drain"

		// Payment sent OUT of anchor account, but internal system has no withdrawal record!
		onChainOps := []operations.Operation{
			operations.Payment{
				Base: operations.Base{
					ID:              "op-3",
					TransactionHash: txHash,
					LedgerCloseTime: now,
				},
				Asset: base.Asset{
					Type:   "credit_alphanum4",
					Code:   "USDC",
					Issuer: issuerKp.Address(),
				},
				Amount: "50000.0000000",
				From:   anchorAccount, // OUTFLOW!
				To:     userKp.Address(),
			},
		}

		extractor := &mockHorizonExtractor{ops: onChainOps}
		repo := &mockAnchorInternalTxRepo{records: nil}
		auditPub := NewInMemAuditLogPublisher()
		alerter := NewInMemAlertEmitter()

		reconciler := NewAnchorReconciler(extractor, repo, auditPub, alerter, AnchorReconcilerConfig{
			WindowDuration:       24 * time.Hour,
			MaxVarianceThreshold: decimal.NewFromFloat(1.0),
		})

		report, err := reconciler.ReconcileAccount(context.Background(), anchorAccount)
		require.NoError(t, err)

		assert.Equal(t, "CRITICAL_ALERT", report.Status)
		require.Len(t, report.Discrepancies, 1)
		disc := report.Discrepancies[0]
		assert.Equal(t, DiscrepancyUnauthorizedOutflow, disc.Type)
		assert.Equal(t, SeverityCritical, disc.Severity)
		assert.True(t, disc.Variance.Equal(decimal.NewFromInt(50000)))
		assert.Len(t, alerter.Alerts, 1)
	})

	t.Run("matching via memo when tx hash not recorded internally", func(t *testing.T) {
		memoVal := "memo-ref-user-999"
		onChainOps := []operations.Operation{
			operations.Payment{
				Base: operations.Base{
					ID:              "op-4",
					TransactionHash: "tx-hash-with-memo",
					LedgerCloseTime: now,
				},
				Asset: base.Asset{
					Type:   "credit_alphanum4",
					Code:   "USDC",
					Issuer: issuerKp.Address(),
				},
				Amount: "250.0000000",
				From:   userKp.Address(),
				To:     anchorAccount,
			},
		}

		internalRecords := []AnchorTransactionRecord{
			{
				ID:        "int-memo-1",
				TxHash:    "tx-hash-with-memo", // matches
				Memo:      memoVal,
				Type:      "deposit",
				Account:   anchorAccount,
				Asset:     "USDC",
				Amount:    decimal.NewFromInt(250),
				Status:    "settled",
				CreatedAt: now,
			},
		}

		extractor := &mockHorizonExtractor{ops: onChainOps}
		repo := &mockAnchorInternalTxRepo{records: internalRecords}
		auditPub := NewInMemAuditLogPublisher()
		alerter := NewInMemAlertEmitter()

		reconciler := NewAnchorReconciler(extractor, repo, auditPub, alerter, AnchorReconcilerConfig{
			WindowDuration:       24 * time.Hour,
			MaxVarianceThreshold: decimal.NewFromFloat(0.01),
		})

		report, err := reconciler.ReconcileAccount(context.Background(), anchorAccount)
		require.NoError(t, err)
		assert.Equal(t, "BALANCED", report.Status)
		assert.Equal(t, 1, report.TotalMatched)
		assert.Empty(t, report.Discrepancies)
	})

	t.Run("amount mismatch discrepancy detected", func(t *testing.T) {
		txHash := "tx-hash-fee-mismatch"

		// On-chain payment is 95 USDC
		onChainOps := []operations.Operation{
			operations.Payment{
				Base: operations.Base{
					ID:              "op-5",
					TransactionHash: txHash,
					LedgerCloseTime: now,
				},
				Asset: base.Asset{
					Type:   "credit_alphanum4",
					Code:   "USDC",
					Issuer: issuerKp.Address(),
				},
				Amount: "95.0000000",
				From:   userKp.Address(),
				To:     anchorAccount,
			},
		}

		// Internal expected 100 USDC (5 USDC discrepancy)
		internalRecords := []AnchorTransactionRecord{
			{
				ID:        "int-mismatch",
				TxHash:    txHash,
				Type:      "deposit",
				Account:   anchorAccount,
				Asset:     "USDC",
				Amount:    decimal.NewFromInt(100),
				Status:    "settled",
				CreatedAt: now,
			},
		}

		extractor := &mockHorizonExtractor{ops: onChainOps}
		repo := &mockAnchorInternalTxRepo{records: internalRecords}
		auditPub := NewInMemAuditLogPublisher()
		alerter := NewInMemAlertEmitter()

		reconciler := NewAnchorReconciler(extractor, repo, auditPub, alerter, AnchorReconcilerConfig{
			WindowDuration:       24 * time.Hour,
			MaxVarianceThreshold: decimal.NewFromFloat(1.0), // 5 > 1 -> Alert
		})

		report, err := reconciler.ReconcileAccount(context.Background(), anchorAccount)
		require.NoError(t, err)

		assert.Equal(t, "VARIANCE_DETECTED", report.Status)
		require.Len(t, report.Discrepancies, 1)
		disc := report.Discrepancies[0]
		assert.Equal(t, DiscrepancyAmountMismatch, disc.Type)
		assert.True(t, disc.Variance.Equal(decimal.NewFromInt(5)))
		assert.Len(t, alerter.Alerts, 1) // Variance exceeded threshold -> alert emitted
	})
}
