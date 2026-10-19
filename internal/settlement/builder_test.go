package settlement

import (
	"context"
	"fmt"
	"testing"

	"github.com/Lumen-Nexora/Nexora/internal/stellar"
	"github.com/shopspring/decimal"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockFeeStatsProvider struct {
	stats *FeeStats
	err   error
}

func (m *mockFeeStatsProvider) GetFeeStats(_ context.Context) (*FeeStats, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.stats, nil
}

type mockSimulator struct {
	result *SimulationResult
	err    error
}

func (m *mockSimulator) Simulate(_ context.Context, _ *txnbuild.Transaction) (*SimulationResult, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.result, nil
}

func TestAtomicTransactionBuilder_BundleUpTo100Operations(t *testing.T) {
	sourceKp, err := keypair.Random()
	require.NoError(t, err)
	feeWalletKp, err := keypair.Random()
	require.NoError(t, err)
	issuerKp, err := keypair.Random()
	require.NoError(t, err)

	usdc := stellar.AssetDescriptor{
		Type:   "credit_alphanum4",
		Code:   "USDC",
		Issuer: issuerKp.Address(),
	}

	sourceAcct := &horizon.Account{
		AccountID: sourceKp.Address(),
		Sequence:  100,
	}

	surgePolicy := DefaultSurgePricingPolicy()
	builder := NewAtomicTransactionBuilder(sourceAcct, "testnet", surgePolicy, nil, nil)

	// Add 30 transfers, 30 fee deductions, 30 trustline checks = 90 ops
	for i := 0; i < 30; i++ {
		recipientKp, err := keypair.Random()
		require.NoError(t, err)

		err = builder.AddTransfer(TransferOp{
			Destination: recipientKp.Address(),
			Asset:       usdc,
			Amount:      decimal.NewFromInt(10),
		})
		require.NoError(t, err)

		err = builder.AddFeeDeduction(FeeDeductionOp{
			SourceAccount: sourceKp.Address(),
			FeeCollector:  feeWalletKp.Address(),
			Asset:         usdc,
			Amount:        decimal.NewFromFloat(0.1),
		})
		require.NoError(t, err)

		err = builder.AddTrustlineCheck(TrustlineCheckOp{
			Account: recipientKp.Address(),
			Asset:   usdc,
		})
		require.NoError(t, err)
	}

	assert.Equal(t, 90, builder.OperationCount())

	// Add 10 more transfers to reach exact 100 limit
	for i := 0; i < 10; i++ {
		recipientKp, err := keypair.Random()
		require.NoError(t, err)
		err = builder.AddTransfer(TransferOp{
			Destination: recipientKp.Address(),
			Asset:       usdc,
			Amount:      decimal.NewFromInt(5),
		})
		require.NoError(t, err)
	}

	assert.Equal(t, 100, builder.OperationCount())

	// 101st operation should be rejected
	recipientKp, err := keypair.Random()
	require.NoError(t, err)
	err = builder.AddTransfer(TransferOp{
		Destination: recipientKp.Address(),
		Asset:       usdc,
		Amount:      decimal.NewFromInt(1),
	})
	assert.ErrorIs(t, err, ErrEnvelopeCapacityExceeded)

	// Build transaction
	tx, sizeReport, err := builder.Build(context.Background())
	require.NoError(t, err)
	require.NotNil(t, tx)
	require.NotNil(t, sizeReport)

	assert.Equal(t, 100, sizeReport.OperationCount)
	assert.True(t, sizeReport.EnvelopeSizeBytes > 0)
	assert.True(t, sizeReport.FitsStandardLedger)
	assert.Equal(t, int64(100), sizeReport.BaseFeePerOp)
	assert.Equal(t, int64(10000), sizeReport.TotalFeeStroops) // 100 ops * 100 stroops
}

func TestAtomicTransactionBuilder_DynamicSurgePricing(t *testing.T) {
	sourceKp, err := keypair.Random()
	require.NoError(t, err)

	sourceAcct := &horizon.Account{
		AccountID: sourceKp.Address(),
		Sequence:  200,
	}

	surgePolicy := SurgePricingPolicy{
		MinBaseFee:          100,
		MaxBaseFee:          5000,
		CongestionThreshold: 0.80, // 80% capacity
		SurgeMultiplier:     1.5,
	}

	t.Run("normal traffic: minimum base fee applied", func(t *testing.T) {
		feeStats := &mockFeeStatsProvider{
			stats: &FeeStats{
				ModeFee:             100,
				P70Fee:              100,
				P90Fee:              150,
				LedgerCapacityUsage: 0.40, // only 40% full
			},
		}

		builder := NewAtomicTransactionBuilder(sourceAcct, "testnet", surgePolicy, feeStats, nil)
		err := builder.AddTransfer(TransferOp{
			Destination: sourceKp.Address(),
			Asset:       stellar.AssetDescriptor{Type: "native"},
			Amount:      decimal.NewFromInt(1),
		})
		require.NoError(t, err)

		fee, err := builder.CalculateDynamicSurgeFee(context.Background())
		require.NoError(t, err)
		assert.Equal(t, int64(100), fee)
	})

	t.Run("high congestion: surge multiplier applied", func(t *testing.T) {
		feeStats := &mockFeeStatsProvider{
			stats: &FeeStats{
				ModeFee:             500,
				P70Fee:              800,
				P90Fee:              1000,
				LedgerCapacityUsage: 0.92, // 92% full (exceeds 80% threshold!)
			},
		}

		builder := NewAtomicTransactionBuilder(sourceAcct, "testnet", surgePolicy, feeStats, nil)
		err := builder.AddTransfer(TransferOp{
			Destination: sourceKp.Address(),
			Asset:       stellar.AssetDescriptor{Type: "native"},
			Amount:      decimal.NewFromInt(1),
		})
		require.NoError(t, err)

		// Expected surge fee = ceil(1000 * 1.5) = 1500
		fee, err := builder.CalculateDynamicSurgeFee(context.Background())
		require.NoError(t, err)
		assert.Equal(t, int64(1500), fee)

		tx, report, err := builder.Build(context.Background())
		require.NoError(t, err)
		assert.Equal(t, int64(1500), int64(tx.BaseFee()))
		assert.Equal(t, int64(1500), report.TotalFeeStroops)
	})

	t.Run("extreme surge capped by MaxBaseFee", func(t *testing.T) {
		feeStats := &mockFeeStatsProvider{
			stats: &FeeStats{
				ModeFee:             10000,
				P90Fee:              20000,
				LedgerCapacityUsage: 0.99,
			},
		}

		builder := NewAtomicTransactionBuilder(sourceAcct, "testnet", surgePolicy, feeStats, nil)
		fee, err := builder.CalculateDynamicSurgeFee(context.Background())
		require.NoError(t, err)
		assert.Equal(t, int64(5000), fee) // Capped at MaxBaseFee (5000)
	})
}

func TestAtomicTransactionBuilder_MultiPartyAtomicSigning(t *testing.T) {
	primaryKp, err := keypair.Random()
	require.NoError(t, err)
	participantKp, err := keypair.Random()
	require.NoError(t, err)

	sourceAcct := &horizon.Account{
		AccountID: primaryKp.Address(),
		Sequence:  300,
	}

	builder := NewAtomicTransactionBuilder(sourceAcct, "testnet", DefaultSurgePricingPolicy(), nil, nil)

	// Primary party transfer
	err = builder.AddTransfer(TransferOp{
		SourceAccount: primaryKp.Address(),
		Destination:   participantKp.Address(),
		Asset:         stellar.AssetDescriptor{Type: "native"},
		Amount:        decimal.NewFromInt(10),
	})
	require.NoError(t, err)

	// Participant party transfer in same envelope
	err = builder.AddTransfer(TransferOp{
		SourceAccount: participantKp.Address(),
		Destination:   primaryKp.Address(),
		Asset:         stellar.AssetDescriptor{Type: "native"},
		Amount:        decimal.NewFromInt(5),
	})
	require.NoError(t, err)

	tx, _, err := builder.Build(context.Background())
	require.NoError(t, err)

	// Initially neither has signed
	err = builder.VerifyAllPartiesSigned(tx)
	assert.ErrorIs(t, err, ErrMissingSignatures)

	// Sign only with primary
	signedOnce, err := builder.Sign(tx, primaryKp)
	require.NoError(t, err)
	err = builder.VerifyAllPartiesSigned(signedOnce)
	assert.ErrorIs(t, err, ErrMissingSignatures)

	// Sign with participant as well
	signedBoth, err := builder.Sign(signedOnce, participantKp)
	require.NoError(t, err)
	err = builder.VerifyAllPartiesSigned(signedBoth)
	require.NoError(t, err) // All parties signed!
	assert.Len(t, signedBoth.Signatures(), 2)
}

func TestAtomicTransactionBuilder_Simulation(t *testing.T) {
	sourceKp, err := keypair.Random()
	require.NoError(t, err)

	sourceAcct := &horizon.Account{
		AccountID: sourceKp.Address(),
		Sequence:  400,
	}

	sim := &mockSimulator{
		result: &SimulationResult{
			Success:         true,
			EstimatedLedger: 1050,
			EstimatedFee:    200,
			OperationResults: []string{
				"PAYMENT destination=G... amount=10.0000000: VALID",
			},
		},
	}

	builder := NewAtomicTransactionBuilder(sourceAcct, "testnet", DefaultSurgePricingPolicy(), nil, sim)

	err = builder.AddTransfer(TransferOp{
		Destination: sourceKp.Address(),
		Asset:       stellar.AssetDescriptor{Type: "native"},
		Amount:      decimal.NewFromInt(10),
	})
	require.NoError(t, err)

	tx, _, err := builder.Build(context.Background())
	require.NoError(t, err)

	simResult, err := builder.Simulate(context.Background(), tx)
	require.NoError(t, err)
	assert.True(t, simResult.Success)
	assert.Equal(t, int32(1050), simResult.EstimatedLedger)
	assert.Equal(t, int64(200), simResult.EstimatedFee)
}

func TestAtomicTransactionBuilder_ExactByteSize(t *testing.T) {
	sourceKp, err := keypair.Random()
	require.NoError(t, err)

	sourceAcct := &horizon.Account{
		AccountID: sourceKp.Address(),
		Sequence:  500,
	}

	builder := NewAtomicTransactionBuilder(sourceAcct, "testnet", DefaultSurgePricingPolicy(), nil, nil)

	for i := 0; i < 5; i++ {
		destKp, err := keypair.Random()
		require.NoError(t, err)
		err = builder.AddTransfer(TransferOp{
			Destination: destKp.Address(),
			Asset:       stellar.AssetDescriptor{Type: "native"},
			Amount:      decimal.NewFromInt(int64(i + 1)),
		})
		require.NoError(t, err)
	}

	tx, report, err := builder.Build(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 5, report.OperationCount)
	assert.True(t, report.EnvelopeSizeBytes > 300) // typical 5-op envelope is ~400-500 bytes
	assert.True(t, report.FeePerByte > 0.0)
	assert.True(t, report.FitsStandardLedger)
	fmt.Printf("Envelope size: %d bytes, fee per byte: %.4f stroops/byte\n", report.EnvelopeSizeBytes, report.FeePerByte)
	_ = tx
}
