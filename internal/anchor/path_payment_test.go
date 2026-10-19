package anchor

import (
	"context"
	"testing"

	"github.com/Lumen-Nexora/Nexora/internal/stellar"
	"github.com/shopspring/decimal"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/protocols/horizon/operations"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockStellarClient struct {
	accountSeq  int64
	submittedTx *txnbuild.Transaction
}

func (m *mockStellarClient) LoadAccount(accountID string) (horizon.Account, error) {
	m.accountSeq++
	return horizon.Account{
		AccountID: accountID,
		Sequence:  m.accountSeq,
	}, nil
}

func (m *mockStellarClient) SubmitTransaction(tx *txnbuild.Transaction) (horizon.Transaction, error) {
	m.submittedTx = tx
	return horizon.Transaction{
		Hash:   "mock-tx-hash-12345",
		Ledger: 1000,
	}, nil
}

func (m *mockStellarClient) FindPathsStrict(_, _, _, _, _ string) ([]horizon.Path, error) {
	return nil, nil
}
func (m *mockStellarClient) TransactionDetail(_ string) (horizon.Transaction, error) {
	return horizon.Transaction{}, nil
}
func (m *mockStellarClient) OperationsForTransaction(_ string) ([]operations.Operation, error) {
	return nil, nil
}
func (m *mockStellarClient) Payments(_, _ string, _ uint) ([]operations.Operation, error) {
	return nil, nil
}
func (m *mockStellarClient) StreamPayments(_ context.Context, _, _ string, _ func(operations.Operation) error) error {
	return nil
}
func (m *mockStellarClient) Offers(_ string, _ uint) ([]horizon.Offer, error) {
	return nil, nil
}

type mockPathFinder struct {
	sendPaths    []stellar.PaymentPath
	receivePaths []stellar.PaymentPath
	depthAssess  *stellar.DepthAssessment
	depthErr     error
}

func (m *mockPathFinder) FindStrictSendPaths(_ context.Context, _ stellar.StrictSendRequest) ([]stellar.PaymentPath, error) {
	return m.sendPaths, nil
}
func (m *mockPathFinder) FindStrictReceivePaths(_ context.Context, _ stellar.StrictReceiveRequest) ([]stellar.PaymentPath, error) {
	return m.receivePaths, nil
}
func (m *mockPathFinder) SelectBestStrictSendPath(paths []stellar.PaymentPath) (*stellar.PaymentPath, error) {
	if len(paths) == 0 {
		return nil, stellar.ErrNoPathsFound
	}
	return &paths[0], nil
}
func (m *mockPathFinder) SelectBestStrictReceivePath(paths []stellar.PaymentPath) (*stellar.PaymentPath, error) {
	if len(paths) == 0 {
		return nil, stellar.ErrNoPathsFound
	}
	return &paths[0], nil
}
func (m *mockPathFinder) AssessPathDepth(_ context.Context, _ stellar.PaymentPath, _ decimal.Decimal) (*stellar.DepthAssessment, error) {
	if m.depthErr != nil {
		return m.depthAssess, m.depthErr
	}
	if m.depthAssess != nil {
		return m.depthAssess, nil
	}
	return &stellar.DepthAssessment{SufficientDepth: true}, nil
}

func TestPathPaymentEngine_ExecuteStrictSend(t *testing.T) {
	sourceKp, err := keypair.Random()
	require.NoError(t, err)
	destKp, err := keypair.Random()
	require.NoError(t, err)
	issuerKp, err := keypair.Random()
	require.NoError(t, err)

	usdc := stellar.AssetDescriptor{Type: "credit_alphanum4", Code: "USDC", Issuer: issuerKp.Address()}
	eurc := stellar.AssetDescriptor{Type: "credit_alphanum4", Code: "EURC", Issuer: issuerKp.Address()}
	xlm := stellar.AssetDescriptor{Type: "native", Code: "XLM"}

	mockClient := &mockStellarClient{}
	mockFinder := &mockPathFinder{
		sendPaths: []stellar.PaymentPath{
			{
				SourceAsset:   usdc,
				SourceAmount:  decimal.NewFromInt(100),
				DestAsset:     eurc,
				DestAmount:    decimal.NewFromFloat(92.5),
				Path:          []stellar.AssetDescriptor{xlm},
				EffectiveRate: decimal.NewFromFloat(0.925),
				HopCount:      1,
			},
		},
	}

	engine := NewPathPaymentEngine(mockFinder, mockClient, "testnet")

	t.Run("successful strict send execution", func(t *testing.T) {
		req := StrictSendExecutionRequest{
			SourceSecretKey:    sourceKp.Seed(),
			SourceAccount:      sourceKp.Address(),
			SourceAsset:        usdc,
			SourceAmount:       decimal.NewFromInt(100),
			DestinationAccount: destKp.Address(),
			DestinationAsset:   eurc,
			DestMin:            decimal.NewFromFloat(90.0), // 90.0 < 92.5 expected -> OK
			Memo:               "payment-101",
		}

		res, err := engine.ExecuteStrictSend(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, "mock-tx-hash-12345", res.TxHash)
		assert.Equal(t, decimal.NewFromInt(100), res.SourceAmount)
		assert.Equal(t, decimal.NewFromFloat(92.5), res.DestAmount)
		assert.Len(t, res.PathUsed, 1)

		// Verify transaction constructed correctly
		require.NotNil(t, mockClient.submittedTx)
		ops := mockClient.submittedTx.Operations()
		require.Len(t, ops, 1)
		strictSendOp, ok := ops[0].(*txnbuild.PathPaymentStrictSend)
		require.True(t, ok)
		assert.Equal(t, "100.0000000", strictSendOp.SendAmount)
		assert.Equal(t, destKp.Address(), strictSendOp.Destination)
		assert.Len(t, strictSendOp.Path, 1)
	})

	t.Run("dest_min limit violation rejected", func(t *testing.T) {
		req := StrictSendExecutionRequest{
			SourceSecretKey:    sourceKp.Seed(),
			SourceAccount:      sourceKp.Address(),
			SourceAsset:        usdc,
			SourceAmount:       decimal.NewFromInt(100),
			DestinationAccount: destKp.Address(),
			DestinationAsset:   eurc,
			DestMin:            decimal.NewFromFloat(95.0), // 95.0 > 92.5 expected -> REJECT
		}

		_, err := engine.ExecuteStrictSend(context.Background(), req)
		assert.ErrorIs(t, err, ErrDestMinViolation)
	})

	t.Run("path collapse orderbook depth check rejects submission", func(t *testing.T) {
		collapsedFinder := &mockPathFinder{
			sendPaths: mockFinder.sendPaths,
			depthAssess: &stellar.DepthAssessment{
				SufficientDepth: false,
				WarningMessage:  "orderbook drained",
			},
			depthErr: stellar.ErrPathCollapsed,
		}

		collapsedEngine := NewPathPaymentEngine(collapsedFinder, mockClient, "testnet")

		req := StrictSendExecutionRequest{
			SourceSecretKey:    sourceKp.Seed(),
			SourceAccount:      sourceKp.Address(),
			SourceAsset:        usdc,
			SourceAmount:       decimal.NewFromInt(100),
			DestinationAccount: destKp.Address(),
			DestinationAsset:   eurc,
			DestMin:            decimal.NewFromFloat(90.0),
		}

		_, err := collapsedEngine.ExecuteStrictSend(context.Background(), req)
		assert.ErrorIs(t, err, stellar.ErrPathCollapsed)
	})
}

func TestPathPaymentEngine_ExecuteStrictReceive(t *testing.T) {
	sourceKp, err := keypair.Random()
	require.NoError(t, err)
	destKp, err := keypair.Random()
	require.NoError(t, err)
	issuerKp, err := keypair.Random()
	require.NoError(t, err)

	usdc := stellar.AssetDescriptor{Type: "credit_alphanum4", Code: "USDC", Issuer: issuerKp.Address()}
	eurc := stellar.AssetDescriptor{Type: "credit_alphanum4", Code: "EURC", Issuer: issuerKp.Address()}
	xlm := stellar.AssetDescriptor{Type: "native", Code: "XLM"}

	mockClient := &mockStellarClient{}
	mockFinder := &mockPathFinder{
		receivePaths: []stellar.PaymentPath{
			{
				SourceAsset:   usdc,
				SourceAmount:  decimal.NewFromFloat(108.0),
				DestAsset:     eurc,
				DestAmount:    decimal.NewFromInt(100),
				Path:          []stellar.AssetDescriptor{xlm},
				EffectiveRate: decimal.NewFromFloat(0.925),
				HopCount:      1,
			},
		},
	}

	engine := NewPathPaymentEngine(mockFinder, mockClient, "testnet")

	t.Run("successful strict receive execution", func(t *testing.T) {
		req := StrictReceiveExecutionRequest{
			SourceSecretKey:    sourceKp.Seed(),
			SourceAccount:      sourceKp.Address(),
			SourceAsset:        usdc,
			SendMax:            decimal.NewFromFloat(110.0), // 110.0 > 108.0 expected -> OK
			DestinationAccount: destKp.Address(),
			DestinationAsset:   eurc,
			DestAmount:         decimal.NewFromInt(100),
			Memo:               "invoice-456",
		}

		res, err := engine.ExecuteStrictReceive(context.Background(), req)
		require.NoError(t, err)
		assert.Equal(t, "mock-tx-hash-12345", res.TxHash)
		assert.Equal(t, decimal.NewFromFloat(108.0), res.SourceAmount)
		assert.Equal(t, decimal.NewFromInt(100), res.DestAmount)

		require.NotNil(t, mockClient.submittedTx)
		ops := mockClient.submittedTx.Operations()
		require.Len(t, ops, 1)
		strictReceiveOp, ok := ops[0].(*txnbuild.PathPaymentStrictReceive)
		require.True(t, ok)
		assert.Equal(t, "100.0000000", strictReceiveOp.DestAmount)
		assert.Equal(t, "110.0000000", strictReceiveOp.SendMax)
	})

	t.Run("send_max limit violation rejected", func(t *testing.T) {
		req := StrictReceiveExecutionRequest{
			SourceSecretKey:    sourceKp.Seed(),
			SourceAccount:      sourceKp.Address(),
			SourceAsset:        usdc,
			SendMax:            decimal.NewFromFloat(105.0), // 105.0 < 108.0 expected -> REJECT
			DestinationAccount: destKp.Address(),
			DestinationAsset:   eurc,
			DestAmount:         decimal.NewFromInt(100),
		}

		_, err := engine.ExecuteStrictReceive(context.Background(), req)
		assert.ErrorIs(t, err, ErrSendMaxViolation)
	})
}
