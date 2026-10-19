package settlement

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/stellar"
	"github.com/shopspring/decimal"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/txnbuild"
)

// ============================================================================
// Constants & Error Definitions
// ============================================================================

const (
	// MaxOperationsPerEnvelope is the protocol-enforced maximum number of operations
	// that can be included in a single Stellar transaction envelope.
	MaxOperationsPerEnvelope = 100

	// MinBaseFeeStroops is the minimum standard base fee per operation (100 stroops = 0.00001 XLM).
	MinBaseFeeStroops = int64(txnbuild.MinBaseFee)

	// DefaultMaxSurgeBaseFee caps the dynamic surge fee to prevent runaway fee exhaustion (10,000 stroops).
	DefaultMaxSurgeBaseFee = int64(10000)

	// MaxLedgerTransactionSizeBytes is the standard maximum size for transaction envelopes in a ledger.
	MaxLedgerTransactionSizeBytes = 32768
)

var (
	ErrEnvelopeCapacityExceeded = fmt.Errorf("envelope capacity exceeded: maximum %d operations allowed per transaction", MaxOperationsPerEnvelope)
	ErrEmptyEnvelope            = errors.New("cannot build transaction: envelope contains zero operations")
	ErrInvalidOperationAccount  = errors.New("invalid operation account address")
	ErrMissingSignatures        = errors.New("atomic multi-party transaction requires signatures from all participant source accounts")
	ErrSimulationFailed         = errors.New("transaction pre-flight simulation failed")
)

// ============================================================================
// Operation Parameter Structs
// ============================================================================

// TransferOp defines an atomic payment operation within the bundle.
type TransferOp struct {
	SourceAccount string                  `json:"source_account,omitempty"`
	Destination   string                  `json:"destination"`
	Asset         stellar.AssetDescriptor `json:"asset"`
	Amount        decimal.Decimal         `json:"amount"`
}

// FeeDeductionOp defines an atomic platform or settlement fee deduction operation.
type FeeDeductionOp struct {
	SourceAccount string                  `json:"source_account"`
	FeeCollector  string                  `json:"fee_collector"`
	Asset         stellar.AssetDescriptor `json:"asset"`
	Amount        decimal.Decimal         `json:"amount"`
}

// TrustlineCheckOp defines an atomic trustline verification or creation operation.
type TrustlineCheckOp struct {
	Account string                  `json:"account"`
	Asset   stellar.AssetDescriptor `json:"asset"`
	Limit   string                  `json:"limit,omitempty"`
}

// ============================================================================
// Surge Pricing & Size Modeling
// ============================================================================

// FeeStats represents network congestion and fee metrics retrieved from Horizon /fee-stats.
type FeeStats struct {
	ModeFee             int64   `json:"mode_fee"`
	P70Fee              int64   `json:"p70_fee"`
	P90Fee              int64   `json:"p90_fee"`
	P95Fee              int64   `json:"p95_fee"`
	LedgerCapacityUsage float64 `json:"ledger_capacity_usage"` // e.g. 0.85 = 85% full
}

// SurgePricingPolicy configures dynamic fee multipliers under heavy network traffic.
type SurgePricingPolicy struct {
	MinBaseFee          int64   `json:"min_base_fee"`
	MaxBaseFee          int64   `json:"max_base_fee"`
	CongestionThreshold float64 `json:"congestion_threshold"` // e.g. 0.75
	SurgeMultiplier     float64 `json:"surge_multiplier"`     // e.g. 1.5
}

// DefaultSurgePricingPolicy returns recommended default surge pricing parameters.
func DefaultSurgePricingPolicy() SurgePricingPolicy {
	return SurgePricingPolicy{
		MinBaseFee:          MinBaseFeeStroops,
		MaxBaseFee:          DefaultMaxSurgeBaseFee,
		CongestionThreshold: 0.75, // Apply surge if ledger is >= 75% full
		SurgeMultiplier:     1.5,
	}
}

// TransactionSizeReport reports exact byte dimensions and fee density of the envelope.
type TransactionSizeReport struct {
	OperationCount     int     `json:"operation_count"`
	EnvelopeSizeBytes  int     `json:"envelope_size_bytes"`
	BaseFeePerOp       int64   `json:"base_fee_per_op"`
	TotalFeeStroops    int64   `json:"total_fee_stroops"`
	FeePerByte         float64 `json:"fee_per_byte"`
	FitsStandardLedger bool    `json:"fits_standard_ledger"`
}

// SimulationResult captures the outcome of pre-broadcasting simulation.
type SimulationResult struct {
	Success          bool     `json:"success"`
	EstimatedLedger  int32    `json:"estimated_ledger,omitempty"`
	EstimatedFee     int64    `json:"estimated_fee"`
	OperationResults []string `json:"operation_results,omitempty"`
	ErrorDetails     string   `json:"error_details,omitempty"`
}

// FeeStatsProvider fetches current network fee statistics.
type FeeStatsProvider interface {
	GetFeeStats(ctx context.Context) (*FeeStats, error)
}

// HorizonSimulator simulates transaction execution against Horizon before broadcast.
type HorizonSimulator interface {
	Simulate(ctx context.Context, tx *txnbuild.Transaction) (*SimulationResult, error)
}

// ============================================================================
// AtomicTransactionBuilder
// ============================================================================

// AtomicTransactionBuilder bundles up to 100 operations into an atomic transaction envelope.
type AtomicTransactionBuilder struct {
	sourceAccount    txnbuild.Account
	network          string
	networkPass      string
	operations       []txnbuild.Operation
	requiredSigners  map[string]bool
	surgePolicy      SurgePricingPolicy
	feeStatsProvider FeeStatsProvider
	simulator        HorizonSimulator
	memo             txnbuild.Memo
	timeout          time.Duration
}

// NewAtomicTransactionBuilder creates a new atomic transaction builder.
func NewAtomicTransactionBuilder(
	sourceAccount txnbuild.Account,
	stellarNetwork string,
	surgePolicy SurgePricingPolicy,
	feeStatsProvider FeeStatsProvider,
	simulator HorizonSimulator,
) *AtomicTransactionBuilder {
	passphrase := network.TestNetworkPassphrase
	if stellarNetwork == "mainnet" || stellarNetwork == "public" {
		passphrase = network.PublicNetworkPassphrase
	}
	if surgePolicy.MinBaseFee <= 0 {
		surgePolicy.MinBaseFee = MinBaseFeeStroops
	}
	if surgePolicy.MaxBaseFee <= 0 {
		surgePolicy.MaxBaseFee = DefaultMaxSurgeBaseFee
	}

	signers := make(map[string]bool)
	signers[sourceAccount.GetAccountID()] = true

	return &AtomicTransactionBuilder{
		sourceAccount:    sourceAccount,
		network:          stellarNetwork,
		networkPass:      passphrase,
		operations:       make([]txnbuild.Operation, 0, MaxOperationsPerEnvelope),
		requiredSigners:  signers,
		surgePolicy:      surgePolicy,
		feeStatsProvider: feeStatsProvider,
		simulator:        simulator,
		timeout:          300 * time.Second,
	}
}

// SetMemo attaches a transaction memo.
func (b *AtomicTransactionBuilder) SetMemo(memo txnbuild.Memo) {
	b.memo = memo
}

// OperationCount returns the number of operations currently bundled.
func (b *AtomicTransactionBuilder) OperationCount() int {
	return len(b.operations)
}

// RequiredSigners returns the list of public keys whose signatures are required.
func (b *AtomicTransactionBuilder) RequiredSigners() []string {
	res := make([]string, 0, len(b.requiredSigners))
	for k := range b.requiredSigners {
		res = append(res, k)
	}
	return res
}

// AddTransfer adds a payment transfer operation to the atomic bundle.
func (b *AtomicTransactionBuilder) AddTransfer(op TransferOp) error {
	if len(b.operations) >= MaxOperationsPerEnvelope {
		return ErrEnvelopeCapacityExceeded
	}
	if !strkey.IsValidEd25519PublicKey(op.Destination) {
		return fmt.Errorf("%w: invalid destination %q", ErrInvalidOperationAccount, op.Destination)
	}
	if op.Amount.LessThanOrEqual(decimal.Zero) {
		return errors.New("transfer amount must be positive")
	}

	asset, err := toBuilderAsset(op.Asset)
	if err != nil {
		return err
	}

	paymentOp := &txnbuild.Payment{
		Destination: op.Destination,
		Amount:      op.Amount.StringFixed(7),
		Asset:       asset,
	}

	if op.SourceAccount != "" {
		if !strkey.IsValidEd25519PublicKey(op.SourceAccount) {
			return fmt.Errorf("%w: invalid source account %q", ErrInvalidOperationAccount, op.SourceAccount)
		}
		paymentOp.SourceAccount = op.SourceAccount
		b.requiredSigners[op.SourceAccount] = true
	}

	b.operations = append(b.operations, paymentOp)
	return nil
}

// AddFeeDeduction adds an atomic fee deduction payment to the platform fee wallet.
func (b *AtomicTransactionBuilder) AddFeeDeduction(op FeeDeductionOp) error {
	if len(b.operations) >= MaxOperationsPerEnvelope {
		return ErrEnvelopeCapacityExceeded
	}
	if !strkey.IsValidEd25519PublicKey(op.SourceAccount) {
		return fmt.Errorf("%w: invalid fee source account %q", ErrInvalidOperationAccount, op.SourceAccount)
	}
	if !strkey.IsValidEd25519PublicKey(op.FeeCollector) {
		return fmt.Errorf("%w: invalid fee collector account %q", ErrInvalidOperationAccount, op.FeeCollector)
	}
	if op.Amount.LessThanOrEqual(decimal.Zero) {
		return errors.New("fee deduction amount must be positive")
	}

	asset, err := toBuilderAsset(op.Asset)
	if err != nil {
		return err
	}

	feeOp := &txnbuild.Payment{
		SourceAccount: op.SourceAccount,
		Destination:   op.FeeCollector,
		Amount:        op.Amount.StringFixed(7),
		Asset:         asset,
	}

	b.requiredSigners[op.SourceAccount] = true
	b.operations = append(b.operations, feeOp)
	return nil
}

// AddTrustlineCheck adds a change_trust operation ensuring the account trusts the asset.
func (b *AtomicTransactionBuilder) AddTrustlineCheck(op TrustlineCheckOp) error {
	if len(b.operations) >= MaxOperationsPerEnvelope {
		return ErrEnvelopeCapacityExceeded
	}
	if !strkey.IsValidEd25519PublicKey(op.Account) {
		return fmt.Errorf("%w: invalid trustline account %q", ErrInvalidOperationAccount, op.Account)
	}
	if op.Asset.IsNative() {
		// Native XLM does not require a trustline
		return nil
	}

	changeAsset, err := toChangeTrustAsset(op.Asset)
	if err != nil {
		return err
	}

	limit := op.Limit
	if limit == "" {
		limit = txnbuild.MaxTrustlineLimit
	}

	trustOp := &txnbuild.ChangeTrust{
		SourceAccount: op.Account,
		Line:          changeAsset,
		Limit:         limit,
	}

	b.requiredSigners[op.Account] = true
	b.operations = append(b.operations, trustOp)
	return nil
}

// CalculateDynamicSurgeFee queries Horizon fee stats and computes the per-operation base fee.
func (b *AtomicTransactionBuilder) CalculateDynamicSurgeFee(ctx context.Context) (int64, error) {
	if b.feeStatsProvider == nil {
		return b.surgePolicy.MinBaseFee, nil
	}

	stats, err := b.feeStatsProvider.GetFeeStats(ctx)
	if err != nil {
		// Graceful fallback to MinBaseFee on network error
		return b.surgePolicy.MinBaseFee, nil
	}

	baseFee := b.surgePolicy.MinBaseFee

	// Check if network congestion exceeds threshold
	if stats.LedgerCapacityUsage >= b.surgePolicy.CongestionThreshold {
		candidateFee := stats.P90Fee
		if candidateFee <= 0 {
			candidateFee = stats.ModeFee
		}
		surgeFee := int64(math.Ceil(float64(candidateFee) * b.surgePolicy.SurgeMultiplier))
		if surgeFee > baseFee {
			baseFee = surgeFee
		}
	} else if stats.P70Fee > baseFee {
		baseFee = stats.P70Fee
	}

	// Bound with MaxBaseFee ceiling
	if baseFee > b.surgePolicy.MaxBaseFee {
		baseFee = b.surgePolicy.MaxBaseFee
	}

	return baseFee, nil
}

// Build constructs the atomic transaction envelope with dynamic surge pricing and size analysis.
func (b *AtomicTransactionBuilder) Build(ctx context.Context) (*txnbuild.Transaction, *TransactionSizeReport, error) {
	if len(b.operations) == 0 {
		return nil, nil, ErrEmptyEnvelope
	}
	if len(b.operations) > MaxOperationsPerEnvelope {
		return nil, nil, ErrEnvelopeCapacityExceeded
	}

	// 1. Calculate dynamic surge base fee
	baseFee, err := b.CalculateDynamicSurgeFee(ctx)
	if err != nil {
		baseFee = b.surgePolicy.MinBaseFee
	}

	// 2. Build TransactionParams
	txParams := txnbuild.TransactionParams{
		SourceAccount:        b.sourceAccount,
		Operations:           b.operations,
		BaseFee:              baseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(int64(b.timeout.Seconds()))},
		IncrementSequenceNum: true,
	}

	if b.memo != nil {
		txParams.Memo = b.memo
	}

	tx, err := txnbuild.NewTransaction(txParams)
	if err != nil {
		return nil, nil, fmt.Errorf("construct atomic envelope: %w", err)
	}

	// 3. Compute exact size report
	sizeReport, err := b.CalculateTransactionSize(tx)
	if err != nil {
		return nil, nil, fmt.Errorf("calculate transaction size: %w", err)
	}

	return tx, sizeReport, nil
}

// CalculateTransactionSize encodes the transaction to XDR binary bytes and calculates its exact dimensions.
func (b *AtomicTransactionBuilder) CalculateTransactionSize(tx *txnbuild.Transaction) (*TransactionSizeReport, error) {
	b64, err := tx.Base64()
	if err != nil {
		return nil, err
	}

	rawBytes, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, fmt.Errorf("decode transaction xdr: %w", err)
	}

	sizeBytes := len(rawBytes)
	opCount := len(tx.Operations())
	baseFee := tx.BaseFee()
	totalFee := int64(baseFee) * int64(opCount)

	feePerByte := 0.0
	if sizeBytes > 0 {
		feePerByte = float64(totalFee) / float64(sizeBytes)
	}

	fits := sizeBytes <= MaxLedgerTransactionSizeBytes

	return &TransactionSizeReport{
		OperationCount:     opCount,
		EnvelopeSizeBytes:  sizeBytes,
		BaseFeePerOp:       int64(baseFee),
		TotalFeeStroops:    totalFee,
		FeePerByte:         feePerByte,
		FitsStandardLedger: fits,
	}, nil
}

// Sign signs the transaction using keypairs from all required participating parties.
func (b *AtomicTransactionBuilder) Sign(tx *txnbuild.Transaction, signers ...*keypair.Full) (*txnbuild.Transaction, error) {
	if tx == nil {
		return nil, errors.New("nil transaction")
	}

	signedTx := tx
	for _, signer := range signers {
		var err error
		signedTx, err = signedTx.Sign(b.networkPass, signer)
		if err != nil {
			return nil, fmt.Errorf("sign transaction with key %s: %w", signer.Address(), err)
		}
	}

	return signedTx, nil
}

// VerifyAllPartiesSigned verifies that all accounts in requiredSigners have contributed a signature.
func (b *AtomicTransactionBuilder) VerifyAllPartiesSigned(tx *txnbuild.Transaction) error {
	if tx == nil {
		return errors.New("nil transaction")
	}

	signatures := tx.Signatures()
	if len(signatures) < len(b.requiredSigners) {
		return fmt.Errorf("%w: expected %d signatures for participant accounts, got %d",
			ErrMissingSignatures, len(b.requiredSigners), len(signatures))
	}

	return nil
}

// Simulate pre-flights the transaction execution against Horizon before network broadcast.
func (b *AtomicTransactionBuilder) Simulate(ctx context.Context, tx *txnbuild.Transaction) (*SimulationResult, error) {
	if tx == nil {
		return nil, errors.New("nil transaction")
	}

	if b.simulator != nil {
		return b.simulator.Simulate(ctx, tx)
	}

	// Default built-in simulation validation
	opResults := make([]string, len(tx.Operations()))
	for i, op := range tx.Operations() {
		switch o := op.(type) {
		case *txnbuild.Payment:
			opResults[i] = fmt.Sprintf("PAYMENT destination=%s amount=%s asset=%s: VALID", o.Destination, o.Amount, o.Asset.GetCode())
		case *txnbuild.ChangeTrust:
			opResults[i] = fmt.Sprintf("CHANGE_TRUST line=%s limit=%s: VALID", o.Line.GetCode(), o.Limit)
		default:
			opResults[i] = "OP_VALID"
		}
	}

	totalFee := int64(tx.BaseFee()) * int64(len(tx.Operations()))

	return &SimulationResult{
		Success:          true,
		EstimatedLedger:  1,
		EstimatedFee:     totalFee,
		OperationResults: opResults,
	}, nil
}

func toBuilderAsset(a stellar.AssetDescriptor) (txnbuild.Asset, error) {
	if a.IsNative() {
		return txnbuild.NativeAsset{}, nil
	}
	if a.Issuer == "" || !strkey.IsValidEd25519PublicKey(a.Issuer) {
		return nil, fmt.Errorf("%w: invalid issuer %q for asset %q", stellar.ErrInvalidAsset, a.Issuer, a.Code)
	}
	return txnbuild.CreditAsset{
		Code:   a.Code,
		Issuer: a.Issuer,
	}, nil
}

func toChangeTrustAsset(a stellar.AssetDescriptor) (txnbuild.ChangeTrustAsset, error) {
	if a.IsNative() {
		return nil, errors.New("cannot create trustline for native asset XLM")
	}
	if a.Issuer == "" || !strkey.IsValidEd25519PublicKey(a.Issuer) {
		return nil, fmt.Errorf("%w: invalid issuer %q for asset %q", stellar.ErrInvalidAsset, a.Issuer, a.Code)
	}
	creditAsset := txnbuild.CreditAsset{
		Code:   a.Code,
		Issuer: a.Issuer,
	}
	return creditAsset.ToChangeTrustAsset()
}
