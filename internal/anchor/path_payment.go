package anchor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/stellar"
	"github.com/shopspring/decimal"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/txnbuild"
)

var (
	ErrDestMinViolation = errors.New("expected destination amount is less than specified dest_min")
	ErrSendMaxViolation = errors.New("required source amount exceeds specified send_max")
	ErrInvalidAccount   = errors.New("invalid account public key")
	ErrAccountLoad      = errors.New("failed to load source account from horizon")
)

// PathPaymentResult contains execution metadata of a submitted path payment.
type PathPaymentResult struct {
	TxHash       string                    `json:"tx_hash"`
	Ledger       int32                     `json:"ledger"`
	SourceAsset  stellar.AssetDescriptor   `json:"source_asset"`
	SourceAmount decimal.Decimal           `json:"source_amount"`
	DestAsset    stellar.AssetDescriptor   `json:"destination_asset"`
	DestAmount   decimal.Decimal           `json:"destination_amount"`
	PathUsed     []stellar.AssetDescriptor `json:"path_used"`
	HopCount     int                       `json:"hop_count"`
	ExecutedAt   time.Time                 `json:"executed_at"`
}

// StrictSendExecutionRequest configures a strict send path payment.
type StrictSendExecutionRequest struct {
	SourceSecretKey      string
	SourceAccount        string
	SourceAsset          stellar.AssetDescriptor
	SourceAmount         decimal.Decimal
	DestinationAccount   string
	DestinationAsset     stellar.AssetDescriptor
	DestMin              decimal.Decimal // Minimum acceptable receive amount
	MaxSlippageTolerance decimal.Decimal // Allowed price movement (e.g. 0.01 for 1%)
	ExplicitPath         []stellar.AssetDescriptor
	Memo                 string
}

// StrictReceiveExecutionRequest configures a strict receive path payment.
type StrictReceiveExecutionRequest struct {
	SourceSecretKey      string
	SourceAccount        string
	SourceAsset          stellar.AssetDescriptor
	SendMax              decimal.Decimal // Maximum acceptable spend amount
	DestinationAccount   string
	DestinationAsset     stellar.AssetDescriptor
	DestAmount           decimal.Decimal
	MaxSlippageTolerance decimal.Decimal
	ExplicitPath         []stellar.AssetDescriptor
	Memo                 string
}

// PathPaymentEngine executes atomic PathPaymentStrictSend and PathPaymentStrictReceive transactions.
type PathPaymentEngine struct {
	pathFinder        stellar.PathFinder
	client            stellar.Client
	networkPassphrase string
	baseFee           int64
}

// NewPathPaymentEngine builds a new PathPaymentEngine.
func NewPathPaymentEngine(pathFinder stellar.PathFinder, client stellar.Client, networkName string) *PathPaymentEngine {
	passphrase := network.TestNetworkPassphrase
	if networkName == "mainnet" || networkName == "public" {
		passphrase = network.PublicNetworkPassphrase
	}
	return &PathPaymentEngine{
		pathFinder:        pathFinder,
		client:            client,
		networkPassphrase: passphrase,
		baseFee:           txnbuild.MinBaseFee,
	}
}

// SetBaseFee overrides the base transaction fee in stroops.
func (e *PathPaymentEngine) SetBaseFee(fee int64) {
	if fee >= txnbuild.MinBaseFee {
		e.baseFee = fee
	}
}

// ExecuteStrictSend executes an optimal PathPaymentStrictSend operation.
func (e *PathPaymentEngine) ExecuteStrictSend(ctx context.Context, req StrictSendExecutionRequest) (*PathPaymentResult, error) {
	if !strkey.IsValidEd25519PublicKey(req.SourceAccount) {
		return nil, fmt.Errorf("%w: invalid source account", ErrInvalidAccount)
	}
	if !strkey.IsValidEd25519PublicKey(req.DestinationAccount) {
		return nil, fmt.Errorf("%w: invalid destination account", ErrInvalidAccount)
	}
	if req.SourceAmount.LessThanOrEqual(decimal.Zero) {
		return nil, stellar.ErrInvalidAmount
	}

	var chosenPath stellar.PaymentPath

	// 1. Path discovery if explicit path is not supplied
	if len(req.ExplicitPath) == 0 && req.SourceAsset.CanonicalString() != req.DestinationAsset.CanonicalString() {
		paths, err := e.pathFinder.FindStrictSendPaths(ctx, stellar.StrictSendRequest{
			SourceAccount:      req.SourceAccount,
			SourceAsset:        req.SourceAsset,
			SourceAmount:       req.SourceAmount,
			DestinationAccount: req.DestinationAccount,
		})
		if err != nil {
			return nil, fmt.Errorf("find strict-send paths: %w", err)
		}

		best, err := e.pathFinder.SelectBestStrictSendPath(paths)
		if err != nil {
			return nil, err
		}
		chosenPath = *best
	} else {
		chosenPath = stellar.PaymentPath{
			SourceAsset:  req.SourceAsset,
			SourceAmount: req.SourceAmount,
			DestAsset:    req.DestinationAsset,
			DestAmount:   req.DestMin,
			Path:         req.ExplicitPath,
			HopCount:     len(req.ExplicitPath),
		}
	}

	// 2. Validate DestMin limit
	if req.DestMin.GreaterThan(decimal.Zero) && chosenPath.DestAmount.LessThan(req.DestMin) {
		return nil, fmt.Errorf("%w: expected %s < dest_min %s", ErrDestMinViolation, chosenPath.DestAmount, req.DestMin)
	}

	// 3. Pre-submission orderbook depth assessment to prevent path collapse
	if e.pathFinder != nil && len(chosenPath.Path) > 0 {
		assessment, err := e.pathFinder.AssessPathDepth(ctx, chosenPath, req.MaxSlippageTolerance)
		if err != nil {
			return nil, fmt.Errorf("depth assessment failed: %w", err)
		}
		if !assessment.SufficientDepth {
			return nil, fmt.Errorf("%w: %s", stellar.ErrPathCollapsed, assessment.WarningMessage)
		}
	}

	// 4. Construct txnbuild assets
	sendAsset, err := toTxnAsset(chosenPath.SourceAsset)
	if err != nil {
		return nil, err
	}

	destAsset, err := toTxnAsset(chosenPath.DestAsset)
	if err != nil {
		return nil, err
	}

	pathHops := make([]txnbuild.Asset, 0, len(chosenPath.Path))
	for _, hop := range chosenPath.Path {
		txHop, err := toTxnAsset(hop)
		if err != nil {
			return nil, err
		}
		pathHops = append(pathHops, txHop)
	}

	destMinStr := req.DestMin.StringFixed(7)
	if req.DestMin.LessThanOrEqual(decimal.Zero) {
		// Default 1% slippage buffer if dest_min not explicitly given
		destMinStr = chosenPath.DestAmount.Mul(decimal.NewFromFloat(0.99)).StringFixed(7)
	}

	op := &txnbuild.PathPaymentStrictSend{
		SendAsset:   sendAsset,
		SendAmount:  req.SourceAmount.StringFixed(7),
		Destination: req.DestinationAccount,
		DestAsset:   destAsset,
		DestMin:     destMinStr,
		Path:        pathHops,
	}

	// 5. Build, sign, and submit transaction
	sourceAccount, err := e.client.LoadAccount(req.SourceAccount)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAccountLoad, err)
	}

	kp, err := keypair.ParseFull(req.SourceSecretKey)
	if err != nil {
		return nil, fmt.Errorf("parse source secret key: %w", err)
	}

	txParams := txnbuild.TransactionParams{
		SourceAccount:        &sourceAccount,
		Operations:           []txnbuild.Operation{op},
		BaseFee:              e.baseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(300)},
		IncrementSequenceNum: true,
	}

	if req.Memo != "" {
		txParams.Memo = txnbuild.MemoText(req.Memo)
	}

	tx, err := txnbuild.NewTransaction(txParams)
	if err != nil {
		return nil, fmt.Errorf("construct transaction: %w", err)
	}

	signedTx, err := tx.Sign(e.networkPassphrase, kp)
	if err != nil {
		return nil, fmt.Errorf("sign transaction: %w", err)
	}

	resp, err := e.client.SubmitTransaction(signedTx)
	if err != nil {
		return nil, fmt.Errorf("submit path payment transaction: %w", err)
	}

	return &PathPaymentResult{
		TxHash:       resp.Hash,
		Ledger:       resp.Ledger,
		SourceAsset:  chosenPath.SourceAsset,
		SourceAmount: req.SourceAmount,
		DestAsset:    chosenPath.DestAsset,
		DestAmount:   chosenPath.DestAmount,
		PathUsed:     chosenPath.Path,
		HopCount:     len(chosenPath.Path),
		ExecutedAt:   time.Now().UTC(),
	}, nil
}

// ExecuteStrictReceive executes an optimal PathPaymentStrictReceive operation.
func (e *PathPaymentEngine) ExecuteStrictReceive(ctx context.Context, req StrictReceiveExecutionRequest) (*PathPaymentResult, error) {
	if !strkey.IsValidEd25519PublicKey(req.SourceAccount) {
		return nil, fmt.Errorf("%w: invalid source account", ErrInvalidAccount)
	}
	if !strkey.IsValidEd25519PublicKey(req.DestinationAccount) {
		return nil, fmt.Errorf("%w: invalid destination account", ErrInvalidAccount)
	}
	if req.DestAmount.LessThanOrEqual(decimal.Zero) {
		return nil, stellar.ErrInvalidAmount
	}

	var chosenPath stellar.PaymentPath

	// 1. Path discovery if explicit path is not supplied
	if len(req.ExplicitPath) == 0 && req.SourceAsset.CanonicalString() != req.DestinationAsset.CanonicalString() {
		paths, err := e.pathFinder.FindStrictReceivePaths(ctx, stellar.StrictReceiveRequest{
			SourceAccount: req.SourceAccount,
			DestAccount:   req.DestinationAccount,
			DestAsset:     req.DestinationAsset,
			DestAmount:    req.DestAmount,
		})
		if err != nil {
			return nil, fmt.Errorf("find strict-receive paths: %w", err)
		}

		best, err := e.pathFinder.SelectBestStrictReceivePath(paths)
		if err != nil {
			return nil, err
		}
		chosenPath = *best
	} else {
		chosenPath = stellar.PaymentPath{
			SourceAsset:  req.SourceAsset,
			SourceAmount: req.SendMax,
			DestAsset:    req.DestinationAsset,
			DestAmount:   req.DestAmount,
			Path:         req.ExplicitPath,
			HopCount:     len(req.ExplicitPath),
		}
	}

	// 2. Validate SendMax limit
	if req.SendMax.GreaterThan(decimal.Zero) && chosenPath.SourceAmount.GreaterThan(req.SendMax) {
		return nil, fmt.Errorf("%w: required %s > send_max %s", ErrSendMaxViolation, chosenPath.SourceAmount, req.SendMax)
	}

	// 3. Pre-submission orderbook depth assessment to prevent path collapse
	if e.pathFinder != nil && len(chosenPath.Path) > 0 {
		assessment, err := e.pathFinder.AssessPathDepth(ctx, chosenPath, req.MaxSlippageTolerance)
		if err != nil {
			return nil, fmt.Errorf("depth assessment failed: %w", err)
		}
		if !assessment.SufficientDepth {
			return nil, fmt.Errorf("%w: %s", stellar.ErrPathCollapsed, assessment.WarningMessage)
		}
	}

	// 4. Construct txnbuild assets
	sendAsset, err := toTxnAsset(chosenPath.SourceAsset)
	if err != nil {
		return nil, err
	}

	destAsset, err := toTxnAsset(chosenPath.DestAsset)
	if err != nil {
		return nil, err
	}

	pathHops := make([]txnbuild.Asset, 0, len(chosenPath.Path))
	for _, hop := range chosenPath.Path {
		txHop, err := toTxnAsset(hop)
		if err != nil {
			return nil, err
		}
		pathHops = append(pathHops, txHop)
	}

	sendMaxStr := req.SendMax.StringFixed(7)
	if req.SendMax.LessThanOrEqual(decimal.Zero) {
		// Default 1% slippage buffer if send_max not explicitly given
		sendMaxStr = chosenPath.SourceAmount.Mul(decimal.NewFromFloat(1.01)).StringFixed(7)
	}

	op := &txnbuild.PathPaymentStrictReceive{
		SendAsset:   sendAsset,
		SendMax:     sendMaxStr,
		Destination: req.DestinationAccount,
		DestAsset:   destAsset,
		DestAmount:  req.DestAmount.StringFixed(7),
		Path:        pathHops,
	}

	// 5. Build, sign, and submit transaction
	sourceAccount, err := e.client.LoadAccount(req.SourceAccount)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAccountLoad, err)
	}

	kp, err := keypair.ParseFull(req.SourceSecretKey)
	if err != nil {
		return nil, fmt.Errorf("parse source secret key: %w", err)
	}

	txParams := txnbuild.TransactionParams{
		SourceAccount:        &sourceAccount,
		Operations:           []txnbuild.Operation{op},
		BaseFee:              e.baseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(300)},
		IncrementSequenceNum: true,
	}

	if req.Memo != "" {
		txParams.Memo = txnbuild.MemoText(req.Memo)
	}

	tx, err := txnbuild.NewTransaction(txParams)
	if err != nil {
		return nil, fmt.Errorf("construct transaction: %w", err)
	}

	signedTx, err := tx.Sign(e.networkPassphrase, kp)
	if err != nil {
		return nil, fmt.Errorf("sign transaction: %w", err)
	}

	resp, err := e.client.SubmitTransaction(signedTx)
	if err != nil {
		return nil, fmt.Errorf("submit path payment transaction: %w", err)
	}

	return &PathPaymentResult{
		TxHash:       resp.Hash,
		Ledger:       resp.Ledger,
		SourceAsset:  chosenPath.SourceAsset,
		SourceAmount: chosenPath.SourceAmount,
		DestAsset:    chosenPath.DestAsset,
		DestAmount:   req.DestAmount,
		PathUsed:     chosenPath.Path,
		HopCount:     len(chosenPath.Path),
		ExecutedAt:   time.Now().UTC(),
	}, nil
}

func toTxnAsset(a stellar.AssetDescriptor) (txnbuild.Asset, error) {
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
