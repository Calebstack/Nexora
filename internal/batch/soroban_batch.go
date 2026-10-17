package batch

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/stellar"
	"github.com/shopspring/decimal"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
)

// SorobanMode selects how the batch_transfer contract treats a failing
// transfer. The values are the contract's BatchMode variant names.
type SorobanMode string

const (
	// SorobanModeAtomic reverts the whole batch if any transfer fails.
	SorobanModeAtomic SorobanMode = "Atomic"
	// SorobanModeBestEffort pays the valid transfers and logs the failures.
	SorobanModeBestEffort SorobanMode = "BestEffort"
)

// SorobanTransfer is one payout inside a contract batch.
type SorobanTransfer struct {
	Destination string
	Amount      decimal.Decimal
}

// SorobanDispatcher sends payouts from the batch queue through the
// batch_transfer contract, so up to MaxItems payouts settle in one
// transaction instead of one transaction per item.
type SorobanDispatcher struct {
	soroban         stellar.SorobanClient
	signer          stellar.Signer
	contractID      string
	sourceAccount   string
	encryptedSecret string
}

func NewSorobanDispatcher(
	soroban stellar.SorobanClient,
	signer stellar.Signer,
	contractID, sourceAccount, encryptedSecret string,
) *SorobanDispatcher {
	return &SorobanDispatcher{
		soroban:         soroban,
		signer:          signer,
		contractID:      contractID,
		sourceAccount:   sourceAccount,
		encryptedSecret: encryptedSecret,
	}
}

// Dispatch submits transfers of the asset behind assetContract and returns
// one transaction hash per contract invocation. Atomic mode needs the whole
// batch in a single invocation, so it is limited to MaxItems; best-effort
// mode is split into MaxItems-sized invocations.
func (d *SorobanDispatcher) Dispatch(ctx context.Context, assetContract string, transfers []SorobanTransfer, mode SorobanMode) ([]string, error) {
	if len(transfers) == 0 {
		return nil, domain.ErrBatchEmpty
	}
	if mode != SorobanModeAtomic && mode != SorobanModeBestEffort {
		return nil, fmt.Errorf("unknown soroban batch mode %q", mode)
	}
	if mode == SorobanModeAtomic && len(transfers) > MaxItems {
		return nil, domain.ErrBatchTooLarge
	}
	for i, t := range transfers {
		if t.Amount.LessThanOrEqual(decimal.Zero) {
			return nil, fmt.Errorf("transfer %d: %w", i, domain.ErrInvalidAmount)
		}
		if t.Amount.Exponent() < -7 {
			return nil, fmt.Errorf("transfer %d: %w", i, domain.ErrSubPrecisionAmount)
		}
	}
	if d.signer == nil {
		return nil, domain.ErrNoContractSigner
	}

	var hashes []string
	for start := 0; start < len(transfers); start += MaxItems {
		end := start + MaxItems
		if end > len(transfers) {
			end = len(transfers)
		}
		hash, err := d.invoke(ctx, assetContract, transfers[start:end], mode)
		if err != nil {
			return hashes, err
		}
		hashes = append(hashes, hash)
	}
	return hashes, nil
}

func (d *SorobanDispatcher) invoke(ctx context.Context, assetContract string, chunk []SorobanTransfer, mode SorobanMode) (string, error) {
	args, err := buildBatchArgs(d.sourceAccount, assetContract, chunk, mode)
	if err != nil {
		return "", err
	}

	contractID, err := stellar.ParseContractID(d.contractID)
	if err != nil {
		return "", err
	}
	op := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &contractID,
				},
				FunctionName: xdr.ScSymbol("batch_transfer"),
				Args:         args,
			},
		},
		SourceAccount: d.sourceAccount,
	}

	tx, err := d.soroban.PrepareInvocation(ctx, d.sourceAccount, op)
	if err != nil {
		return "", fmt.Errorf("prepare batch_transfer invocation: %w", err)
	}
	signed, err := d.signer.Sign(tx, d.encryptedSecret)
	if err != nil {
		return "", fmt.Errorf("sign batch_transfer invocation: %w", err)
	}
	hash, err := d.soroban.SubmitTransaction(ctx, signed)
	if err != nil {
		return "", fmt.Errorf("submit batch_transfer invocation: %w", err)
	}
	return hash, nil
}

// buildBatchArgs encodes (sender, asset, transfers, mode) in the order the
// contract's batch_transfer function declares them.
func buildBatchArgs(sender, assetContract string, chunk []SorobanTransfer, mode SorobanMode) (xdr.ScVec, error) {
	senderArg, err := stellar.AddressScVal(sender)
	if err != nil {
		return nil, err
	}
	assetArg, err := stellar.ContractScVal(assetContract)
	if err != nil {
		return nil, err
	}

	items := make([]xdr.ScVal, 0, len(chunk))
	for _, t := range chunk {
		to, err := stellar.AddressScVal(t.Destination)
		if err != nil {
			return nil, err
		}
		stroops := t.Amount.Shift(7).Truncate(0).BigInt()
		if stroops.Sign() <= 0 {
			return nil, errors.New("transfer amount rounds to zero stroops")
		}
		items = append(items, stellar.StructScVal(
			[]string{"amount", "to"},
			[]xdr.ScVal{stellar.I128ScVal(new(big.Int).Set(stroops)), to},
		))
	}

	modeArg := stellar.VecScVal([]xdr.ScVal{stellar.SymbolScVal(string(mode))})

	return xdr.ScVec{senderArg, assetArg, stellar.VecScVal(items), modeArg}, nil
}
