package batch

import (
	"context"
	"errors"
	"testing"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/shopspring/decimal"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
)

const (
	sorobanTestContract = "CAAQEAYEAUDAOCAJBIFQYDIOB4IBCEQTCQKRMFYYDENBWHA5DYPSBFLM"
	sorobanTestAsset    = "CDEMPRWFYTB4FQOAX67L3PF3XK43RN5WWW2LHMVRWCX25LNMVOVKTGRU"
	sorobanTestSource   = "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"
)

type stubBatchSoroban struct {
	args      []xdr.ScVec
	functions []string
}

func (s *stubBatchSoroban) NetworkPassphrase() string { return "Test SDF Network ; September 2015" }

func (s *stubBatchSoroban) PrepareInvocation(_ context.Context, source string, op *txnbuild.InvokeHostFunction) (*txnbuild.Transaction, error) {
	s.functions = append(s.functions, string(op.HostFunction.InvokeContract.FunctionName))
	s.args = append(s.args, op.HostFunction.InvokeContract.Args)
	return txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &txnbuild.SimpleAccount{AccountID: source, Sequence: 1},
		IncrementSequenceNum: true,
		Operations:           []txnbuild.Operation{op},
		BaseFee:              txnbuild.MinBaseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(300)},
	})
}

func (s *stubBatchSoroban) SimulateInvocation(context.Context, string, *txnbuild.InvokeHostFunction) (xdr.ScVal, error) {
	return xdr.ScVal{}, errors.New("not used")
}

func (s *stubBatchSoroban) SubmitTransaction(context.Context, *txnbuild.Transaction) (string, error) {
	return "hash", nil
}

type passthroughSigner struct{}

func (passthroughSigner) Sign(tx *txnbuild.Transaction, _ string) (*txnbuild.Transaction, error) {
	return tx, nil
}

func sorobanTransfers(n int) []SorobanTransfer {
	transfers := make([]SorobanTransfer, n)
	for i := range transfers {
		transfers[i] = SorobanTransfer{Destination: sorobanTestSource, Amount: decimal.NewFromInt(1)}
	}
	return transfers
}

func newTestDispatcher(stub *stubBatchSoroban) *SorobanDispatcher {
	return NewSorobanDispatcher(stub, passthroughSigner{}, sorobanTestContract, sorobanTestSource, "secret")
}

func TestSorobanDispatchEncodesBatchTransferCall(t *testing.T) {
	stub := &stubBatchSoroban{}
	hashes, err := newTestDispatcher(stub).Dispatch(context.Background(), sorobanTestAsset, sorobanTransfers(2), SorobanModeAtomic)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if len(hashes) != 1 || stub.functions[0] != "batch_transfer" {
		t.Fatalf("expected one batch_transfer call, got hashes=%v functions=%v", hashes, stub.functions)
	}
	args := stub.args[0]
	if len(args) != 4 {
		t.Fatalf("expected 4 contract arguments, got %d", len(args))
	}
	vec, ok := args[2].GetVec()
	if !ok || vec == nil || len(*vec) != 2 {
		t.Fatalf("expected the transfers argument to hold 2 entries")
	}
}

func TestSorobanDispatchRejectsOversizedAtomicBatch(t *testing.T) {
	stub := &stubBatchSoroban{}
	_, err := newTestDispatcher(stub).Dispatch(context.Background(), sorobanTestAsset, sorobanTransfers(MaxItems+1), SorobanModeAtomic)
	if !errors.Is(err, domain.ErrBatchTooLarge) {
		t.Fatalf("expected ErrBatchTooLarge, got %v", err)
	}
	if len(stub.functions) != 0 {
		t.Fatal("nothing should be submitted for a rejected batch")
	}
}

func TestSorobanDispatchSplitsBestEffortBatches(t *testing.T) {
	stub := &stubBatchSoroban{}
	hashes, err := newTestDispatcher(stub).Dispatch(context.Background(), sorobanTestAsset, sorobanTransfers(MaxItems*2+5), SorobanModeBestEffort)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if len(hashes) != 3 {
		t.Fatalf("expected 3 invocations, got %d", len(hashes))
	}
	last, _ := stub.args[2][2].GetVec()
	if last == nil || len(*last) != 5 {
		t.Fatal("expected the last invocation to carry the 5 remaining transfers")
	}
}

func TestSorobanDispatchValidatesAmounts(t *testing.T) {
	stub := &stubBatchSoroban{}
	d := newTestDispatcher(stub)

	zero := []SorobanTransfer{{Destination: sorobanTestSource, Amount: decimal.Zero}}
	if _, err := d.Dispatch(context.Background(), sorobanTestAsset, zero, SorobanModeAtomic); !errors.Is(err, domain.ErrInvalidAmount) {
		t.Fatalf("expected ErrInvalidAmount, got %v", err)
	}

	fine := []SorobanTransfer{{Destination: sorobanTestSource, Amount: decimal.RequireFromString("0.00000001")}}
	if _, err := d.Dispatch(context.Background(), sorobanTestAsset, fine, SorobanModeAtomic); !errors.Is(err, domain.ErrSubPrecisionAmount) {
		t.Fatalf("expected ErrSubPrecisionAmount, got %v", err)
	}

	if _, err := d.Dispatch(context.Background(), sorobanTestAsset, nil, SorobanModeAtomic); !errors.Is(err, domain.ErrBatchEmpty) {
		t.Fatalf("expected ErrBatchEmpty, got %v", err)
	}
}
