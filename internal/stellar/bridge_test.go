package stellar

import (
	"bytes"
	"context"
	"math/big"
	"testing"

	"github.com/stellar/go/network"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
)

type recordingSorobanClient struct {
	op       *txnbuild.InvokeHostFunction
	prepared int
	signed   int
	submit   int
	result   xdr.ScVal
}

func (c *recordingSorobanClient) PrepareInvocation(_ context.Context, sourceAccount string, op *txnbuild.InvokeHostFunction) (*txnbuild.Transaction, error) {
	c.op = op
	c.prepared++
	return txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &txnbuild.SimpleAccount{AccountID: sourceAccount, Sequence: 1},
		IncrementSequenceNum: true,
		Operations:           []txnbuild.Operation{op},
		BaseFee:              txnbuild.MinBaseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewInfiniteTimeout()},
	})
}

func (c *recordingSorobanClient) SimulateInvocation(_ context.Context, _ string, op *txnbuild.InvokeHostFunction) (xdr.ScVal, error) {
	c.op = op
	return c.result, nil
}

func (c *recordingSorobanClient) SubmitTransaction(context.Context, *txnbuild.Transaction) (string, error) {
	c.submit++
	return "tx-hash", nil
}

func (c *recordingSorobanClient) NetworkPassphrase() string { return network.TestNetworkPassphrase }

type countingSigner struct{ client *recordingSorobanClient }

func (s countingSigner) Sign(tx *txnbuild.Transaction, _ string) (*txnbuild.Transaction, error) {
	s.client.signed++
	return tx, nil
}

func testContractID(t *testing.T) string {
	t.Helper()
	id, err := EncodeContractID([32]byte{7})
	if err != nil {
		t.Fatalf("EncodeContractID: %v", err)
	}
	return id
}

func TestBridgeReleasePayloadLayout(t *testing.T) {
	contractID := testContractID(t)

	payload, err := BridgeReleasePayload(network.TestNetworkPassphrase, contractID, dstAccount, big.NewInt(500), 9)
	if err != nil {
		t.Fatalf("BridgeReleasePayload: %v", err)
	}

	networkID := network.ID(network.TestNetworkPassphrase)
	contractVal, _ := ContractScVal(contractID)
	contractXDR, _ := contractVal.MarshalBinary()
	recipientVal, _ := AddressScVal(dstAccount)
	recipientXDR, _ := recipientVal.MarshalBinary()

	want := []byte("NEXORA_BRIDGE_RELEASE")
	want = append(want, networkID[:]...)
	want = append(want, contractXDR...)
	want = append(want, recipientXDR...)
	want = append(want, make([]byte, 14)...)
	want = append(want, 0x01, 0xf4) // 500 as a big-endian i128
	want = append(want, 0, 0, 0, 0, 0, 0, 0, 9)
	if !bytes.Equal(payload, want) {
		t.Fatalf("payload mismatch\n got %x\nwant %x", payload, want)
	}
}

func TestBridgeReleasePayloadRejectsBadAmounts(t *testing.T) {
	contractID := testContractID(t)
	for _, amount := range []*big.Int{big.NewInt(0), big.NewInt(-1), new(big.Int).Lsh(big.NewInt(1), 127)} {
		if _, err := BridgeReleasePayload(network.TestNetworkPassphrase, contractID, dstAccount, amount, 1); err == nil {
			t.Errorf("amount %s: expected an error", amount)
		}
	}
}

func TestBridgeRotationPayloadLayout(t *testing.T) {
	contractID := testContractID(t)
	validators := [][32]byte{{1}, {2}}

	payload, err := BridgeRotationPayload(network.TestNetworkPassphrase, contractID, validators, 2, 3)
	if err != nil {
		t.Fatalf("BridgeRotationPayload: %v", err)
	}

	tail := payload[len(payload)-(4+8+64):]
	if !bytes.Equal(tail[:4], []byte{0, 0, 0, 2}) || !bytes.Equal(tail[4:12], []byte{0, 0, 0, 0, 0, 0, 0, 3}) {
		t.Fatalf("threshold or nonce misplaced: %x", tail)
	}
	if tail[12] != 1 || tail[12+32] != 2 {
		t.Fatalf("validators misplaced: %x", tail)
	}
	if !bytes.HasPrefix(payload, []byte("NEXORA_BRIDGE_ROTATE")) {
		t.Fatalf("missing domain prefix: %x", payload)
	}
}

func TestBridgeReleaseSignsAndSubmits(t *testing.T) {
	client := &recordingSorobanClient{}
	binding := NewBridgeAdapterBinding(client, countingSigner{client}, testContractID(t), srcAccount, "secret")
	sigs := []BridgeSignature{{Validator: [32]byte{1}, Signature: [64]byte{2}}}

	hash, err := binding.Release(context.Background(), dstAccount, big.NewInt(500), 9, sigs)
	if err != nil {
		t.Fatalf("Release: %v", err)
	}

	if hash != "tx-hash" || client.prepared != 1 || client.signed != 1 || client.submit != 1 {
		t.Fatalf("unexpected flow: hash=%s prepared=%d signed=%d submit=%d", hash, client.prepared, client.signed, client.submit)
	}
	invoke := client.op.HostFunction.InvokeContract
	if string(invoke.FunctionName) != "release" || len(invoke.Args) != 4 {
		t.Fatalf("unexpected invocation: %s with %d args", invoke.FunctionName, len(invoke.Args))
	}
	vec, ok := invoke.Args[3].GetVec()
	if !ok || vec == nil || len(*vec) != 1 {
		t.Fatalf("signatures not encoded as a one-element vec: %+v", invoke.Args[3])
	}
	if _, err := DecodeMapField((*vec)[0], "validator"); err != nil {
		t.Fatalf("signature struct missing validator: %v", err)
	}
	if _, err := DecodeMapField((*vec)[0], "signature"); err != nil {
		t.Fatalf("signature struct missing signature: %v", err)
	}
}

func TestBridgeIsPausedDecodesSimulation(t *testing.T) {
	yes := true
	client := &recordingSorobanClient{result: xdr.ScVal{Type: xdr.ScValTypeScvBool, B: &yes}}
	binding := NewBridgeAdapterBinding(client, countingSigner{client}, testContractID(t), srcAccount, "secret")

	paused, err := binding.IsPaused(context.Background())
	if err != nil || !paused {
		t.Fatalf("IsPaused = %v, %v; want true", paused, err)
	}
	if client.prepared != 0 || client.submit != 0 {
		t.Fatal("a read must not prepare or submit a transaction")
	}
}
