package stellar

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/stellar/go/network"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
)

// The domain prefixes must match RELEASE_DOMAIN and ROTATE_DOMAIN in
// contracts/soroban/bridge_adapter/src/lib.rs: the contract verifies validator
// signatures over exactly the bytes built below.
const (
	bridgeReleaseDomain = "NEXORA_BRIDGE_RELEASE"
	bridgeRotateDomain  = "NEXORA_BRIDGE_ROTATE"
)

// BridgeSignature is one validator's Ed25519 signature over a bridge payload.
type BridgeSignature struct {
	Validator [32]byte
	Signature [64]byte
}

// InvokeContractOp builds an InvokeHostFunction operation that calls function
// on the contract with args.
func InvokeContractOp(contractID, function, sourceAccount string, args []xdr.ScVal) (*txnbuild.InvokeHostFunction, error) {
	id, err := ParseContractID(contractID)
	if err != nil {
		return nil, err
	}
	return &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &id,
				},
				FunctionName: xdr.ScSymbol(function),
				Args:         args,
			},
		},
		SourceAccount: sourceAccount,
	}, nil
}

// bridgePayloadPrefix is the part shared by every bridge payload: the domain,
// the network id and the bridge contract address.
func bridgePayloadPrefix(domain, networkPassphrase, contractID string) ([]byte, error) {
	contract, err := ContractScVal(contractID)
	if err != nil {
		return nil, err
	}
	contractXDR, err := contract.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("encode bridge contract address: %w", err)
	}

	networkID := network.ID(networkPassphrase)
	payload := append([]byte(domain), networkID[:]...)
	return append(payload, contractXDR...), nil
}

// BridgeReleasePayload returns the bytes validators sign to approve releasing
// amount to recipient under nonce.
func BridgeReleasePayload(networkPassphrase, contractID, recipient string, amount *big.Int, nonce uint64) ([]byte, error) {
	if amount.Sign() <= 0 || amount.BitLen() > 127 {
		return nil, fmt.Errorf("bridge amount %s out of range", amount)
	}

	payload, err := bridgePayloadPrefix(bridgeReleaseDomain, networkPassphrase, contractID)
	if err != nil {
		return nil, err
	}
	recipientVal, err := AddressScVal(recipient)
	if err != nil {
		return nil, err
	}
	recipientXDR, err := recipientVal.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("encode bridge recipient: %w", err)
	}

	payload = append(payload, recipientXDR...)
	payload = append(payload, amount.FillBytes(make([]byte, 16))...)
	return binary.BigEndian.AppendUint64(payload, nonce), nil
}

// BridgeRotationPayload returns the bytes the current validators sign to
// approve replacing the validator set and threshold under nonce.
func BridgeRotationPayload(networkPassphrase, contractID string, validators [][32]byte, threshold uint32, nonce uint64) ([]byte, error) {
	payload, err := bridgePayloadPrefix(bridgeRotateDomain, networkPassphrase, contractID)
	if err != nil {
		return nil, err
	}

	payload = binary.BigEndian.AppendUint32(payload, threshold)
	payload = binary.BigEndian.AppendUint64(payload, nonce)
	for _, validator := range validators {
		payload = append(payload, validator[:]...)
	}
	return payload, nil
}

func signatureScVal(sig BridgeSignature) xdr.ScVal {
	// Soroban encodes a contract struct as a map keyed by field name, sorted
	// by key.
	entries := xdr.ScMap{
		{Key: symbolScVal("signature"), Val: BytesScVal(sig.Signature[:])},
		{Key: symbolScVal("validator"), Val: BytesScVal(sig.Validator[:])},
	}
	entriesPtr := &entries
	return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &entriesPtr}
}

func symbolScVal(name string) xdr.ScVal {
	sym := xdr.ScSymbol(name)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

func signaturesScVal(sigs []BridgeSignature) xdr.ScVal {
	items := make([]xdr.ScVal, 0, len(sigs))
	for _, sig := range sigs {
		items = append(items, signatureScVal(sig))
	}
	return VecScVal(items)
}

// BridgeAdapterBinding calls the Soroban bridge adapter contract. Releases and
// rotations are permissionless on-chain, authorized only by validator
// signatures, so sourceAccount only pays the transaction fee.
type BridgeAdapterBinding struct {
	client          SorobanClient
	signer          Signer
	contractID      string
	sourceAccount   string
	encryptedSecret string
}

func NewBridgeAdapterBinding(client SorobanClient, signer Signer, contractID, sourceAccount, encryptedSecret string) *BridgeAdapterBinding {
	return &BridgeAdapterBinding{
		client:          client,
		signer:          signer,
		contractID:      contractID,
		sourceAccount:   sourceAccount,
		encryptedSecret: encryptedSecret,
	}
}

// Release submits a validator-attested release of amount to recipient and
// returns the transaction hash.
func (b *BridgeAdapterBinding) Release(ctx context.Context, recipient string, amount *big.Int, nonce uint64, sigs []BridgeSignature) (string, error) {
	recipientVal, err := AddressScVal(recipient)
	if err != nil {
		return "", err
	}
	return b.invoke(ctx, "release", []xdr.ScVal{
		recipientVal,
		I128ScVal(amount),
		U64ScVal(nonce),
		signaturesScVal(sigs),
	})
}

// RotateValidators submits a quorum-signed replacement of the validator set
// and returns the transaction hash.
func (b *BridgeAdapterBinding) RotateValidators(ctx context.Context, validators [][32]byte, threshold uint32, nonce uint64, sigs []BridgeSignature) (string, error) {
	set := make([]xdr.ScVal, 0, len(validators))
	for _, validator := range validators {
		set = append(set, BytesScVal(validator[:]))
	}
	return b.invoke(ctx, "rotate_validators", []xdr.ScVal{
		VecScVal(set),
		U32ScVal(threshold),
		U64ScVal(nonce),
		signaturesScVal(sigs),
	})
}

// IsNonceUsed reports whether a transfer nonce was already consumed on-chain.
func (b *BridgeAdapterBinding) IsNonceUsed(ctx context.Context, nonce uint64) (bool, error) {
	op, err := InvokeContractOp(b.contractID, "is_nonce_used", b.sourceAccount, []xdr.ScVal{U64ScVal(nonce)})
	if err != nil {
		return false, err
	}
	result, err := b.client.SimulateInvocation(ctx, b.sourceAccount, op)
	if err != nil {
		return false, fmt.Errorf("simulate bridge is_nonce_used: %w", err)
	}
	return DecodeBool(result)
}

// IsPaused reports whether the bridge emergency shutdown is active.
func (b *BridgeAdapterBinding) IsPaused(ctx context.Context) (bool, error) {
	op, err := InvokeContractOp(b.contractID, "is_paused", b.sourceAccount, nil)
	if err != nil {
		return false, err
	}
	result, err := b.client.SimulateInvocation(ctx, b.sourceAccount, op)
	if err != nil {
		return false, fmt.Errorf("simulate bridge is_paused: %w", err)
	}
	return DecodeBool(result)
}

func (b *BridgeAdapterBinding) invoke(ctx context.Context, function string, args []xdr.ScVal) (string, error) {
	op, err := InvokeContractOp(b.contractID, function, b.sourceAccount, args)
	if err != nil {
		return "", err
	}

	tx, err := b.client.PrepareInvocation(ctx, b.sourceAccount, op)
	if err != nil {
		return "", fmt.Errorf("prepare bridge %s: %w", function, err)
	}
	signed, err := b.signer.Sign(tx, b.encryptedSecret)
	if err != nil {
		return "", fmt.Errorf("sign bridge %s: %w", function, err)
	}
	hash, err := b.client.SubmitTransaction(ctx, signed)
	if err != nil {
		return "", fmt.Errorf("submit bridge %s: %w", function, err)
	}
	return hash, nil
}
