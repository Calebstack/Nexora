package stellar

import (
	"fmt"
	"math/big"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// AccountScVal encodes a G... account address as a contract argument.
func AccountScVal(accountID string) (xdr.ScVal, error) {
	acct, err := xdr.AddressToAccountId(accountID)
	if err != nil {
		return xdr.ScVal{}, fmt.Errorf("encode account address %s: %w", accountID, err)
	}
	return xdr.ScVal{
		Type: xdr.ScValTypeScvAddress,
		Address: &xdr.ScAddress{
			Type:      xdr.ScAddressTypeScAddressTypeAccount,
			AccountId: &acct,
		},
	}, nil
}

// ContractScVal encodes a C... contract address as a contract argument.
func ContractScVal(contractID string) (xdr.ScVal, error) {
	id, err := ParseContractID(contractID)
	if err != nil {
		return xdr.ScVal{}, err
	}
	return xdr.ScVal{
		Type: xdr.ScValTypeScvAddress,
		Address: &xdr.ScAddress{
			Type:       xdr.ScAddressTypeScAddressTypeContract,
			ContractId: &id,
		},
	}, nil
}

// AddressScVal encodes either a G... account or a C... contract address,
// dispatching on the strkey prefix.
func AddressScVal(address string) (xdr.ScVal, error) {
	if strkey.IsValidContractAddress(address) {
		return ContractScVal(address)
	}
	return AccountScVal(address)
}

// EncodeContractID renders raw contract id bytes as a C... strkey.
func EncodeContractID(id [32]byte) (string, error) {
	encoded, err := strkey.Encode(strkey.VersionByteContract, id[:])
	if err != nil {
		return "", fmt.Errorf("encode contract id: %w", err)
	}
	return encoded, nil
}

// ParseContractID decodes a C... strkey into raw contract id bytes.
func ParseContractID(contractID string) (xdr.ContractId, error) {
	decoded, err := strkey.Decode(strkey.VersionByteContract, contractID)
	if err != nil {
		return xdr.ContractId{}, fmt.Errorf("decode contract id %s: %w", contractID, err)
	}
	var id xdr.ContractId
	copy(id[:], decoded)
	return id, nil
}

// I128ScVal encodes a signed 128-bit integer contract argument.
func I128ScVal(v *big.Int) xdr.ScVal {
	parts := xdr.Int128Parts{
		Hi: xdr.Int64(new(big.Int).Rsh(v, 64).Int64()),
		Lo: xdr.Uint64(new(big.Int).And(v, maskLow64).Uint64()),
	}
	return xdr.ScVal{Type: xdr.ScValTypeScvI128, I128: &parts}
}

var maskLow64 = new(big.Int).SetUint64(^uint64(0))

// StringScVal encodes a UTF-8 string contract argument.
func StringScVal(s string) xdr.ScVal {
	str := xdr.ScString(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &str}
}

// U32ScVal encodes an unsigned 32-bit integer contract argument.
func U32ScVal(v uint32) xdr.ScVal {
	u := xdr.Uint32(v)
	return xdr.ScVal{Type: xdr.ScValTypeScvU32, U32: &u}
}

// BytesScVal encodes a variable-length bytes contract argument. BytesN<N>
// arguments use the same encoding.
func BytesScVal(b []byte) xdr.ScVal {
	v := xdr.ScBytes(append([]byte(nil), b...))
	return xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &v}
}

// VecScVal encodes a Vec contract argument.
func VecScVal(items []xdr.ScVal) xdr.ScVal {
	vec := xdr.ScVec(items)
	vecPtr := &vec
	return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vecPtr}
}

// VoidScVal encodes the `None` variant of a contract `Option<T>` argument.
func VoidScVal() xdr.ScVal {
	return xdr.ScVal{Type: xdr.ScValTypeScvVoid}
}

// U64ScVal encodes an unsigned 64-bit integer contract argument.
func U64ScVal(v uint64) xdr.ScVal {
	u := xdr.Uint64(v)
	return xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &u}
}

// SymbolScVal encodes a symbol contract argument, such as a unit enum variant.
func SymbolScVal(s string) xdr.ScVal {
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

// VecScVal encodes a contract Vec argument.
func VecScVal(items []xdr.ScVal) xdr.ScVal {
	vec := xdr.ScVec(items)
	vecPtr := &vec
	return xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vecPtr}
}

// StructScVal encodes a contract struct argument. Soroban requires map keys
// in sorted order, so fields must be passed sorted by name.
func StructScVal(names []string, values []xdr.ScVal) xdr.ScVal {
	entries := make(xdr.ScMap, 0, len(names))
	for i, name := range names {
		entries = append(entries, xdr.ScMapEntry{Key: SymbolScVal(name), Val: values[i]})
	}
	mapPtr := &entries
	return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &mapPtr}
}

// BytesScVal encodes a Bytes/BytesN contract argument.
func BytesScVal(b []byte) xdr.ScVal {
	bytes := xdr.ScBytes(b)
	return xdr.ScVal{Type: xdr.ScValTypeScvBytes, Bytes: &bytes}
}

// DecodeI128 reads a signed 128-bit integer out of a contract return value.
func DecodeI128(v xdr.ScVal) (*big.Int, error) {
	parts, ok := v.GetI128()
	if !ok {
		return nil, fmt.Errorf("expected i128 value, got %s", v.Type)
	}
	result := new(big.Int).SetInt64(int64(parts.Hi))
	result.Lsh(result, 64)
	result.Or(result, new(big.Int).SetUint64(uint64(parts.Lo)))
	return result, nil
}

// DecodeU64 reads an unsigned 64-bit integer out of a contract return value.
func DecodeU64(v xdr.ScVal) (uint64, error) {
	u, ok := v.GetU64()
	if !ok {
		return 0, fmt.Errorf("expected u64 value, got %s", v.Type)
	}
	return uint64(u), nil
}

// DecodeU32 reads an unsigned 32-bit integer out of a contract return value.
func DecodeU32(v xdr.ScVal) (uint32, error) {
	u, ok := v.GetU32()
	if !ok {
		return 0, fmt.Errorf("expected u32 value, got %s", v.Type)
	}
	return uint32(u), nil
}

// DecodeBool reads a boolean out of a contract return value.
func DecodeBool(v xdr.ScVal) (bool, error) {
	b, ok := v.GetB()
	if !ok {
		return false, fmt.Errorf("expected bool value, got %s", v.Type)
	}
	return b, nil
}

// DecodeAddress renders a contract address return value back to strkey form.
func DecodeAddress(v xdr.ScVal) (string, error) {
	addr, ok := v.GetAddress()
	if !ok {
		return "", fmt.Errorf("expected address value, got %s", v.Type)
	}
	encoded, err := addr.String()
	if err != nil {
		return "", fmt.Errorf("encode address: %w", err)
	}
	return encoded, nil
}

// DecodeAddressVec renders a contract Vec<Address> return value to strkeys.
func DecodeAddressVec(v xdr.ScVal) ([]string, error) {
	vec, ok := v.GetVec()
	if !ok || vec == nil {
		return nil, fmt.Errorf("expected vec value, got %s", v.Type)
	}
	addresses := make([]string, 0, len(*vec))
	for _, item := range *vec {
		addr, err := DecodeAddress(item)
		if err != nil {
			return nil, err
		}
		addresses = append(addresses, addr)
	}
	return addresses, nil
}

// DecodeMapField pulls a single named field out of a contract struct return
// value, which Soroban encodes as an ScMap keyed by symbol.
func DecodeMapField(v xdr.ScVal, field string) (xdr.ScVal, error) {
	m, ok := v.GetMap()
	if !ok || m == nil {
		return xdr.ScVal{}, fmt.Errorf("expected map value, got %s", v.Type)
	}
	for _, entry := range *m {
		sym, ok := entry.Key.GetSym()
		if ok && string(sym) == field {
			return entry.Val, nil
		}
	}
	return xdr.ScVal{}, fmt.Errorf("field %q missing from contract return value", field)
}
