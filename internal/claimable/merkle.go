package claimable

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"sort"

	"github.com/Lumen-Nexora/Nexora/internal/stellar"
	"github.com/shopspring/decimal"
)

var (
	ErrEmptyMerkleTree    = errors.New("merkle tree needs at least one claim")
	ErrInvalidMerkleClaim = errors.New("invalid merkle claim")
	ErrProofIndexRange    = errors.New("merkle proof index out of range")
)

// MerkleClaim is one airdrop entitlement: the recipient may claim Amount (in
// the token's smallest unit) at position Index of the tree.
type MerkleClaim struct {
	Index   uint32
	Address string
	Amount  *big.Int
}

// BalanceSnapshot is one recipient's balance read from the database.
type BalanceSnapshot struct {
	Address string
	Balance decimal.Decimal
}

// MerkleLeaf hashes a claim exactly as the merkle_distributor contract does:
// sha256(index_be32 || xdr(address) || amount_be128). The address is encoded
// as the ScVal a Soroban Address serializes to.
func MerkleLeaf(index uint32, address string, amount *big.Int) ([32]byte, error) {
	if amount == nil || amount.Sign() <= 0 || amount.BitLen() > 127 {
		return [32]byte{}, fmt.Errorf("%w: amount must be a positive 127-bit integer", ErrInvalidMerkleClaim)
	}
	addr, err := stellar.AddressScVal(address)
	if err != nil {
		return [32]byte{}, fmt.Errorf("%w: %v", ErrInvalidMerkleClaim, err)
	}
	addrXDR, err := addr.MarshalBinary()
	if err != nil {
		return [32]byte{}, fmt.Errorf("encode address %s: %w", address, err)
	}

	var amountBytes [16]byte
	amount.FillBytes(amountBytes[:])

	var indexBytes [4]byte
	binary.BigEndian.PutUint32(indexBytes[:], index)

	h := sha256.New()
	h.Write(indexBytes[:])
	h.Write(addrXDR)
	h.Write(amountBytes[:])
	var leaf [32]byte
	copy(leaf[:], h.Sum(nil))
	return leaf, nil
}

// merkleParent hashes two siblings in sorted order, matching the contract, so
// proofs carry sibling hashes only.
func merkleParent(a, b [32]byte) [32]byte {
	if bytes.Compare(a[:], b[:]) > 0 {
		a, b = b, a
	}
	h := sha256.New()
	h.Write(a[:])
	h.Write(b[:])
	var parent [32]byte
	copy(parent[:], h.Sum(nil))
	return parent
}

// ClaimsFromSnapshot turns database balances into indexed claims. decimals is
// the token's decimal places (7 for Stellar assets). Zero balances are
// dropped; recipients are ordered by address so the same snapshot always
// yields the same tree.
func ClaimsFromSnapshot(snapshot []BalanceSnapshot, decimals int32) ([]MerkleClaim, error) {
	seen := make(map[string]struct{}, len(snapshot))
	entries := make([]BalanceSnapshot, 0, len(snapshot))
	for _, s := range snapshot {
		if s.Balance.IsNegative() {
			return nil, fmt.Errorf("%w: negative balance for %s", ErrInvalidMerkleClaim, s.Address)
		}
		if s.Balance.IsZero() {
			continue
		}
		if _, dup := seen[s.Address]; dup {
			return nil, fmt.Errorf("%w: duplicate address %s", ErrInvalidMerkleClaim, s.Address)
		}
		seen[s.Address] = struct{}{}
		entries = append(entries, s)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Address < entries[j].Address })

	claims := make([]MerkleClaim, 0, len(entries))
	for i, e := range entries {
		units := e.Balance.Shift(decimals)
		if !units.IsInteger() {
			return nil, fmt.Errorf("%w: balance %s for %s has more than %d decimals", ErrInvalidMerkleClaim, e.Balance, e.Address, decimals)
		}
		claims = append(claims, MerkleClaim{Index: uint32(i), Address: e.Address, Amount: units.BigInt()})
	}
	return claims, nil
}

// MerkleTree is a built tree over a set of claims.
type MerkleTree struct {
	layers [][][32]byte
}

// BuildMerkleTree builds the tree for claims, whose Index fields must be
// 0..n-1 in order. An odd node at the end of a level is promoted unchanged.
func BuildMerkleTree(claims []MerkleClaim) (*MerkleTree, error) {
	if len(claims) == 0 {
		return nil, ErrEmptyMerkleTree
	}

	leaves := make([][32]byte, len(claims))
	for i, c := range claims {
		if c.Index != uint32(i) {
			return nil, fmt.Errorf("%w: claim %d has index %d", ErrInvalidMerkleClaim, i, c.Index)
		}
		leaf, err := MerkleLeaf(c.Index, c.Address, c.Amount)
		if err != nil {
			return nil, err
		}
		leaves[i] = leaf
	}

	layers := [][][32]byte{leaves}
	for level := leaves; len(level) > 1; {
		next := make([][32]byte, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			if i+1 < len(level) {
				next = append(next, merkleParent(level[i], level[i+1]))
			} else {
				next = append(next, level[i])
			}
		}
		layers = append(layers, next)
		level = next
	}
	return &MerkleTree{layers: layers}, nil
}

// Root is the 32-byte value to pass to the contract constructor.
func (t *MerkleTree) Root() [32]byte {
	top := t.layers[len(t.layers)-1]
	return top[0]
}

// Leaf returns the leaf hash at index.
func (t *MerkleTree) Leaf(index uint32) ([32]byte, error) {
	if int(index) >= len(t.layers[0]) {
		return [32]byte{}, ErrProofIndexRange
	}
	return t.layers[0][index], nil
}

// Proof returns the sibling hashes, bottom to top, that prove the claim at
// index against Root.
func (t *MerkleTree) Proof(index uint32) ([][32]byte, error) {
	if int(index) >= len(t.layers[0]) {
		return nil, ErrProofIndexRange
	}
	var proof [][32]byte
	pos := int(index)
	for _, level := range t.layers[:len(t.layers)-1] {
		if sibling := pos ^ 1; sibling < len(level) {
			proof = append(proof, level[sibling])
		}
		pos /= 2
	}
	return proof, nil
}

// VerifyMerkleProof folds proof into leaf the way the contract does and
// compares the result to root.
func VerifyMerkleProof(root, leaf [32]byte, proof [][32]byte) bool {
	node := leaf
	for _, sibling := range proof {
		node = merkleParent(node, sibling)
	}
	return node == root
}
