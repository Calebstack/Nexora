package claimable

import (
	"errors"
	"math/big"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stellar/go/keypair"
)

func merkleTestClaims(t *testing.T, n int) []MerkleClaim {
	t.Helper()
	claims := make([]MerkleClaim, n)
	for i := range claims {
		claims[i] = MerkleClaim{
			Index:   uint32(i),
			Address: keypair.MustRandom().Address(),
			Amount:  big.NewInt(int64(100 * (i + 1))),
		}
	}
	return claims
}

func TestMerkleProofsVerifyForEveryLeaf(t *testing.T) {
	for _, n := range []int{1, 2, 3, 4, 5, 8, 13} {
		claims := merkleTestClaims(t, n)
		tree, err := BuildMerkleTree(claims)
		if err != nil {
			t.Fatalf("n=%d: build: %v", n, err)
		}
		for _, c := range claims {
			leaf, err := MerkleLeaf(c.Index, c.Address, c.Amount)
			if err != nil {
				t.Fatalf("n=%d: leaf: %v", n, err)
			}
			proof, err := tree.Proof(c.Index)
			if err != nil {
				t.Fatalf("n=%d: proof %d: %v", n, c.Index, err)
			}
			if !VerifyMerkleProof(tree.Root(), leaf, proof) {
				t.Errorf("n=%d: proof for index %d does not verify", n, c.Index)
			}
		}
	}
}

func TestMerkleProofRejectsTamperedClaim(t *testing.T) {
	claims := merkleTestClaims(t, 5)
	tree, err := BuildMerkleTree(claims)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := tree.Proof(2)
	if err != nil {
		t.Fatal(err)
	}
	tampered, err := MerkleLeaf(2, claims[2].Address, new(big.Int).Add(claims[2].Amount, big.NewInt(1)))
	if err != nil {
		t.Fatal(err)
	}
	if VerifyMerkleProof(tree.Root(), tampered, proof) {
		t.Fatal("proof verified for a tampered amount")
	}
}

func TestMerkleProofIndexOutOfRange(t *testing.T) {
	tree, err := BuildMerkleTree(merkleTestClaims(t, 3))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tree.Proof(3); !errors.Is(err, ErrProofIndexRange) {
		t.Fatalf("err = %v, want ErrProofIndexRange", err)
	}
}

func TestBuildMerkleTreeRejectsInvalidInput(t *testing.T) {
	if _, err := BuildMerkleTree(nil); !errors.Is(err, ErrEmptyMerkleTree) {
		t.Fatalf("empty: err = %v", err)
	}

	claims := merkleTestClaims(t, 2)
	claims[1].Index = 5
	if _, err := BuildMerkleTree(claims); !errors.Is(err, ErrInvalidMerkleClaim) {
		t.Fatalf("bad index: err = %v", err)
	}

	claims = merkleTestClaims(t, 1)
	claims[0].Amount = big.NewInt(0)
	if _, err := BuildMerkleTree(claims); !errors.Is(err, ErrInvalidMerkleClaim) {
		t.Fatalf("zero amount: err = %v", err)
	}

	claims = merkleTestClaims(t, 1)
	claims[0].Address = "not-an-address"
	if _, err := BuildMerkleTree(claims); !errors.Is(err, ErrInvalidMerkleClaim) {
		t.Fatalf("bad address: err = %v", err)
	}
}

func TestClaimsFromSnapshotIsDeterministicAndScaled(t *testing.T) {
	a, b := keypair.MustRandom().Address(), keypair.MustRandom().Address()
	snapshot := []BalanceSnapshot{
		{Address: b, Balance: decimal.RequireFromString("1.5")},
		{Address: a, Balance: decimal.RequireFromString("0.0000001")},
		{Address: keypair.MustRandom().Address(), Balance: decimal.Zero},
	}

	claims, err := ClaimsFromSnapshot(snapshot, 7)
	if err != nil {
		t.Fatalf("ClaimsFromSnapshot: %v", err)
	}
	if len(claims) != 2 {
		t.Fatalf("got %d claims, want 2 (zero balance dropped)", len(claims))
	}
	first, second := a, b
	if first > second {
		first, second = second, first
	}
	if claims[0].Address != first || claims[1].Address != second || claims[0].Index != 0 || claims[1].Index != 1 {
		t.Fatalf("claims not ordered by address with sequential indices: %+v", claims)
	}
	for _, c := range claims {
		want := int64(15_000_000)
		if c.Address == a {
			want = 1
		}
		if c.Amount.Int64() != want {
			t.Errorf("amount for %s = %s, want %d", c.Address, c.Amount, want)
		}
	}
}

func TestClaimsFromSnapshotRejectsBadBalances(t *testing.T) {
	addr := keypair.MustRandom().Address()
	for name, snapshot := range map[string][]BalanceSnapshot{
		"negative":        {{Address: addr, Balance: decimal.RequireFromString("-1")}},
		"too precise":     {{Address: addr, Balance: decimal.RequireFromString("0.00000001")}},
		"duplicate owner": {{Address: addr, Balance: decimal.NewFromInt(1)}, {Address: addr, Balance: decimal.NewFromInt(2)}},
	} {
		if _, err := ClaimsFromSnapshot(snapshot, 7); !errors.Is(err, ErrInvalidMerkleClaim) {
			t.Errorf("%s: err = %v, want ErrInvalidMerkleClaim", name, err)
		}
	}
}
