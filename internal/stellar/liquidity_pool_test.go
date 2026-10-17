package stellar

import (
	"errors"
	"math/big"
	"testing"
)

func TestQuoteSwapMatchesConstantProductWithFee(t *testing.T) {
	// 1000 in against 100000/100000 at 30 bps:
	// 1000*9970*100000 / (100000*10000 + 1000*9970) = 987
	out, err := QuoteSwap(big.NewInt(100_000), big.NewInt(100_000), big.NewInt(1_000), 30, nil)
	if err != nil {
		t.Fatalf("QuoteSwap: %v", err)
	}
	if out.Cmp(big.NewInt(987)) != 0 {
		t.Fatalf("expected 987, got %s", out)
	}
}

func TestQuoteSwapRejectsSlippageAndEmptyPools(t *testing.T) {
	_, err := QuoteSwap(big.NewInt(100_000), big.NewInt(100_000), big.NewInt(1_000), 30, big.NewInt(988))
	if !errors.Is(err, ErrPoolSlippage) {
		t.Fatalf("expected ErrPoolSlippage, got %v", err)
	}

	_, err = QuoteSwap(big.NewInt(0), big.NewInt(100_000), big.NewInt(1_000), 30, nil)
	if !errors.Is(err, ErrPoolImbalance) {
		t.Fatalf("expected ErrPoolImbalance, got %v", err)
	}
}

func TestPoolCacheAppliesSignedDeltas(t *testing.T) {
	cache := NewPoolCache()
	cache.Set("pool", PoolReserves{ReserveA: big.NewInt(100), ReserveB: big.NewInt(200), TotalShares: big.NewInt(50)})

	got, err := cache.apply("pool", big.NewInt(10), big.NewInt(20), big.NewInt(5))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got.ReserveA.Int64() != 110 || got.ReserveB.Int64() != 220 || got.TotalShares.Int64() != 55 {
		t.Fatalf("unexpected reserves after deposit: %v", got)
	}

	if _, err := cache.apply("pool", big.NewInt(-1000), big.NewInt(0), big.NewInt(0)); err == nil {
		t.Fatal("expected a negative reserve to be rejected")
	}
	if _, err := cache.apply("unknown", big.NewInt(1), big.NewInt(1), big.NewInt(1)); !errors.Is(err, ErrPoolNotCached) {
		t.Fatalf("expected ErrPoolNotCached, got %v", err)
	}

	cached, _ := cache.Get("pool")
	if cached.ReserveA.Int64() != 110 {
		t.Fatal("a rejected change must not alter the cache")
	}
}
