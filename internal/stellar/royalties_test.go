package stellar

import (
	"errors"
	"math/big"
	"testing"
)

func TestQuoteRoyaltySplitsSale(t *testing.T) {
	quote, err := QuoteRoyalty(big.NewInt(1_000_000), 500, DefaultMaxRoyaltyBps)
	if err != nil {
		t.Fatalf("QuoteRoyalty: %v", err)
	}
	if quote.RoyaltyAmount.Int64() != 50_000 || quote.SellerProceeds.Int64() != 950_000 {
		t.Fatalf("royalty %s / proceeds %s, want 50000 / 950000", quote.RoyaltyAmount, quote.SellerProceeds)
	}
}

func TestQuoteRoyaltyRoundsDown(t *testing.T) {
	quote, err := QuoteRoyalty(big.NewInt(19), 500, DefaultMaxRoyaltyBps)
	if err != nil {
		t.Fatalf("QuoteRoyalty: %v", err)
	}
	if quote.RoyaltyAmount.Sign() != 0 || quote.SellerProceeds.Int64() != 19 {
		t.Fatalf("royalty %s / proceeds %s, want 0 / 19", quote.RoyaltyAmount, quote.SellerProceeds)
	}
}

func TestQuoteRoyaltyRejectsInvalidInput(t *testing.T) {
	for _, tc := range []struct {
		name  string
		price *big.Int
		bps   uint32
		max   uint32
		want  error
	}{
		{"nil price", nil, 500, DefaultMaxRoyaltyBps, ErrInvalidSalePrice},
		{"zero price", big.NewInt(0), 500, DefaultMaxRoyaltyBps, ErrInvalidSalePrice},
		{"negative price", big.NewInt(-1), 500, DefaultMaxRoyaltyBps, ErrInvalidSalePrice},
		{"rate above maximum", big.NewInt(100), 2_501, DefaultMaxRoyaltyBps, ErrRoyaltyExceedsMaxBps},
		{"maximum above 100%", big.NewInt(100), 500, 10_001, ErrInvalidRoyaltyBps},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := QuoteRoyalty(tc.price, tc.bps, tc.max); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}
