package stellar

import (
	"errors"
	"fmt"
	"math/big"
)

const (
	// BasisPointsDenominator is the number of basis points in 100%.
	BasisPointsDenominator = 10_000
	// DefaultMaxRoyaltyBps is the 25% ceiling the royalties contract is
	// deployed with by default.
	DefaultMaxRoyaltyBps = 2_500
)

var (
	ErrInvalidSalePrice     = errors.New("sale price must be positive")
	ErrInvalidRoyaltyBps    = errors.New("royalty basis points out of range")
	ErrRoyaltyExceedsMaxBps = errors.New("royalty exceeds maximum allowed basis points")
)

// RoyaltyQuote is the split of a sale between the creator and the seller, in
// the asset's smallest unit.
type RoyaltyQuote struct {
	SalePrice      *big.Int
	RoyaltyBps     uint32
	RoyaltyAmount  *big.Int
	SellerProceeds *big.Int
}

// QuoteRoyalty mirrors the royalties contract's settlement maths: the royalty
// is salePrice * royaltyBps / 10000 rounded down, and a rate above
// maxRoyaltyBps is refused the same way the contract reverts it.
func QuoteRoyalty(salePrice *big.Int, royaltyBps, maxRoyaltyBps uint32) (RoyaltyQuote, error) {
	if salePrice == nil || salePrice.Sign() <= 0 {
		return RoyaltyQuote{}, ErrInvalidSalePrice
	}
	if maxRoyaltyBps > BasisPointsDenominator {
		return RoyaltyQuote{}, fmt.Errorf("%w: maximum %d exceeds %d", ErrInvalidRoyaltyBps, maxRoyaltyBps, BasisPointsDenominator)
	}
	if royaltyBps > maxRoyaltyBps {
		return RoyaltyQuote{}, fmt.Errorf("%w: %d > %d", ErrRoyaltyExceedsMaxBps, royaltyBps, maxRoyaltyBps)
	}

	royalty := new(big.Int).Mul(salePrice, big.NewInt(int64(royaltyBps)))
	royalty.Quo(royalty, big.NewInt(BasisPointsDenominator))

	return RoyaltyQuote{
		SalePrice:      new(big.Int).Set(salePrice),
		RoyaltyBps:     royaltyBps,
		RoyaltyAmount:  royalty,
		SellerProceeds: new(big.Int).Sub(salePrice, royalty),
	}, nil
}
