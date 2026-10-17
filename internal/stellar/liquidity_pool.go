package stellar

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"github.com/shopspring/decimal"
	"github.com/stellar/go/protocols/horizon/operations"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
)

const feeDenominatorBps = 10_000

var (
	// ErrPoolSlippage means the output of a trade is below the caller's minimum.
	ErrPoolSlippage = errors.New("pool output is below the minimum accepted amount")
	// ErrPoolImbalance means a pool has no usable reserves on one side.
	ErrPoolImbalance = errors.New("pool reserves are empty or imbalanced")
	// ErrPoolNotCached means a confirmed transaction touched a pool whose
	// reserves have not been seeded into the cache yet.
	ErrPoolNotCached = errors.New("pool reserves are not cached")
)

// PoolReserves are a classic liquidity pool's reserves and share supply, in
// stroops.
type PoolReserves struct {
	ReserveA    *big.Int
	ReserveB    *big.Int
	TotalShares *big.Int
}

func (r PoolReserves) clone() PoolReserves {
	return PoolReserves{
		ReserveA:    new(big.Int).Set(r.ReserveA),
		ReserveB:    new(big.Int).Set(r.ReserveB),
		TotalShares: new(big.Int).Set(r.TotalShares),
	}
}

// QuoteSwap returns the exact constant-product output for amountIn, using the
// same integer arithmetic as the classic_bridge contract so the two never
// disagree on a trade. minOut of nil skips the slippage check.
func QuoteSwap(reserveIn, reserveOut, amountIn *big.Int, feeBps uint32, minOut *big.Int) (*big.Int, error) {
	if amountIn.Sign() <= 0 || feeBps >= feeDenominatorBps {
		return nil, fmt.Errorf("invalid swap amount or fee")
	}
	if reserveIn.Sign() <= 0 || reserveOut.Sign() <= 0 {
		return nil, ErrPoolImbalance
	}

	inAfterFee := new(big.Int).Mul(amountIn, big.NewInt(int64(feeDenominatorBps-feeBps)))
	numerator := new(big.Int).Mul(inAfterFee, reserveOut)
	denominator := new(big.Int).Mul(reserveIn, big.NewInt(feeDenominatorBps))
	denominator.Add(denominator, inAfterFee)
	out := numerator.Div(numerator, denominator)

	if out.Sign() <= 0 || out.Cmp(reserveOut) >= 0 {
		return nil, ErrPoolImbalance
	}
	if minOut != nil && out.Cmp(minOut) < 0 {
		return nil, ErrPoolSlippage
	}
	return out, nil
}

// PoolCache is Nexora's view of classic pool reserves, kept current from
// confirmed pool transactions.
type PoolCache struct {
	mu    sync.RWMutex
	pools map[string]PoolReserves
}

func NewPoolCache() *PoolCache {
	return &PoolCache{pools: make(map[string]PoolReserves)}
}

// Set seeds or replaces the reserves of a pool.
func (c *PoolCache) Set(poolID string, r PoolReserves) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pools[poolID] = r.clone()
}

// Get returns a copy of a pool's cached reserves.
func (c *PoolCache) Get(poolID string) (PoolReserves, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	r, ok := c.pools[poolID]
	if !ok {
		return PoolReserves{}, false
	}
	return r.clone(), true
}

// apply adds a signed delta to a cached pool and rejects a result below zero.
func (c *PoolCache) apply(poolID string, dA, dB, dShares *big.Int) (PoolReserves, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cur, ok := c.pools[poolID]
	if !ok {
		return PoolReserves{}, fmt.Errorf("%w: %s", ErrPoolNotCached, poolID)
	}
	next := PoolReserves{
		ReserveA:    new(big.Int).Add(cur.ReserveA, dA),
		ReserveB:    new(big.Int).Add(cur.ReserveB, dB),
		TotalShares: new(big.Int).Add(cur.TotalShares, dShares),
	}
	if next.ReserveA.Sign() < 0 || next.ReserveB.Sign() < 0 || next.TotalShares.Sign() < 0 {
		return PoolReserves{}, fmt.Errorf("pool %s: confirmed change would make reserves negative", poolID)
	}
	c.pools[poolID] = next
	return next.clone(), nil
}

// PoolBridge watches confirmed transactions for classic liquidity pool
// deposits and withdrawals and mirrors them into the PoolCache.
type PoolBridge struct {
	client   Client
	cache    *PoolCache
	onUpdate func(poolID string, reserves PoolReserves)
}

func NewPoolBridge(client Client, cache *PoolCache) *PoolBridge {
	return &PoolBridge{client: client, cache: cache}
}

// WithOnUpdate registers a callback run after each cache update, for example
// to push the new reserves to the classic_bridge contract.
func (b *PoolBridge) WithOnUpdate(fn func(poolID string, reserves PoolReserves)) *PoolBridge {
	b.onUpdate = fn
	return b
}

// ConfirmTransaction applies every liquidity pool operation of a confirmed
// transaction to the cache and returns the IDs of the pools it updated.
func (b *PoolBridge) ConfirmTransaction(hash string) ([]string, error) {
	ops, err := b.client.OperationsForTransaction(hash)
	if err != nil {
		return nil, fmt.Errorf("load operations for %s: %w", hash, err)
	}

	var updated []string
	var errs []error
	for _, op := range ops {
		var (
			poolID               string
			dA, dB, dShares      *big.Int
			applyErr, convertErr error
		)

		switch o := op.(type) {
		case operations.LiquidityPoolDeposit:
			poolID = o.LiquidityPoolID
			if len(o.ReservesDeposited) != 2 {
				errs = append(errs, fmt.Errorf("pool %s deposit: expected 2 reserves", poolID))
				continue
			}
			dA, dB, dShares, convertErr = parseDeltas(o.ReservesDeposited[0].Amount, o.ReservesDeposited[1].Amount, o.SharesReceived, 1)
		case operations.LiquidityPoolWithdraw:
			poolID = o.LiquidityPoolID
			if len(o.ReservesReceived) != 2 {
				errs = append(errs, fmt.Errorf("pool %s withdraw: expected 2 reserves", poolID))
				continue
			}
			dA, dB, dShares, convertErr = parseDeltas(o.ReservesReceived[0].Amount, o.ReservesReceived[1].Amount, o.Shares, -1)
		default:
			continue
		}
		if convertErr != nil {
			errs = append(errs, convertErr)
			continue
		}

		var reserves PoolReserves
		if reserves, applyErr = b.cache.apply(poolID, dA, dB, dShares); applyErr != nil {
			errs = append(errs, applyErr)
			continue
		}
		updated = append(updated, poolID)
		if b.onUpdate != nil {
			b.onUpdate(poolID, reserves)
		}
	}

	return updated, errors.Join(errs...)
}

// parseDeltas converts Horizon decimal amounts to signed stroop deltas.
func parseDeltas(a, b, shares string, sign int64) (*big.Int, *big.Int, *big.Int, error) {
	out := make([]*big.Int, 0, 3)
	for _, s := range []string{a, b, shares} {
		d, err := decimal.NewFromString(s)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("parse pool amount %q: %w", s, err)
		}
		out = append(out, new(big.Int).Mul(d.Shift(7).BigInt(), big.NewInt(sign)))
	}
	return out[0], out[1], out[2], nil
}

// ClassicBridgeContract invokes the classic_bridge Soroban contract.
type ClassicBridgeContract struct {
	soroban         SorobanClient
	signer          Signer
	contractID      string
	adminAccount    string
	encryptedSecret string
}

func NewClassicBridgeContract(soroban SorobanClient, signer Signer, contractID, adminAccount, encryptedSecret string) *ClassicBridgeContract {
	return &ClassicBridgeContract{
		soroban:         soroban,
		signer:          signer,
		contractID:      contractID,
		adminAccount:    adminAccount,
		encryptedSecret: encryptedSecret,
	}
}

// SyncReserves pushes confirmed classic pool reserves to the contract, which
// prices on-chain trades off them. poolID is the 64 character hex pool ID.
func (c *ClassicBridgeContract) SyncReserves(ctx context.Context, poolID string, r PoolReserves) (string, error) {
	id, err := hex.DecodeString(poolID)
	if err != nil || len(id) != 32 {
		return "", fmt.Errorf("invalid liquidity pool id %q", poolID)
	}
	contractID, err := ParseContractID(c.contractID)
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
				FunctionName: xdr.ScSymbol("sync_reserves"),
				Args: xdr.ScVec{
					BytesScVal(id),
					I128ScVal(r.ReserveA),
					I128ScVal(r.ReserveB),
					I128ScVal(r.TotalShares),
				},
			},
		},
		SourceAccount: c.adminAccount,
	}

	tx, err := c.soroban.PrepareInvocation(ctx, c.adminAccount, op)
	if err != nil {
		return "", fmt.Errorf("prepare sync_reserves invocation: %w", err)
	}
	signed, err := c.signer.Sign(tx, c.encryptedSecret)
	if err != nil {
		return "", fmt.Errorf("sign sync_reserves invocation: %w", err)
	}
	hash, err := c.soroban.SubmitTransaction(ctx, signed)
	if err != nil {
		return "", fmt.Errorf("submit sync_reserves invocation: %w", err)
	}
	return hash, nil
}
