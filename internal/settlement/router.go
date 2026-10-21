package settlement

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/fx"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
)

// Settlement router errors.
var (
	// ErrAllLegsFailed means every leg of a split execution failed.
	ErrAllLegsFailed = errors.New("settlement router: all legs failed")
	// ErrPartialFill means some legs succeeded and others failed; the caller
	// must decide whether to proceed with what landed or roll back.
	ErrPartialFill = errors.New("settlement router: partial fill")
)

// LegStatus is the outcome of a single leg's execution.
type LegStatus int

const (
	LegPending   LegStatus = iota
	LegSubmitted
	LegConfirmed
	LegFailed
	LegRolledBack
)

func (s LegStatus) String() string {
	switch s {
	case LegSubmitted:
		return "submitted"
	case LegConfirmed:
		return "confirmed"
	case LegFailed:
		return "failed"
	case LegRolledBack:
		return "rolled_back"
	default:
		return "pending"
	}
}

// LegResult is the outcome of executing one leg of a routed settlement.
type LegResult struct {
	Leg       fx.RouteLeg
	Status    LegStatus
	TxHash    string
	Error     error
	StartedAt time.Time
	EndedAt   time.Time
}

// SettlementResult is the outcome of a full routed settlement.
type SettlementResult struct {
	Decision   *fx.RouteDecision
	Legs       []LegResult
	Succeeded  int
	Failed     int
	TotalSent  decimal.Decimal
	StartedAt  time.Time
	EndedAt    time.Time
}

// LegExecutor submits one leg of a split settlement and returns the
// transaction hash on success. Implementations wrap the Stellar submission
// path (channel pool → sequence manager → Horizon broadcast).
type LegExecutor interface {
	// Execute submits a payment of amount via the given venue and returns the
	// on-chain transaction hash.
	Execute(ctx context.Context, fromAsset, toAsset string, amount decimal.Decimal, venue fx.Venue) (txHash string, err error)
}

// LegRollback reverses a settled leg. For Stellar DEX legs this is typically a
// path payment in the reverse direction; for anchor legs it may be a refund
// API call. The interface is intentionally narrow so the router doesn't
// prescribe the reversal strategy.
type LegRollback interface {
	Rollback(ctx context.Context, txHash string, fromAsset, toAsset string, amount decimal.Decimal, venue fx.Venue) error
}

// SettlementRouterConfig configures the execution router.
type SettlementRouterConfig struct {
	// MaxConcurrentLegs bounds the number of legs executed in parallel.
	MaxConcurrentLegs int
	// LegTimeout bounds a single leg's execution time.
	LegTimeout time.Duration
	// RollbackOnPartial, when true, attempts to reverse successful legs if
	// any leg fails; when false the caller receives ErrPartialFill and decides.
	RollbackOnPartial bool
}

// DefaultSettlementRouterConfig returns conservative defaults.
func DefaultSettlementRouterConfig() SettlementRouterConfig {
	return SettlementRouterConfig{
		MaxConcurrentLegs: 4,
		LegTimeout:        30 * time.Second,
		RollbackOnPartial: false,
	}
}

// SettlementRouterMetrics is a snapshot of the router's counters.
type SettlementRouterMetrics struct {
	Executed      int64
	Succeeded     int64
	PartialFills  int64
	AllFailed     int64
	RollbacksRun  int64
	LegsSubmitted int64
	LegsFailed    int64
}

// SettlementRouter takes a RouteDecision from the SmartRouter and executes the
// legs concurrently, coordinating atomic rollback when configured (issue #84).
//
// It is agnostic to how legs are submitted: the LegExecutor and LegRollback
// interfaces are supplied by the caller so the router can be tested without a
// live Stellar network.
type SettlementRouter struct {
	executor LegExecutor
	rollback LegRollback // may be nil (no automatic rollback)
	cfg      SettlementRouterConfig

	executed, succeeded, partial, allFailed, rollbacksRun atomic.Int64
	legsSubmitted, legsFailed                             atomic.Int64
}

// NewSettlementRouter builds an execution router.
func NewSettlementRouter(executor LegExecutor, rollback LegRollback, cfg SettlementRouterConfig) *SettlementRouter {
	if cfg.MaxConcurrentLegs <= 0 {
		cfg.MaxConcurrentLegs = DefaultSettlementRouterConfig().MaxConcurrentLegs
	}
	if cfg.LegTimeout <= 0 {
		cfg.LegTimeout = DefaultSettlementRouterConfig().LegTimeout
	}
	return &SettlementRouter{
		executor: executor,
		rollback: rollback,
		cfg:      cfg,
	}
}

// Execute runs every leg of decision concurrently, bounded by
// MaxConcurrentLegs, and returns a SettlementResult summarising the outcome.
//
// When every leg succeeds: error is nil. When some fail and RollbackOnPartial
// is false: error is ErrPartialFill. When some fail and RollbackOnPartial is
// true: the router attempts to reverse the succeeded legs and the error
// describes what happened. When all fail: error is ErrAllLegsFailed.
func (r *SettlementRouter) Execute(ctx context.Context, decision *fx.RouteDecision) (*SettlementResult, error) {
	if decision == nil || len(decision.Legs) == 0 {
		return nil, fmt.Errorf("settlement router: empty route decision")
	}
	r.executed.Add(1)
	start := time.Now()

	results := make([]LegResult, len(decision.Legs))
	sem := make(chan struct{}, r.cfg.MaxConcurrentLegs)
	var wg sync.WaitGroup

	for i, leg := range decision.Legs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			legCtx, cancel := context.WithTimeout(ctx, r.cfg.LegTimeout)
			defer cancel()

			lr := LegResult{
				Leg:       leg,
				Status:    LegPending,
				StartedAt: time.Now(),
			}

			hash, err := r.executor.Execute(legCtx, decision.FromAsset, decision.ToAsset, leg.Amount, leg.Venue)
			lr.EndedAt = time.Now()
			if err != nil {
				lr.Status = LegFailed
				lr.Error = err
				r.legsFailed.Add(1)
				log.Warn().Err(err).
					Str("venue", string(leg.Venue)).
					Str("amount", leg.Amount.String()).
					Msg("settlement router: leg failed")
			} else {
				lr.Status = LegConfirmed
				lr.TxHash = hash
				r.legsSubmitted.Add(1)
			}
			results[i] = lr
		}()
	}
	wg.Wait()

	// Tally.
	var succeeded, failed int
	totalSent := decimal.Zero
	for _, lr := range results {
		switch lr.Status {
		case LegConfirmed:
			succeeded++
			totalSent = totalSent.Add(lr.Leg.Amount)
		default:
			failed++
		}
	}

	result := &SettlementResult{
		Decision:  decision,
		Legs:      results,
		Succeeded: succeeded,
		Failed:    failed,
		TotalSent: totalSent,
		StartedAt: start,
		EndedAt:   time.Now(),
	}

	switch {
	case failed == 0:
		r.succeeded.Add(1)
		return result, nil

	case succeeded == 0:
		r.allFailed.Add(1)
		return result, ErrAllLegsFailed

	default:
		// Partial fill.
		r.partial.Add(1)
		if r.cfg.RollbackOnPartial && r.rollback != nil {
			r.attemptRollback(ctx, decision, results)
		}
		return result, ErrPartialFill
	}
}

// attemptRollback tries to reverse every leg that succeeded.
func (r *SettlementRouter) attemptRollback(ctx context.Context, decision *fx.RouteDecision, results []LegResult) {
	r.rollbacksRun.Add(1)
	var wg sync.WaitGroup
	for i := range results {
		if results[i].Status != LegConfirmed {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			rbCtx, cancel := context.WithTimeout(ctx, r.cfg.LegTimeout)
			defer cancel()

			err := r.rollback.Rollback(rbCtx, results[i].TxHash,
				decision.FromAsset, decision.ToAsset,
				results[i].Leg.Amount, results[i].Leg.Venue)
			if err != nil {
				log.Error().Err(err).
					Str("tx_hash", results[i].TxHash).
					Msg("settlement router: rollback failed")
			} else {
				results[i].Status = LegRolledBack
			}
		}()
	}
	wg.Wait()
}

// Metrics returns a snapshot of the router's counters.
func (r *SettlementRouter) Metrics() SettlementRouterMetrics {
	return SettlementRouterMetrics{
		Executed:      r.executed.Load(),
		Succeeded:     r.succeeded.Load(),
		PartialFills:  r.partial.Load(),
		AllFailed:     r.allFailed.Load(),
		RollbacksRun:  r.rollbacksRun.Load(),
		LegsSubmitted: r.legsSubmitted.Load(),
		LegsFailed:    r.legsFailed.Load(),
	}
}
