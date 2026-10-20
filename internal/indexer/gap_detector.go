package indexer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/tenant"
	"github.com/rs/zerolog/log"
	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/protocols/horizon/operations"
)

// horizonPageLimit is Horizon's hard maximum page size.
const horizonPageLimit = 200

// ErrLedgerPruned means a missing ledger is older than the oldest ledger the
// Horizon node still serves, so it cannot be backfilled from that node.
var ErrLedgerPruned = errors.New("ledger is older than horizon history")

// LedgerRange is an inclusive range of ledger sequences.
type LedgerRange struct {
	From, To uint32
}

// Len is the number of ledgers in the range.
func (r LedgerRange) Len() uint32 { return r.To - r.From + 1 }

// LedgerBounds summarises what is indexed for one tenant and mode.
// Contiguous is the highest sequence below which nothing is missing.
type LedgerBounds struct {
	Min, Max, Contiguous uint32
}

// LedgerCheckpointStore records which ledgers each tenant has ingested.
type LedgerCheckpointStore interface {
	Tenants(ctx context.Context, mode domain.Mode) ([]string, error)
	TenantWallets(ctx context.Context, tenantID string, mode domain.Mode) ([]*domain.Wallet, error)
	// Bounds returns ok=false when nothing is indexed yet.
	Bounds(ctx context.Context, tenantID string, mode domain.Mode) (b LedgerBounds, ok bool, err error)
	// Gaps returns up to limit missing ranges above from, ascending.
	Gaps(ctx context.Context, tenantID string, mode domain.Mode, from uint32, limit int) ([]LedgerRange, error)
	// MarkIndexed records a ledger as fully applied. It is idempotent.
	MarkIndexed(ctx context.Context, tenantID string, mode domain.Mode, sequence uint32) error
	// AdvanceContiguous raises the contiguous watermark; it never lowers it.
	AdvanceContiguous(ctx context.Context, tenantID string, mode domain.Mode, sequence uint32) error
}

// LedgerSource reads ledgers from Horizon.
type LedgerSource interface {
	// History returns the oldest and newest ledgers Horizon can serve.
	History(ctx context.Context) (elder, latest uint32, err error)
	// LedgerPayments returns every payment operation in a ledger in
	// application order.
	LedgerPayments(ctx context.Context, sequence uint32) ([]operations.Operation, error)
}

// HorizonLedgerClient is satisfied by *horizonclient.Client.
type HorizonLedgerClient interface {
	Root() (horizon.Root, error)
	Payments(request horizonclient.OperationRequest) (operations.OperationsPage, error)
}

// HorizonLedgerSource reads ledgers through Horizon's /ledgers/{sequence}/payments.
type HorizonLedgerSource struct {
	client HorizonLedgerClient
}

func NewHorizonLedgerSource(client HorizonLedgerClient) *HorizonLedgerSource {
	return &HorizonLedgerSource{client: client}
}

func (s *HorizonLedgerSource) History(context.Context) (uint32, uint32, error) {
	root, err := s.client.Root()
	if err != nil {
		return 0, 0, fmt.Errorf("horizon root: %w", err)
	}
	return uint32(root.HistoryElderSequence), uint32(root.HorizonSequence), nil
}

// LedgerPayments pages through the ledger's payments in ascending order,
// joined with their transactions for memo lookup.
func (s *HorizonLedgerSource) LedgerPayments(ctx context.Context, sequence uint32) ([]operations.Operation, error) {
	var ops []operations.Operation
	cursor := ""
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := s.client.Payments(horizonclient.OperationRequest{
			ForLedger: uint(sequence),
			Cursor:    cursor,
			Order:     horizonclient.OrderAsc,
			Limit:     horizonPageLimit,
			Join:      "transactions",
		})
		if err != nil {
			return nil, fmt.Errorf("payments for ledger %d: %w", sequence, err)
		}
		records := page.Embedded.Records
		ops = append(ops, records...)
		if len(records) < horizonPageLimit {
			return ops, nil
		}
		cursor = records[len(records)-1].PagingToken()
	}
}

// GapDetectorConfig tunes the detector. Zero values fall back to defaults.
type GapDetectorConfig struct {
	Mode domain.Mode
	// Workers bounds concurrent ledger fetches (default 8).
	Workers int
	// MaxGapsPerPass and MaxTailPerPass bound one pass's work per tenant
	// (defaults 100 ranges and 1000 ledgers).
	MaxGapsPerPass int
	MaxTailPerPass uint32
	// StartLedger is where a tenant with nothing indexed starts; zero
	// starts at Horizon's latest ledger.
	StartLedger uint32
}

// LedgerMetrics is the latest per-tenant ingestion state.
type LedgerMetrics struct {
	TenantID         string
	Min, Max         uint32
	Contiguous       uint32
	LatestLedger     uint32
	LagLedgers       uint32
	OpenGaps         int
	MissingLedgers   uint64
	IngestedTotal    uint64
	UnrecoverableGap bool
	ScannedAt        time.Time
}

// GapDetector keeps every tenant's ledger index complete: it ingests new
// ledgers at the tail, finds holes left by partitions or restarts, and
// backfills them with a bounded worker pool while applying ledgers strictly
// in ascending order.
type GapDetector struct {
	idx    *Indexer
	store  LedgerCheckpointStore
	source LedgerSource
	cfg    GapDetectorConfig

	mu       sync.Mutex
	metrics  map[string]LedgerMetrics
	ingested atomic.Uint64
}

func NewGapDetector(idx *Indexer, store LedgerCheckpointStore, source LedgerSource, cfg GapDetectorConfig) *GapDetector {
	if cfg.Mode == "" {
		cfg.Mode = domain.ModeLive
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 8
	}
	if cfg.MaxGapsPerPass <= 0 {
		cfg.MaxGapsPerPass = 100
	}
	if cfg.MaxTailPerPass == 0 {
		cfg.MaxTailPerPass = 1000
	}
	return &GapDetector{idx: idx, store: store, source: source, cfg: cfg, metrics: make(map[string]LedgerMetrics)}
}

// ScanAll runs one pass for every tenant; one tenant's failure does not stop
// the others.
func (d *GapDetector) ScanAll(ctx context.Context) error {
	elder, latest, err := d.source.History(ctx)
	if err != nil {
		return err
	}
	tenants, err := d.store.Tenants(ctx, d.cfg.Mode)
	if err != nil {
		return fmt.Errorf("list tenants: %w", err)
	}
	var errs []error
	for _, tenantID := range tenants {
		if err := d.ScanTenant(ctx, tenantID, elder, latest); err != nil {
			errs = append(errs, fmt.Errorf("tenant %s: %w", tenantID, err))
		}
	}
	return errors.Join(errs...)
}

// ScanTenant ingests the tail, then detects and backfills gaps above the
// contiguous watermark, then advances the watermark. The gap scan reads only
// rows above the watermark, so a pass costs O(k) for k ledgers indexed since
// the last pass, not O(history).
func (d *GapDetector) ScanTenant(ctx context.Context, tenantID string, elder, latest uint32) error {
	ctx = tenant.WithMode(tenant.WithID(ctx, tenantID), d.cfg.Mode)
	wallets, err := d.store.TenantWallets(ctx, tenantID, d.cfg.Mode)
	if err != nil {
		return fmt.Errorf("list wallets: %w", err)
	}
	byAccount := make(map[string]*domain.Wallet, len(wallets))
	for _, w := range wallets {
		byAccount[w.PublicKey] = w
	}

	bounds, ok, err := d.store.Bounds(ctx, tenantID, d.cfg.Mode)
	if err != nil {
		return fmt.Errorf("read bounds: %w", err)
	}
	if !ok {
		start := d.cfg.StartLedger
		if start == 0 || start > latest {
			start = latest
		}
		if start == 0 {
			return nil // Horizon has not ingested any ledger yet
		}
		bounds = LedgerBounds{Min: start, Max: start - 1, Contiguous: start - 1}
	}

	if bounds.Max < latest {
		tail := LedgerRange{From: bounds.Max + 1, To: latest}
		if tail.Len() > d.cfg.MaxTailPerPass {
			tail.To = tail.From + d.cfg.MaxTailPerPass - 1
		}
		if err := d.ingest(ctx, tenantID, byAccount, tail); err != nil {
			return fmt.Errorf("ingest tail %d-%d: %w", tail.From, tail.To, err)
		}
		bounds.Max = tail.To
	}

	gaps, err := d.store.Gaps(ctx, tenantID, d.cfg.Mode, bounds.Contiguous, d.cfg.MaxGapsPerPass)
	if err != nil {
		return fmt.Errorf("detect gaps: %w", err)
	}
	m := LedgerMetrics{TenantID: tenantID, Min: bounds.Min, Max: bounds.Max, LatestLedger: latest, OpenGaps: len(gaps), ScannedAt: time.Now().UTC()}
	if latest > bounds.Max {
		m.LagLedgers = latest - bounds.Max
	}
	for _, g := range gaps {
		m.MissingLedgers += uint64(g.Len())
	}

	var backfillErr error
	for _, g := range gaps {
		if g.From < elder {
			// Retrying cannot help; it needs a full-history Horizon.
			m.UnrecoverableGap = true
			log.Error().Err(ErrLedgerPruned).Str("tenant_id", tenantID).Uint32("from", g.From).Uint32("to", g.To).
				Uint32("history_elder_ledger", elder).Msg("indexer: unrecoverable ledger gap")
			continue
		}
		log.Info().Str("tenant_id", tenantID).Uint32("from", g.From).Uint32("to", g.To).Msg("indexer: backfilling ledger gap")
		if err := d.ingest(ctx, tenantID, byAccount, g); err != nil {
			backfillErr = fmt.Errorf("backfill %d-%d: %w", g.From, g.To, err)
			break
		}
		m.MissingLedgers -= uint64(g.Len())
		m.OpenGaps--
	}

	// Everything up to the first unfilled gap is now contiguous.
	contiguous := bounds.Max
	if remaining, err := d.store.Gaps(ctx, tenantID, d.cfg.Mode, bounds.Contiguous, 1); err != nil {
		return fmt.Errorf("re-check gaps: %w", err)
	} else if len(remaining) > 0 {
		contiguous = remaining[0].From - 1
	}
	if contiguous > bounds.Contiguous {
		if err := d.store.AdvanceContiguous(ctx, tenantID, d.cfg.Mode, contiguous); err != nil {
			return fmt.Errorf("advance watermark: %w", err)
		}
	}
	m.Contiguous = max(contiguous, bounds.Contiguous)
	m.IngestedTotal = d.ingested.Load()

	d.mu.Lock()
	d.metrics[tenantID] = m
	d.mu.Unlock()
	log.Info().Str("tenant_id", tenantID).Uint32("min_ledger", m.Min).Uint32("max_ledger", m.Max).
		Uint32("contiguous_ledger", m.Contiguous).Uint32("latest_ledger", latest).Uint32("lag_ledgers", m.LagLedgers).
		Int("open_gaps", m.OpenGaps).Uint64("missing_ledgers", m.MissingLedgers).Uint64("ingested_total", m.IngestedTotal).
		Msg("indexer: ledger ingestion metrics")
	return backfillErr
}

// ingest fetches the range with at most Workers concurrent requests and
// applies ledgers strictly in ascending order. A dispatcher hands out
// sequences in order, each holding a slot in a window of 2*Workers, so
// buffered results are bounded by O(Workers) ledgers and the next ledger to
// apply always holds a slot (no deadlock). When a fetch fails, dispatch stops
// and every ledger below the failed one is still applied before the error is
// returned, so the indexed prefix is as long as possible.
func (d *GapDetector) ingest(ctx context.Context, tenantID string, wallets map[string]*domain.Wallet, r LedgerRange) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	dispatchCtx, stopDispatch := context.WithCancel(ctx)
	defer stopDispatch()

	type fetched struct {
		seq uint32
		ops []operations.Operation
		err error
	}
	window := make(chan struct{}, 2*d.cfg.Workers)
	jobs := make(chan uint32)
	results := make(chan fetched)

	go func() {
		defer close(jobs)
		for seq := r.From; seq <= r.To && seq >= r.From; seq++ {
			select {
			case window <- struct{}{}:
			case <-dispatchCtx.Done():
				return
			}
			select {
			case jobs <- seq:
			case <-dispatchCtx.Done():
				return
			}
		}
	}()

	var workers sync.WaitGroup
	for range d.cfg.Workers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for seq := range jobs {
				ops, err := d.source.LedgerPayments(ctx, seq)
				select {
				case results <- fetched{seq: seq, ops: ops, err: err}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() { workers.Wait(); close(results) }()

	pending := make(map[uint32]fetched, cap(window))
	next := r.From
	var (
		failedSeq uint32
		fetchErr  error
	)
	for res := range results {
		if res.err != nil {
			if fetchErr == nil || res.seq < failedSeq {
				failedSeq, fetchErr = res.seq, fmt.Errorf("fetch ledger %d: %w", res.seq, res.err)
			}
			stopDispatch()
		} else {
			pending[res.seq] = res
		}
		for fetchErr == nil || next < failedSeq {
			ready, ok := pending[next]
			if !ok {
				break
			}
			delete(pending, next)
			if err := d.applyLedger(ctx, tenantID, wallets, ready.seq, ready.ops); err != nil {
				return err
			}
			<-window
			if next == r.To {
				return nil
			}
			next++
		}
		if fetchErr != nil && next == failedSeq {
			return fetchErr
		}
	}
	if fetchErr != nil {
		return fetchErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("ledger %d was not fetched", next)
}

// applyLedger applies the ledger's payments in Horizon order, then marks it
// indexed. Payments are recorded with UpsertByTxHash, so re-applying a ledger
// after a crash between the two steps is a no-op.
func (d *GapDetector) applyLedger(ctx context.Context, tenantID string, wallets map[string]*domain.Wallet, seq uint32, ops []operations.Operation) error {
	for _, op := range ops {
		_, _, to, ok := paymentDetails(op)
		if !ok {
			continue
		}
		w, mine := wallets[to]
		if !mine {
			continue
		}
		if err := d.idx.processPayment(ctx, w, op); err != nil {
			return fmt.Errorf("apply ledger %d payment %s: %w", seq, op.GetID(), err)
		}
	}
	if err := d.store.MarkIndexed(ctx, tenantID, d.cfg.Mode, seq); err != nil {
		return fmt.Errorf("mark ledger %d: %w", seq, err)
	}
	d.ingested.Add(1)
	return nil
}

// Metrics returns the latest per-tenant ingestion state.
func (d *GapDetector) Metrics() []LedgerMetrics {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]LedgerMetrics, 0, len(d.metrics))
	for _, m := range d.metrics {
		out = append(out, m)
	}
	return out
}
