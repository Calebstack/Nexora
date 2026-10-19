package indexer

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/stellar/go/protocols/horizon/base"
	"github.com/stellar/go/protocols/horizon/operations"
)

// memLedgerStore is an in-memory LedgerCheckpointStore.
type memLedgerStore struct {
	mu         sync.Mutex
	wallets    map[string][]*domain.Wallet
	indexed    map[string]map[uint32]bool
	watermarks map[string]uint32
	marked     []uint32
	onMark     func(seq uint32)
}

func newMemLedgerStore() *memLedgerStore {
	return &memLedgerStore{wallets: map[string][]*domain.Wallet{}, indexed: map[string]map[uint32]bool{}, watermarks: map[string]uint32{}}
}

func (s *memLedgerStore) addWallet(tenantID, account string) {
	id := tenantID
	s.wallets[tenantID] = append(s.wallets[tenantID], &domain.Wallet{ID: "w-" + account, PublicKey: account, TenantID: &id})
}

func (s *memLedgerStore) preload(tenantID string, from, to uint32, skip ...LedgerRange) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.indexed[tenantID] == nil {
		s.indexed[tenantID] = map[uint32]bool{}
	}
next:
	for seq := from; seq <= to; seq++ {
		for _, r := range skip {
			if seq >= r.From && seq <= r.To {
				continue next
			}
		}
		s.indexed[tenantID][seq] = true
	}
}

func (s *memLedgerStore) Tenants(context.Context, domain.Mode) ([]string, error) {
	out := make([]string, 0, len(s.wallets))
	for id := range s.wallets {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

func (s *memLedgerStore) TenantWallets(_ context.Context, tenantID string, _ domain.Mode) ([]*domain.Wallet, error) {
	return s.wallets[tenantID], nil
}

func (s *memLedgerStore) sorted(tenantID string) []uint32 {
	out := make([]uint32, 0, len(s.indexed[tenantID]))
	for seq := range s.indexed[tenantID] {
		out = append(out, seq)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (s *memLedgerStore) Bounds(_ context.Context, tenantID string, _ domain.Mode) (LedgerBounds, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seqs := s.sorted(tenantID)
	if len(seqs) == 0 {
		return LedgerBounds{}, false, nil
	}
	b := LedgerBounds{Min: seqs[0], Max: seqs[len(seqs)-1], Contiguous: seqs[0] - 1}
	if w, ok := s.watermarks[tenantID]; ok {
		b.Contiguous = w
	}
	return b, true, nil
}

func (s *memLedgerStore) Gaps(_ context.Context, tenantID string, _ domain.Mode, from uint32, limit int) ([]LedgerRange, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []LedgerRange
	seqs := s.sorted(tenantID)
	for i := 0; i+1 < len(seqs) && len(out) < limit; i++ {
		if seqs[i] >= from && seqs[i+1] > seqs[i]+1 {
			out = append(out, LedgerRange{From: seqs[i] + 1, To: seqs[i+1] - 1})
		}
	}
	return out, nil
}

func (s *memLedgerStore) MarkIndexed(_ context.Context, tenantID string, _ domain.Mode, seq uint32) error {
	s.mu.Lock()
	if s.indexed[tenantID] == nil {
		s.indexed[tenantID] = map[uint32]bool{}
	}
	s.indexed[tenantID][seq] = true
	s.marked = append(s.marked, seq)
	onMark := s.onMark
	s.mu.Unlock()
	if onMark != nil {
		onMark(seq)
	}
	return nil
}

func (s *memLedgerStore) AdvanceContiguous(_ context.Context, tenantID string, _ domain.Mode, seq uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq > s.watermarks[tenantID] {
		s.watermarks[tenantID] = seq
	}
	return nil
}

func (s *memLedgerStore) missing(tenantID string, from, to uint32) []uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []uint32
	for seq := from; seq <= to; seq++ {
		if !s.indexed[tenantID][seq] {
			out = append(out, seq)
		}
	}
	return out
}

// fakeLedgerSource serves one payment per ledger to a rotating account with
// random latency, so fetches complete out of order.
type fakeLedgerSource struct {
	elder, latest uint32
	accounts      []string
	failMu        sync.Mutex
	failAt        map[uint32]bool
	inFlight      atomic.Int32
	peak          atomic.Int32
	maxFetched    atomic.Uint32
	fetches       atomic.Int32
}

func (s *fakeLedgerSource) History(context.Context) (uint32, uint32, error) {
	return s.elder, s.latest, nil
}

func (s *fakeLedgerSource) LedgerPayments(ctx context.Context, seq uint32) ([]operations.Operation, error) {
	n := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)
	for p := s.peak.Load(); n > p && !s.peak.CompareAndSwap(p, n); p = s.peak.Load() {
	}
	for m := s.maxFetched.Load(); seq > m && !s.maxFetched.CompareAndSwap(m, seq); m = s.maxFetched.Load() {
	}
	s.fetches.Add(1)
	time.Sleep(time.Duration(rand.IntN(3000)) * time.Microsecond)
	s.failMu.Lock()
	fail := s.failAt[seq]
	delete(s.failAt, seq)
	s.failMu.Unlock()
	if fail {
		return nil, errors.New("horizon 503")
	}
	to := s.accounts[int(seq)%len(s.accounts)]
	return []operations.Operation{
		operations.Payment{
			Base: operations.Base{ID: fmt.Sprint(seq), PT: fmt.Sprint(seq), TransactionSuccessful: true, TransactionHash: "h" + strconv.Itoa(int(seq))},
			To:   to, Amount: "1.0000000", Asset: base.Asset{Type: "native"},
		},
		// A non-payment operation is ignored.
		operations.CreateAccount{Base: operations.Base{ID: fmt.Sprint(seq) + "-c"}},
	}, nil
}

func newGapFixture(cfg GapDetectorConfig, src *fakeLedgerSource) (*GapDetector, *memLedgerStore, *fakeTransferRepo) {
	txRepo := newFakeTransferRepo()
	idx := New(newFakeWalletRepo(), txRepo, nil)
	store := newMemLedgerStore()
	return NewGapDetector(idx, store, src, cfg), store, txRepo
}

func appliedLedgers(txRepo *fakeTransferRepo) []int {
	out := make([]int, 0, len(txRepo.created))
	for _, tx := range txRepo.created {
		n, _ := strconv.Atoi(tx.TxHash[1:])
		out = append(out, n)
	}
	return out
}

func assertAscending(t *testing.T, seqs []uint32) {
	t.Helper()
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("ledgers applied out of order at %d: %v", i, seqs[max(0, i-3):min(len(seqs), i+3)])
		}
	}
}

func TestGapDetector_BackfillsGapsInParallelAppliesInOrder(t *testing.T) {
	src := &fakeLedgerSource{elder: 1, latest: 300, accounts: []string{"GA", "GB", "GX"}}
	d, store, txRepo := newGapFixture(GapDetectorConfig{Workers: 4}, src)
	store.addWallet("t1", "GA")
	store.addWallet("t1", "GB")
	gaps := []LedgerRange{{From: 120, To: 179}, {From: 200, To: 200}, {From: 250, To: 260}}
	store.preload("t1", 100, 300, gaps...)

	var maxAhead atomic.Int32
	store.onMark = func(seq uint32) {
		if ahead := int32(src.maxFetched.Load()) - int32(seq); ahead > maxAhead.Load() {
			maxAhead.Store(ahead)
		}
	}

	if err := d.ScanAll(context.Background()); err != nil {
		t.Fatalf("ScanAll: %v", err)
	}

	if missing := store.missing("t1", 100, 300); len(missing) != 0 {
		t.Fatalf("still missing %v", missing)
	}
	assertAscending(t, store.marked)
	if len(store.marked) != 72 {
		t.Fatalf("marked %d ledgers, want exactly the 72 missing ones", len(store.marked))
	}
	// Only GA/GB payments in the backfilled ledgers are recorded, in ledger
	// order: 40 in 120-179, none in 200 (GX), 7 in 250-260.
	applied := appliedLedgers(txRepo)
	if len(applied) != 47 || !sort.IntsAreSorted(applied) {
		t.Fatalf("applied payments %v", applied)
	}
	if p := src.peak.Load(); p < 2 || p > 4 {
		t.Fatalf("peak concurrent fetches = %d, want bounded at 4 and actually parallel", p)
	}
	if maxAhead.Load() > 2*4 {
		t.Fatalf("fetched %d ledgers ahead of the applier, window is 8", maxAhead.Load())
	}
	m := d.Metrics()[0]
	if m.OpenGaps != 0 || m.MissingLedgers != 0 || m.Contiguous != 300 || m.LagLedgers != 0 || m.IngestedTotal != 72 {
		t.Fatalf("metrics = %+v", m)
	}
}

func TestGapDetector_IngestsTailAndReportsLag(t *testing.T) {
	src := &fakeLedgerSource{elder: 1, latest: 5000, accounts: []string{"GA"}}
	d, store, _ := newGapFixture(GapDetectorConfig{Workers: 8, StartLedger: 4000, MaxTailPerPass: 600}, src)
	store.addWallet("t1", "GA")

	if err := d.ScanAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	m := d.Metrics()[0]
	if m.Min != 4000 || m.Max != 4599 || m.LagLedgers != 401 || m.Contiguous != 4599 {
		t.Fatalf("after first pass metrics = %+v", m)
	}
	if err := d.ScanAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m := d.Metrics()[0]; m.Max != 5000 || m.LagLedgers != 0 {
		t.Fatalf("after second pass metrics = %+v", m)
	}
	assertAscending(t, store.marked)
}

func TestGapDetector_FailureKeepsAppliedPrefixAndResumes(t *testing.T) {
	src := &fakeLedgerSource{elder: 1, latest: 200, accounts: []string{"GA"}, failAt: map[uint32]bool{150: true}}
	d, store, _ := newGapFixture(GapDetectorConfig{Workers: 4}, src)
	store.addWallet("t1", "GA")
	store.preload("t1", 100, 200, LedgerRange{From: 120, To: 180})

	if err := d.ScanAll(context.Background()); err == nil {
		t.Fatal("expected the fetch failure to surface")
	}
	if missing := store.missing("t1", 120, 149); len(missing) != 0 {
		t.Fatalf("prefix before the failure must stay applied, missing %v", missing)
	}
	if !store.indexed["t1"][119] || store.indexed["t1"][150] {
		t.Fatal("ledger 150 must not be marked")
	}
	if w := store.watermarks["t1"]; w != 0 && w >= 150 {
		t.Fatalf("watermark %d passed the unfilled gap", w)
	}

	if err := d.ScanAll(context.Background()); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if missing := store.missing("t1", 100, 200); len(missing) != 0 {
		t.Fatalf("still missing %v", missing)
	}
	assertAscending(t, store.marked)
}

func TestGapDetector_PrunedGapIsUnrecoverableButOthersFill(t *testing.T) {
	src := &fakeLedgerSource{elder: 150, latest: 300, accounts: []string{"GA"}}
	d, store, _ := newGapFixture(GapDetectorConfig{Workers: 2}, src)
	store.addWallet("t1", "GA")
	store.preload("t1", 100, 300, LedgerRange{From: 110, To: 119}, LedgerRange{From: 200, To: 209})

	if err := d.ScanAll(context.Background()); err != nil {
		t.Fatalf("a pruned gap must not fail the pass: %v", err)
	}
	if len(store.missing("t1", 200, 209)) != 0 {
		t.Fatal("the recoverable gap was not filled")
	}
	if len(store.missing("t1", 110, 119)) != 10 {
		t.Fatal("the pruned gap cannot have been filled")
	}
	m := d.Metrics()[0]
	if !m.UnrecoverableGap || m.OpenGaps != 1 || m.MissingLedgers != 10 || m.Contiguous != 109 {
		t.Fatalf("metrics = %+v", m)
	}
}

func TestGapDetector_TenantsAreIsolated(t *testing.T) {
	src := &fakeLedgerSource{elder: 1, latest: 120, accounts: []string{"GA", "GB"}}
	d, store, txRepo := newGapFixture(GapDetectorConfig{Workers: 3, StartLedger: 101}, src)
	store.addWallet("tenant-a", "GA")
	store.addWallet("tenant-b", "GB")

	if err := d.ScanAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, tx := range txRepo.created {
		n, _ := strconv.Atoi(tx.TxHash[1:])
		want := "tenant-a"
		if n%2 == 1 {
			want = "tenant-b"
		}
		if tx.TenantID == nil || *tx.TenantID != want {
			t.Fatalf("payment in ledger %d attributed to %v, want %s", n, tx.TenantID, want)
		}
	}
	if len(txRepo.created) != 20 {
		t.Fatalf("recorded %d payments, want 20 (one per ledger, each to one tenant)", len(txRepo.created))
	}
	if len(store.missing("tenant-a", 101, 120)) != 0 || len(store.missing("tenant-b", 101, 120)) != 0 {
		t.Fatal("each tenant indexes every ledger, including ledgers without its payments")
	}
}

func TestLedgerRangeLen(t *testing.T) {
	if (LedgerRange{From: 5, To: 5}).Len() != 1 || (LedgerRange{From: 10, To: 19}).Len() != 10 {
		t.Fatal("Len is inclusive")
	}
}
