package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/stellar"
	"github.com/stellar/go/xdr"
)

type sorobanFakeSource struct {
	latest  uint32
	ledgers map[uint32]*LedgerEvents
}

func (f *sorobanFakeSource) LatestLedger(context.Context) (uint32, error) { return f.latest, nil }

func (f *sorobanFakeSource) LedgerEvents(_ context.Context, seq uint32) (*LedgerEvents, error) {
	l, ok := f.ledgers[seq]
	if !ok {
		return nil, fmt.Errorf("ledger %d unavailable", seq)
	}
	return l, nil
}

type sorobanFakeStore struct {
	ledgers map[uint32]string
	events  map[string]*domain.ContractEvent
}

func newSorobanFakeStore() *sorobanFakeStore {
	return &sorobanFakeStore{ledgers: map[uint32]string{}, events: map[string]*domain.ContractEvent{}}
}

func (f *sorobanFakeStore) SaveLedger(_ context.Context, l domain.IndexedLedger, events []*domain.ContractEvent) ([]*domain.ContractEvent, error) {
	f.ledgers[l.Sequence] = l.Hash
	var inserted []*domain.ContractEvent
	for _, e := range events {
		if _, exists := f.events[e.ID]; exists {
			continue
		}
		f.events[e.ID] = e
		inserted = append(inserted, e)
	}
	return inserted, nil
}

func (f *sorobanFakeStore) GetLedger(_ context.Context, seq uint32) (*domain.IndexedLedger, error) {
	hash, ok := f.ledgers[seq]
	if !ok {
		return nil, nil
	}
	return &domain.IndexedLedger{Sequence: seq, Hash: hash}, nil
}

func (f *sorobanFakeStore) LastLedger(context.Context) (*domain.IndexedLedger, error) {
	var seqs []int
	for seq := range f.ledgers {
		seqs = append(seqs, int(seq))
	}
	if len(seqs) == 0 {
		return nil, nil
	}
	sort.Ints(seqs)
	last := uint32(seqs[len(seqs)-1])
	return &domain.IndexedLedger{Sequence: last, Hash: f.ledgers[last]}, nil
}

func (f *sorobanFakeStore) DeleteFromLedger(_ context.Context, seq uint32) error {
	for s := range f.ledgers {
		if s >= seq {
			delete(f.ledgers, s)
		}
	}
	for id, e := range f.events {
		if e.Ledger >= seq {
			delete(f.events, id)
		}
	}
	return nil
}

type sorobanFakeDispatcher struct {
	eventTypes []string
	payloads   []interface{}
}

func (f *sorobanFakeDispatcher) Dispatch(_ context.Context, eventType string, payload interface{}) error {
	f.eventTypes = append(f.eventTypes, eventType)
	f.payloads = append(f.payloads, payload)
	return nil
}

func sorobanEncode(t testing.TB, v xdr.ScVal) string {
	t.Helper()
	encoded, err := xdr.MarshalBase64(v)
	if err != nil {
		t.Fatalf("encode scval: %v", err)
	}
	return encoded
}

func sorobanSymbol(s string) xdr.ScVal {
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

func sorobanRawEvent(t testing.TB, id string) RawContractEvent {
	return RawContractEvent{
		ID:         id,
		ContractID: "CA3D5KRYM6CB7OWQ6TWYRR3Z4T7GNZLKERYNZGGA5SOAOPIFY6YQGAXE",
		Type:       "contract",
		TxHash:     "abc123",
		TopicsXDR:  []string{sorobanEncode(t, sorobanSymbol("transfer"))},
		ValueXDR:   sorobanEncode(t, stellar.I128ScVal(big.NewInt(100))),
	}
}

func sorobanLedger(t testing.TB, seq uint32, hash, prev string, eventIDs ...string) *LedgerEvents {
	l := &LedgerEvents{Sequence: seq, Hash: hash, PrevHash: prev, ClosedAt: time.Unix(int64(seq), 0).UTC()}
	for _, id := range eventIDs {
		l.Events = append(l.Events, sorobanRawEvent(t, id))
	}
	return l
}

func TestDecodeContractEventDecodesTopicsAndData(t *testing.T) {
	event, err := DecodeContractEvent(sorobanRawEvent(t, "e1"), 7, time.Unix(7, 0).UTC())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if event.Topic != "transfer" {
		t.Errorf("topic = %q, want transfer", event.Topic)
	}
	if string(event.Topics) != `["transfer"]` {
		t.Errorf("topics = %s", event.Topics)
	}
	var data string
	if err := json.Unmarshal(event.Data, &data); err != nil || data != "100" {
		t.Errorf("data = %s (err %v), want \"100\"", event.Data, err)
	}
	if event.Ledger != 7 {
		t.Errorf("ledger = %d, want 7", event.Ledger)
	}
}

func TestDecodeContractEventRejectsInvalidXDR(t *testing.T) {
	raw := sorobanRawEvent(t, "e1")
	raw.TopicsXDR = []string{"not-xdr"}
	if _, err := DecodeContractEvent(raw, 1, time.Now()); err == nil {
		t.Fatal("expected error for invalid topic xdr")
	}
}

func TestSorobanEventIndexerStoresAndDispatchesOnce(t *testing.T) {
	source := &sorobanFakeSource{latest: 2, ledgers: map[uint32]*LedgerEvents{
		1: sorobanLedger(t, 1, "h1", "h0", "e1"),
		2: sorobanLedger(t, 2, "h2", "h1", "e2", "e3"),
	}}
	store := newSorobanFakeStore()
	dispatcher := &sorobanFakeDispatcher{}
	idx := NewSorobanEventIndexer(source, store, dispatcher, SorobanEventConfig{StartLedger: 1})

	if err := idx.Sync(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := idx.Sync(context.Background()); err != nil {
		t.Fatalf("second sync: %v", err)
	}

	if len(store.events) != 3 {
		t.Fatalf("stored %d events, want 3", len(store.events))
	}
	if len(dispatcher.eventTypes) != 3 {
		t.Fatalf("dispatched %d webhooks, want 3", len(dispatcher.eventTypes))
	}
	for _, et := range dispatcher.eventTypes {
		if et != domain.EventContractEventEmitted {
			t.Errorf("event type = %q, want %q", et, domain.EventContractEventEmitted)
		}
	}
}

func TestSorobanEventIndexerRollsBackOnReorg(t *testing.T) {
	source := &sorobanFakeSource{latest: 3, ledgers: map[uint32]*LedgerEvents{
		1: sorobanLedger(t, 1, "h1", "h0", "e1"),
		2: sorobanLedger(t, 2, "h2", "h1", "e2"),
		3: sorobanLedger(t, 3, "h3", "h2", "e3"),
	}}
	store := newSorobanFakeStore()
	idx := NewSorobanEventIndexer(source, store, nil, SorobanEventConfig{StartLedger: 1})
	if err := idx.Sync(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}

	// Ledgers 2 and 3 are replaced by a competing fork.
	source.latest = 4
	source.ledgers[2] = sorobanLedger(t, 2, "h2b", "h1", "e2b")
	source.ledgers[3] = sorobanLedger(t, 3, "h3b", "h2b", "e3b")
	source.ledgers[4] = sorobanLedger(t, 4, "h4b", "h3b", "e4b")
	if err := idx.Sync(context.Background()); err != nil {
		t.Fatalf("sync after reorg: %v", err)
	}

	for _, orphaned := range []string{"e2", "e3"} {
		if _, ok := store.events[orphaned]; ok {
			t.Errorf("orphaned event %s still stored", orphaned)
		}
	}
	for _, canonical := range []string{"e1", "e2b", "e3b", "e4b"} {
		if _, ok := store.events[canonical]; !ok {
			t.Errorf("canonical event %s missing", canonical)
		}
	}
	if store.ledgers[4] != "h4b" {
		t.Errorf("ledger 4 hash = %q, want h4b", store.ledgers[4])
	}
}

func TestSorobanEventIndexerReportsSequenceGap(t *testing.T) {
	source := &sorobanFakeSource{latest: 3, ledgers: map[uint32]*LedgerEvents{
		1: sorobanLedger(t, 1, "h1", "h0"),
		2: sorobanLedger(t, 3, "h3", "h1"), // source skipped ledger 2
	}}
	store := newSorobanFakeStore()
	idx := NewSorobanEventIndexer(source, store, nil, SorobanEventConfig{StartLedger: 1})

	err := idx.Sync(context.Background())
	if !errors.Is(err, ErrLedgerGap) {
		t.Fatalf("err = %v, want ErrLedgerGap", err)
	}
	if _, ok := store.ledgers[3]; ok {
		t.Fatal("ledger after the gap must not be stored")
	}
	if _, ok := store.ledgers[1]; !ok {
		t.Fatal("ledger before the gap should remain stored")
	}
}

// BenchmarkSorobanEventIndexer measures sustained decode-and-store throughput
// against an in-memory store. The target is 500 events/second.
func BenchmarkSorobanEventIndexer(b *testing.B) {
	const eventsPerLedger = 500
	source := &sorobanFakeSource{ledgers: map[uint32]*LedgerEvents{}}
	prev := "h0"
	for seq := uint32(1); seq <= uint32(b.N); seq++ {
		ids := make([]string, eventsPerLedger)
		for i := range ids {
			ids[i] = fmt.Sprintf("%d-%d", seq, i)
		}
		hash := fmt.Sprintf("h%d", seq)
		source.ledgers[seq] = sorobanLedger(b, seq, hash, prev, ids...)
		prev = hash
	}
	source.latest = uint32(b.N)

	idx := NewSorobanEventIndexer(source, newSorobanFakeStore(), &sorobanFakeDispatcher{}, SorobanEventConfig{StartLedger: 1})
	b.ResetTimer()
	if err := idx.Sync(context.Background()); err != nil {
		b.Fatalf("sync: %v", err)
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*eventsPerLedger)/b.Elapsed().Seconds(), "events/s")
}
