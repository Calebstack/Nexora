package settlement

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/protocols/horizon/operations"
	"github.com/stellar/go/support/render/problem"
	"github.com/stellar/go/txnbuild"
)

// seqLedger is a fake Horizon that enforces Stellar's sequence rule: an
// envelope applies only when its sequence is the account's current + 1;
// anything else is rejected with tx_bad_seq and leaves no trace.
type seqLedger struct {
	mu       sync.Mutex
	accounts map[string]int64
	applied  map[string]horizon.Transaction // hash -> tx
	paid     map[string]int                 // payment memo -> times applied
	submits  int

	// bumpEvery advances a random account's sequence out of band (another
	// system signing with the same key) every n submissions.
	bumpEvery int
	// loseEvery applies the envelope but drops the response every n submissions.
	loseEvery int
}

var errResponseLost = errors.New("context deadline exceeded (response lost)")

func newSeqLedger(accounts []string, start int64) *seqLedger {
	l := &seqLedger{accounts: map[string]int64{}, applied: map[string]horizon.Transaction{}, paid: map[string]int{}}
	for _, a := range accounts {
		l.accounts[a] = start
	}
	return l
}

func badSeqError() error {
	return &horizonclient.Error{
		Response: &http.Response{StatusCode: http.StatusBadRequest},
		Problem: problem.P{Status: http.StatusBadRequest, Title: "Transaction Failed",
			Extras: map[string]interface{}{"result_codes": map[string]interface{}{"transaction": "tx_bad_seq"}}},
	}
}

func notFoundError() error {
	return &horizonclient.Error{Response: &http.Response{StatusCode: http.StatusNotFound}, Problem: problem.P{Status: http.StatusNotFound}}
}

func (l *seqLedger) LoadAccount(id string) (horizon.Account, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	seq, ok := l.accounts[id]
	if !ok {
		return horizon.Account{}, notFoundError()
	}
	return horizon.Account{AccountID: id, Sequence: seq}, nil
}

func (l *seqLedger) SubmitTransaction(tx *txnbuild.Transaction) (horizon.Transaction, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.submits++
	src := tx.SourceAccount()
	if l.bumpEvery > 0 && l.submits%l.bumpEvery == 0 {
		l.accounts[src.AccountID]++ // out-of-band transaction consumed the next sequence
	}
	if src.Sequence != l.accounts[src.AccountID]+1 {
		return horizon.Transaction{}, badSeqError()
	}
	hash, err := tx.HashHex(network.TestNetworkPassphrase)
	if err != nil {
		return horizon.Transaction{}, err
	}
	l.accounts[src.AccountID] = src.Sequence
	memo := string(tx.Memo().(txnbuild.MemoText))
	l.paid[memo]++
	res := horizon.Transaction{Hash: hash, Successful: true, AccountSequence: src.Sequence, Memo: memo}
	l.applied[hash] = res
	if l.loseEvery > 0 && l.submits%l.loseEvery == 0 {
		return horizon.Transaction{}, errResponseLost
	}
	return res, nil
}

func (l *seqLedger) TransactionDetail(hash string) (horizon.Transaction, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if tx, ok := l.applied[hash]; ok {
		return tx, nil
	}
	return horizon.Transaction{}, notFoundError()
}

func (l *seqLedger) FindPathsStrict(string, string, string, string, string) ([]horizon.Path, error) {
	return nil, nil
}
func (l *seqLedger) OperationsForTransaction(string) ([]operations.Operation, error) { return nil, nil }
func (l *seqLedger) Payments(string, string, uint) ([]operations.Operation, error)   { return nil, nil }
func (l *seqLedger) StreamPayments(context.Context, string, string, func(operations.Operation) error) error {
	return nil
}
func (l *seqLedger) Offers(string, uint) ([]horizon.Offer, error) { return nil, nil }

// paymentBuilder signs a 1 XLM payment from kp with memo identifying the
// payment, so the ledger can count how many times each payment was applied.
func paymentBuilder(kp *keypair.Full, memo string) EnvelopeBuilder {
	return func(seq int64) (*txnbuild.Transaction, error) {
		tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &txnbuild.SimpleAccount{AccountID: kp.Address(), Sequence: seq - 1},
			IncrementSequenceNum: true,
			BaseFee:              txnbuild.MinBaseFee,
			Memo:                 txnbuild.MemoText(memo),
			Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(30)},
			Operations: []txnbuild.Operation{&txnbuild.Payment{
				Destination: "GAAZI4TCR3TY5OJHCTJC2A4QSY6CJWJH5IAJTGKIN2ER7LBNVKOCCWN7",
				Amount:      "1", Asset: txnbuild.NativeAsset{},
			}},
		})
		if err != nil {
			return nil, err
		}
		return tx.Sign(network.TestNetworkPassphrase, kp)
	}
}

func channelAccounts(n int) []*keypair.Full {
	kps := make([]*keypair.Full, n)
	for i := range kps {
		kps[i] = keypair.MustRandom()
	}
	return kps
}

func addresses(kps []*keypair.Full) []string {
	out := make([]string, len(kps))
	for i, kp := range kps {
		out[i] = kp.Address()
	}
	return out
}

// pay retries a payment the way a job queue would, carrying every envelope
// hash with an unknown outcome into the next attempt.
func pay(ctx context.Context, m *SequenceManager, kp *keypair.Full, memo string) error {
	var prior []string
	for attempt := 0; attempt < 50; attempt++ {
		_, err := m.Submit(ctx, kp.Address(), prior, paymentBuilder(kp, memo))
		if err == nil {
			return nil
		}
		var se *SubmitError
		switch {
		case errors.As(err, &se):
			prior = append(prior, se.Hash) // ambiguous: resolve by hash next time
		case errors.Is(err, ErrSequenceRecoveryExhausted):
			// every envelope was rejected with tx_bad_seq: safe to retry
		default:
			return err
		}
	}
	return fmt.Errorf("payment %s did not settle", memo)
}

func assertExactlyOnce(t *testing.T, l *seqLedger, n int) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.paid) != n {
		t.Fatalf("%d distinct payments applied, want %d", len(l.paid), n)
	}
	for memo, times := range l.paid {
		if times != 1 {
			t.Fatalf("payment %s applied %d times", memo, times)
		}
	}
}

func TestSequenceManager_StressConcurrent1000WithDesyncAndLostResponses(t *testing.T) {
	for _, tc := range []struct {
		name  string
		store func(t *testing.T) SequenceStore
	}{
		{"memory", func(*testing.T) SequenceStore { return NewMemorySequenceStore() }},
		{"redis", func(t *testing.T) SequenceStore {
			mr := miniredis.RunT(t)
			return NewRedisSequenceStore(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kps := channelAccounts(20)
			ledger := newSeqLedger(addresses(kps), 1000)
			ledger.bumpEvery, ledger.loseEvery = 37, 53
			m := NewSequenceManager(tc.store(t), ledger, network.TestNetworkPassphrase, 5)

			const payments = 1000
			var wg sync.WaitGroup
			var failed atomic.Int64
			for i := 0; i < payments; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					if err := pay(context.Background(), m, kps[i%len(kps)], "p"+strconv.Itoa(i)); err != nil {
						failed.Add(1)
						t.Error(err)
					}
				}(i)
			}
			wg.Wait()

			if failed.Load() != 0 {
				t.Fatalf("%d payments failed", failed.Load())
			}
			assertExactlyOnce(t, ledger, payments)
			metrics := m.Metrics()
			if metrics.BadSequence == 0 || metrics.Resyncs == 0 || metrics.AlreadyOnChain == 0 {
				t.Fatalf("recovery paths not exercised: %+v", metrics)
			}
			t.Logf("%s: %+v", tc.name, metrics)
		})
	}
}

func TestSequenceManager_TwoProcessesShareRedisCounter(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	kps := channelAccounts(5)
	ledger := newSeqLedger(addresses(kps), 42)
	workers := []*SequenceManager{
		NewSequenceManager(NewRedisSequenceStore(rdb, ""), ledger, network.TestNetworkPassphrase, 5),
		NewSequenceManager(NewRedisSequenceStore(rdb, ""), ledger, network.TestNetworkPassphrase, 5),
	}

	const payments = 300
	var wg sync.WaitGroup
	for i := 0; i < payments; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := pay(context.Background(), workers[i%2], kps[i%len(kps)], "x"+strconv.Itoa(i)); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	assertExactlyOnce(t, ledger, payments)
}

func TestSequenceManager_BadSeqResyncsAndResigns(t *testing.T) {
	kp := keypair.MustRandom()
	ledger := newSeqLedger([]string{kp.Address()}, 100)
	store := NewMemorySequenceStore()
	m := NewSequenceManager(store, ledger, network.TestNetworkPassphrase, 3)

	if _, err := m.Submit(context.Background(), kp.Address(), nil, paymentBuilder(kp, "a")); err != nil {
		t.Fatal(err)
	}
	// Another system consumes three sequence numbers behind our back.
	ledger.mu.Lock()
	ledger.accounts[kp.Address()] += 3
	ledger.mu.Unlock()

	res, err := m.Submit(context.Background(), kp.Address(), nil, paymentBuilder(kp, "b"))
	if err != nil {
		t.Fatal(err)
	}
	if res.AccountSequence != 105 {
		t.Fatalf("resubmitted with sequence %d, want 105 (on-chain 104 + 1)", res.AccountSequence)
	}
	if mt := m.Metrics(); mt.BadSequence != 1 || mt.Resyncs != 1 {
		t.Fatalf("metrics = %+v", mt)
	}
	if next, _, _ := store.Next(context.Background(), kp.Address()); next != 106 {
		t.Fatalf("counter after recovery hands out %d, want 106", next)
	}
	assertExactlyOnce(t, ledger, 2)
}

func TestSequenceManager_NeverResignsAPaymentThatLanded(t *testing.T) {
	kp := keypair.MustRandom()
	ledger := newSeqLedger([]string{kp.Address()}, 7)
	ledger.loseEvery = 1 // every response is lost after the envelope applies
	m := NewSequenceManager(NewMemorySequenceStore(), ledger, network.TestNetworkPassphrase, 3)

	_, err := m.Submit(context.Background(), kp.Address(), nil, paymentBuilder(kp, "once"))
	var se *SubmitError
	if !errors.As(err, &se) || !errors.Is(err, errResponseLost) {
		t.Fatalf("lost response err = %v, want *SubmitError", err)
	}

	ledger.loseEvery = 0
	res, err := m.Submit(context.Background(), kp.Address(), []string{se.Hash}, paymentBuilder(kp, "once"))
	if err != nil || res.Hash != se.Hash {
		t.Fatalf("retry = %+v, %v; want the landed envelope %s", res, err, se.Hash)
	}
	ledger.mu.Lock()
	submits := ledger.submits
	ledger.mu.Unlock()
	if submits != 1 {
		t.Fatalf("submits = %d, the retry must not submit again", submits)
	}
	assertExactlyOnce(t, ledger, 1)
}

func TestSequenceManager_RejectsUnsafeEnvelopes(t *testing.T) {
	kp := keypair.MustRandom()
	ledger := newSeqLedger([]string{kp.Address()}, 1)
	m := NewSequenceManager(NewMemorySequenceStore(), ledger, network.TestNetworkPassphrase, 1)

	unbounded := func(seq int64) (*txnbuild.Transaction, error) {
		return txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{AccountID: kp.Address(), Sequence: seq - 1}, IncrementSequenceNum: true,
			BaseFee: txnbuild.MinBaseFee, Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewInfiniteTimeout()},
			Operations: []txnbuild.Operation{&txnbuild.BumpSequence{BumpTo: 0}},
		})
	}
	if _, err := m.Submit(context.Background(), kp.Address(), nil, unbounded); !errors.Is(err, ErrUnboundedEnvelope) {
		t.Fatalf("unbounded envelope err = %v", err)
	}
	other := keypair.MustRandom()
	if _, err := m.Submit(context.Background(), kp.Address(), nil, paymentBuilder(other, "m")); err == nil {
		t.Fatal("an envelope from a different source account must be rejected")
	}
	wrongSeq := func(seq int64) (*txnbuild.Transaction, error) { return paymentBuilder(kp, "w")(seq + 1) }
	if _, err := m.Submit(context.Background(), kp.Address(), nil, wrongSeq); err == nil {
		t.Fatal("an envelope ignoring the allocated sequence must be rejected")
	}
}

func TestSequenceManager_ExhaustsWhenDesyncPersists(t *testing.T) {
	kp := keypair.MustRandom()
	ledger := newSeqLedger([]string{kp.Address()}, 1)
	ledger.bumpEvery = 1 // every submission loses the race
	m := NewSequenceManager(NewMemorySequenceStore(), ledger, network.TestNetworkPassphrase, 2)
	if _, err := m.Submit(context.Background(), kp.Address(), nil, paymentBuilder(kp, "z")); !errors.Is(err, ErrSequenceRecoveryExhausted) {
		t.Fatalf("err = %v", err)
	}
	if mt := m.Metrics(); mt.Submitted != 3 || mt.Resyncs != 2 {
		t.Fatalf("metrics = %+v, want 3 attempts with 2 resyncs", mt)
	}
	assertExactlyOnce(t, ledger, 0)
}

func TestRedisSequenceStore(t *testing.T) {
	mr := miniredis.RunT(t)
	s := NewRedisSequenceStore(redis.NewClient(&redis.Options{Addr: mr.Addr()}), "test:")
	ctx := context.Background()
	if _, ok, err := s.Next(ctx, "A"); ok || err != nil {
		t.Fatalf("uninitialised Next = %v, %v", ok, err)
	}
	_ = s.Init(ctx, "A", 10)
	_ = s.Init(ctx, "A", 999) // must not overwrite
	if seq, ok, _ := s.Next(ctx, "A"); !ok || seq != 11 {
		t.Fatalf("Next = %d", seq)
	}
	_ = s.Reset(ctx, "A", 50)
	if seq, _, _ := s.Next(ctx, "A"); seq != 51 {
		t.Fatalf("Next after Reset = %d", seq)
	}
}
