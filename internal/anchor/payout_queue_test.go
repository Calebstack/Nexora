package anchor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/crypto"
	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/tenant"
	"github.com/go-chi/chi/v5"
	"github.com/shopspring/decimal"
)

const (
	payoutUSDCIssuer = "GUSDCISSUER"
	payoutEURCIssuer = "GEURCISSUER"
)

var payoutKey = []byte("abcdefghijklmnopqrstuvwxyz012345")

// memPayoutStore mirrors PayoutRepo semantics in memory.
type memPayoutStore struct {
	mu       sync.Mutex
	balances map[string]decimal.Decimal // asset -> balance
	held     map[string]decimal.Decimal
	batches  map[string]*PayoutBatch // key -> batch
	payouts  map[string]*Payout
	events   map[string]bool
	holds    [][]BalanceHold
}

func newMemPayoutStore() *memPayoutStore {
	return &memPayoutStore{
		balances: map[string]decimal.Decimal{"USDC": decimal.NewFromInt(1000), "EURC": decimal.NewFromInt(500)},
		held:     map[string]decimal.Decimal{},
		batches:  map[string]*PayoutBatch{},
		payouts:  map[string]*Payout{},
		events:   map[string]bool{},
	}
}

func (s *memPayoutStore) CreateBatch(_ context.Context, b *PayoutBatch, holds []BalanceHold) (*PayoutBatch, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.batches[b.TenantID+"/"+b.IdempotencyKey]; ok {
		return existing, false, nil
	}
	for _, h := range holds {
		if s.held[h.AssetCode].Add(h.Amount).GreaterThan(s.balances[h.AssetCode]) {
			return nil, false, domain.ErrInsufficientBalance
		}
	}
	for _, h := range holds {
		s.held[h.AssetCode] = s.held[h.AssetCode].Add(h.Amount)
	}
	s.holds = append(s.holds, holds)
	s.batches[b.TenantID+"/"+b.IdempotencyKey] = b
	for _, p := range b.Payouts {
		cp := *p
		s.payouts[p.ID] = &cp
	}
	return b, true, nil
}

func (s *memPayoutStore) GetBatch(_ context.Context, tenantID, batchID string) (*PayoutBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.batches {
		if b.ID == batchID && b.TenantID == tenantID {
			return b, nil
		}
	}
	return nil, ErrPayoutBatchNotFound
}

func (s *memPayoutStore) ClaimForPartner(_ context.Context, partner string, maxInFlight int, now, leaseUntil time.Time) ([]*Payout, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inFlight := 0
	var due []*Payout
	for _, p := range s.payouts {
		if p.Partner != partner {
			continue
		}
		if p.Status == PayoutSubmitting {
			inFlight++
		} else if p.Status == PayoutQueued && !p.NextAttemptAt.After(now) {
			due = append(due, p)
		}
	}
	sort.Slice(due, func(i, j int) bool { return due[i].Reference < due[j].Reference })
	if slots := maxInFlight - inFlight; len(due) > slots {
		due = due[:max(slots, 0)]
	}
	out := make([]*Payout, 0, len(due))
	for _, p := range due {
		p.Status, p.Attempts = PayoutSubmitting, p.Attempts+1
		cp := *p
		out = append(out, &cp)
	}
	return out, nil
}

func (s *memPayoutStore) MarkSubmitted(_ context.Context, id, ref string, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.payouts[id]
	p.ProviderRef = ref
	if p.Status == PayoutSubmitting {
		p.Status = PayoutSubmitted
	}
	return nil
}

func (s *memPayoutStore) Reschedule(_ context.Context, id string, next time.Time, lastErr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.payouts[id]
	p.Status, p.NextAttemptAt, p.LastError = PayoutQueued, next, lastErr
	return nil
}

func (s *memPayoutStore) ApplyEvent(_ context.Context, e PayoutEvent) (*Payout, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.payouts[e.PayoutID]
	if !ok || p.Partner != e.Partner {
		return nil, false, ErrPayoutNotFound
	}
	key := e.Partner + "/" + e.EventID
	if s.events[key] {
		cp := *p
		return &cp, false, nil
	}
	next, err := NextPayoutState(p.Status, e.Outcome)
	if err != nil {
		return nil, false, err
	}
	s.events[key] = true
	p.Status, p.Reservation = next.Status, next.Reservation
	if next.Release {
		s.held[p.SourceAsset] = s.held[p.SourceAsset].Sub(p.SourceAmount)
	}
	cp := *p
	return &cp, true, nil
}

func (s *memPayoutStore) payout(ref string) Payout {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.payouts {
		if p.Reference == ref {
			return *p
		}
	}
	panic("no payout " + ref)
}

func (s *memPayoutStore) heldUSDC() decimal.Decimal {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.held["USDC"]
}

// fakePartner disburses according to a per-reference script.
type fakePartner struct {
	name       string
	currencies []string
	outcome    map[string]error // by reference; nil = accepted
	delay      time.Duration
	inFlight   atomic.Int32
	peak       atomic.Int32
	seen       sync.Map // reference -> PayoutBeneficiary
}

func (f *fakePartner) Name() string                  { return f.name }
func (f *fakePartner) SupportedCurrencies() []string { return f.currencies }

func (f *fakePartner) Disburse(_ context.Context, p *Payout, b PayoutBeneficiary) (string, error) {
	n := f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	for {
		if peak := f.peak.Load(); n <= peak || f.peak.CompareAndSwap(peak, n) {
			break
		}
	}
	time.Sleep(f.delay)
	f.seen.Store(p.Reference, b)
	if err := f.outcome[p.Reference]; err != nil {
		return "", err
	}
	return "REF-" + p.Reference, nil
}

// VerifyCallback accepts {"event_id","payout_id","outcome"} when the
// X-Test-Signature header is "valid".
func (f *fakePartner) VerifyCallback(_ context.Context, payload []byte, h http.Header) (*PayoutCallback, error) {
	if h.Get("X-Test-Signature") != "valid" {
		return nil, fmt.Errorf("%w: bad signature", ErrCallbackUnverified)
	}
	var body struct {
		EventID  string        `json:"event_id"`
		PayoutID string        `json:"payout_id"`
		Outcome  PayoutOutcome `json:"outcome"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCallbackUnverified, err)
	}
	return &PayoutCallback{EventID: body.EventID, PayoutID: body.PayoutID, Outcome: body.Outcome}, nil
}

type payoutFixture struct {
	now     time.Time
	store   *memPayoutStore
	ng      *fakePartner
	za      *fakePartner
	queue   *PayoutQueue
	tenant  string
	wallet  string
	counter int
}

func newPayoutFixture(t *testing.T) *payoutFixture {
	t.Helper()
	f := &payoutFixture{
		now:    time.Date(2026, 10, 19, 9, 0, 0, 0, time.UTC),
		store:  newMemPayoutStore(),
		ng:     &fakePartner{name: "flutterwave", currencies: []string{"NGN", "KES", "GHS"}, outcome: map[string]error{}},
		za:     &fakePartner{name: "za-bank", currencies: []string{"ZAR"}, outcome: map[string]error{}},
		tenant: "tenant-1",
		wallet: "wallet-1",
	}
	q, err := NewPayoutQueue(f.store, []PayoutPartner{f.ng, f.za}, PayoutQueueConfig{
		EncryptionKey:      payoutKey,
		SourceAssets:       map[string]string{"USDC": payoutUSDCIssuer, "EURC": payoutEURCIssuer},
		PartnerConcurrency: map[string]int{"flutterwave": 2, "za-bank": 1},
		Now:                func() time.Time { return f.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	f.queue = q
	return f
}

func (f *payoutFixture) instruction(ref, partner, currency, dest, asset, src string) PayoutInstruction {
	return PayoutInstruction{
		Reference: ref, Partner: partner, DestinationCurrency: currency,
		DestinationAmount: decimal.RequireFromString(dest), SourceAsset: asset, SourceAmount: decimal.RequireFromString(src),
		Beneficiary: PayoutBeneficiary{AccountName: "Ada Obi", AccountNumber: "0123456789", BankCode: "058"},
	}
}

func (f *payoutFixture) submit(t *testing.T, in ...PayoutInstruction) *PayoutBatch {
	t.Helper()
	f.counter++
	b, created, err := f.queue.SubmitBatch(context.Background(), PayoutBatchRequest{
		TenantID: f.tenant, WalletID: f.wallet, IdempotencyKey: fmt.Sprintf("key-%d", f.counter), Instructions: in,
	})
	if err != nil || !created {
		t.Fatalf("SubmitBatch: %v created=%v", err, created)
	}
	return b
}

func (f *payoutFixture) process(t *testing.T, partner string) int {
	t.Helper()
	n, err := f.queue.ProcessPartner(context.Background(), partner)
	if err != nil {
		t.Fatalf("ProcessPartner(%s): %v", partner, err)
	}
	return n
}

func TestNextPayoutState(t *testing.T) {
	tests := []struct {
		from    PayoutStatus
		outcome PayoutOutcome
		to      PayoutStatus
		res     ReservationState
		release bool
		ok      bool
	}{
		{PayoutSubmitting, PayoutOutcomeSuccess, PayoutCompleted, ReservationCaptured, false, true},
		{PayoutSubmitted, PayoutOutcomeSuccess, PayoutCompleted, ReservationCaptured, false, true},
		{PayoutSubmitted, PayoutOutcomeFailure, PayoutFailed, ReservationReleased, true, true},
		{PayoutSubmitting, PayoutOutcomeFailure, PayoutFailed, ReservationReleased, true, true},
		{PayoutCompleted, PayoutOutcomeReversal, PayoutReversed, ReservationReleased, true, true},
		{PayoutQueued, PayoutOutcomeSuccess, "", "", false, false},
		{PayoutQueued, PayoutOutcomeFailure, "", "", false, false},
		{PayoutCompleted, PayoutOutcomeFailure, "", "", false, false},
		{PayoutFailed, PayoutOutcomeSuccess, "", "", false, false},
		{PayoutSubmitted, PayoutOutcomeReversal, "", "", false, false},
		{PayoutReversed, PayoutOutcomeReversal, "", "", false, false},
	}
	for _, tc := range tests {
		got, err := NextPayoutState(tc.from, tc.outcome)
		if tc.ok != (err == nil) {
			t.Errorf("%s+%s: err = %v", tc.from, tc.outcome, err)
			continue
		}
		if !tc.ok {
			if !errors.Is(err, ErrInvalidPayoutTransition) {
				t.Errorf("%s+%s: err = %v, want ErrInvalidPayoutTransition", tc.from, tc.outcome, err)
			}
			continue
		}
		if got.Status != tc.to || got.Reservation != tc.res || got.Release != tc.release {
			t.Errorf("%s+%s = %+v", tc.from, tc.outcome, got)
		}
	}
}

func TestPayoutQueue_SubmitBatchReservesPerAssetAcrossCorridors(t *testing.T) {
	f := newPayoutFixture(t)
	b := f.submit(t,
		f.instruction("ng-1", "flutterwave", "NGN", "150000.50", "USDC", "100.1234567"),
		f.instruction("ke-1", "flutterwave", "kes", "12900", "USDC", "100"),
		f.instruction("gh-1", "flutterwave", "GHS", "1500.5", "EURC", "120"),
		f.instruction("za-1", "za-bank", "ZAR", "1800.99", "USDC", "99.8765433"),
	)

	if len(b.Payouts) != 4 {
		t.Fatalf("payouts = %d", len(b.Payouts))
	}
	holds := f.store.holds[0]
	if len(holds) != 2 || holds[0].AssetCode != "EURC" || holds[1].AssetCode != "USDC" {
		t.Fatalf("holds = %+v, want one per asset in sorted order", holds)
	}
	if !holds[1].Amount.Equal(decimal.NewFromInt(300)) || holds[1].Issuer != payoutUSDCIssuer || !holds[0].Amount.Equal(decimal.NewFromInt(120)) {
		t.Fatalf("hold totals = %+v", holds)
	}

	byRef := map[string]*Payout{}
	for _, p := range b.Payouts {
		byRef[p.Reference] = p
	}
	ke := byRef["ke-1"]
	if ke.DestinationCurrency != "KES" || ke.Country != "KE" || ke.Status != PayoutQueued || ke.Reservation != ReservationHeld {
		t.Fatalf("ke payout = %+v", ke)
	}
	if bytes.Contains(ke.BeneficiaryCiphertext, []byte("0123456789")) {
		t.Fatal("beneficiary account number stored in plaintext")
	}
	plain, err := crypto.Decrypt(ke.BeneficiaryCiphertext, payoutKey)
	if err != nil || !bytes.Contains(plain, []byte("0123456789")) {
		t.Fatalf("decrypt beneficiary: %s %v", plain, err)
	}
}

func TestPayoutQueue_SubmitBatchValidation(t *testing.T) {
	f := newPayoutFixture(t)
	valid := f.instruction("r", "flutterwave", "NGN", "10", "USDC", "1")
	mutate := func(fn func(*PayoutInstruction)) []PayoutInstruction {
		in := valid
		fn(&in)
		return []PayoutInstruction{in}
	}
	tests := map[string][]PayoutInstruction{
		"unsupported corridor":    mutate(func(i *PayoutInstruction) { i.DestinationCurrency = "USD" }),
		"partner lacks corridor":  mutate(func(i *PayoutInstruction) { i.DestinationCurrency = "ZAR" }),
		"unknown partner":         mutate(func(i *PayoutInstruction) { i.Partner = "nope" }),
		"unsupported asset":       mutate(func(i *PayoutInstruction) { i.SourceAsset = "XLM" }),
		"fiat sub-minor unit":     mutate(func(i *PayoutInstruction) { i.DestinationAmount = decimal.RequireFromString("10.001") }),
		"stellar sub-stroop":      mutate(func(i *PayoutInstruction) { i.SourceAmount = decimal.RequireFromString("1.00000001") }),
		"non-positive amount":     mutate(func(i *PayoutInstruction) { i.SourceAmount = decimal.Zero }),
		"missing beneficiary":     mutate(func(i *PayoutInstruction) { i.Beneficiary.AccountNumber = "" }),
		"missing reference":       mutate(func(i *PayoutInstruction) { i.Reference = "" }),
		"duplicate reference":     {valid, valid},
		"empty batch":             {},
		"batch over maximum size": make([]PayoutInstruction, MaxPayoutBatchSize+1),
	}
	for name, ins := range tests {
		_, _, err := f.queue.SubmitBatch(context.Background(), PayoutBatchRequest{TenantID: f.tenant, WalletID: f.wallet, IdempotencyKey: name, Instructions: ins})
		if !errors.Is(err, ErrInvalidPayout) {
			t.Errorf("%s: err = %v, want ErrInvalidPayout", name, err)
		}
	}
	if len(f.store.batches) != 0 {
		t.Fatal("invalid batches must not reach the store")
	}
}

func TestPayoutQueue_IdempotencyAndInsufficientBalance(t *testing.T) {
	f := newPayoutFixture(t)
	req := PayoutBatchRequest{TenantID: f.tenant, WalletID: f.wallet, IdempotencyKey: "k1",
		Instructions: []PayoutInstruction{f.instruction("a", "flutterwave", "NGN", "10", "USDC", "600")}}

	first, created, err := f.queue.SubmitBatch(context.Background(), req)
	if err != nil || !created {
		t.Fatalf("first: %v", err)
	}
	again, created, err := f.queue.SubmitBatch(context.Background(), req)
	if err != nil || created || again.ID != first.ID {
		t.Fatalf("replay: created=%v err=%v", created, err)
	}
	if !f.store.heldUSDC().Equal(decimal.NewFromInt(600)) {
		t.Fatalf("held = %s, replay must not reserve twice", f.store.heldUSDC())
	}

	changed := req
	changed.Instructions = []PayoutInstruction{f.instruction("a", "flutterwave", "NGN", "10", "USDC", "601")}
	if _, _, err := f.queue.SubmitBatch(context.Background(), changed); !errors.Is(err, ErrIdempotencyKeyReused) {
		t.Fatalf("changed body err = %v", err)
	}

	over := req
	over.IdempotencyKey = "k2"
	over.Instructions = []PayoutInstruction{f.instruction("b", "flutterwave", "NGN", "10", "USDC", "400.0000001")}
	if _, _, err := f.queue.SubmitBatch(context.Background(), over); !errors.Is(err, domain.ErrInsufficientBalance) {
		t.Fatalf("over-reservation err = %v", err)
	}
}

func TestPayoutQueue_ProcessOutcomesAndRelease(t *testing.T) {
	f := newPayoutFixture(t)
	f.ng.outcome["rejected"] = fmt.Errorf("%w: invalid account", ErrPayoutRejected)
	f.ng.outcome["flaky"] = errors.New("partner timeout")
	f.submit(t,
		f.instruction("accepted", "flutterwave", "NGN", "1000", "USDC", "100"),
		f.instruction("flaky", "flutterwave", "NGN", "1000", "USDC", "100"),
	)
	f.submit(t, f.instruction("rejected", "flutterwave", "NGN", "1000", "USDC", "100"))
	ctx := context.Background()

	if n, err := f.queue.ProcessPartner(ctx, "flutterwave"); err != nil || n != 2 {
		t.Fatalf("first pass claimed %d, %v; want 2 (concurrency cap)", n, err)
	}
	if n, _ := f.queue.ProcessPartner(ctx, "flutterwave"); n != 1 {
		t.Fatalf("second pass claimed %d, want the remaining 1", n)
	}

	if p := f.store.payout("accepted"); p.Status != PayoutSubmitted || p.ProviderRef != "REF-accepted" {
		t.Fatalf("accepted = %+v", p)
	}
	b, _ := f.ng.seen.Load("accepted")
	if b.(PayoutBeneficiary).AccountNumber != "0123456789" {
		t.Fatal("partner did not receive the decrypted beneficiary")
	}
	if p := f.store.payout("rejected"); p.Status != PayoutFailed || p.Reservation != ReservationReleased {
		t.Fatalf("rejected = %+v", p)
	}
	flaky := f.store.payout("flaky")
	if flaky.Status != PayoutQueued || flaky.Reservation != ReservationHeld || !flaky.NextAttemptAt.Equal(f.now.Add(30*time.Second)) {
		t.Fatalf("flaky = %+v, want requeued in 30s with the hold kept", flaky)
	}
	if !f.store.heldUSDC().Equal(decimal.NewFromInt(200)) {
		t.Fatalf("held = %s, want 200 (only the rejected payout released)", f.store.heldUSDC())
	}

	// Backoff doubles per attempt.
	f.now = f.now.Add(30 * time.Second)
	f.process(t, "flutterwave")
	if p := f.store.payout("flaky"); !p.NextAttemptAt.Equal(f.now.Add(time.Minute)) || p.Attempts != 2 {
		t.Fatalf("second retry = %+v", p)
	}
}

func TestPayoutQueue_PartnerConcurrencyIsBounded(t *testing.T) {
	f := newPayoutFixture(t)
	f.za.delay = 10 * time.Millisecond
	var ins []PayoutInstruction
	for i := 0; i < 5; i++ {
		ins = append(ins, f.instruction(fmt.Sprintf("za-%d", i), "za-bank", "ZAR", "100", "USDC", "1"))
	}
	f.submit(t, ins...)
	for i := 0; i < 5; i++ {
		if n, _ := f.queue.ProcessPartner(context.Background(), "za-bank"); n != 1 {
			t.Fatalf("pass %d claimed %d, want 1", i, n)
		}
	}
	if f.za.peak.Load() != 1 {
		t.Fatalf("za-bank peak concurrency = %d, want 1", f.za.peak.Load())
	}
}

func TestPayoutQueue_CallbackReconciliation(t *testing.T) {
	f := newPayoutFixture(t)
	f.submit(t,
		f.instruction("ok", "flutterwave", "NGN", "1000", "USDC", "100"),
		f.instruction("bad", "flutterwave", "NGN", "1000", "USDC", "50"),
	)
	ctx := context.Background()
	f.process(t, "flutterwave")
	ok, bad := f.store.payout("ok"), f.store.payout("bad")
	valid := http.Header{"X-Test-Signature": []string{"valid"}}
	cb := func(event, id string, o PayoutOutcome) []byte {
		b, _ := json.Marshal(map[string]any{"event_id": event, "payout_id": id, "outcome": o})
		return b
	}

	if _, err := f.queue.HandleCallback(ctx, "flutterwave", cb("e0", ok.ID, PayoutOutcomeFailure), http.Header{}); !errors.Is(err, ErrCallbackUnverified) {
		t.Fatalf("unsigned callback err = %v", err)
	}
	if f.store.payout("ok").Status != PayoutSubmitted {
		t.Fatal("an unverified callback must not change state")
	}

	p, err := f.queue.HandleCallback(ctx, "flutterwave", cb("e1", ok.ID, PayoutOutcomeSuccess), valid)
	if err != nil || p.Status != PayoutCompleted || p.Reservation != ReservationCaptured {
		t.Fatalf("success = %+v %v", p, err)
	}
	if p, err = f.queue.HandleCallback(ctx, "flutterwave", cb("e1", ok.ID, PayoutOutcomeSuccess), valid); err != nil || p.Status != PayoutCompleted {
		t.Fatalf("duplicate = %+v %v", p, err)
	}

	if _, err := f.queue.HandleCallback(ctx, "flutterwave", cb("e2", bad.ID, PayoutOutcomeFailure), valid); err != nil {
		t.Fatal(err)
	}
	if !f.store.heldUSDC().Equal(decimal.NewFromInt(100)) {
		t.Fatalf("held = %s, want 100 after the verified failure released 50", f.store.heldUSDC())
	}

	if _, err := f.queue.HandleCallback(ctx, "flutterwave", cb("e3", ok.ID, PayoutOutcomeReversal), valid); err != nil {
		t.Fatal(err)
	}
	if p := f.store.payout("ok"); p.Status != PayoutReversed || p.Reservation != ReservationReleased || !f.store.heldUSDC().IsZero() {
		t.Fatalf("reversal = %+v held=%s", p, f.store.heldUSDC())
	}

	if _, err := f.queue.HandleCallback(ctx, "flutterwave", cb("e4", bad.ID, PayoutOutcomeSuccess), valid); !errors.Is(err, ErrInvalidPayoutTransition) {
		t.Fatalf("success after failure err = %v", err)
	}
	if _, err := f.queue.HandleCallback(ctx, "za-bank", cb("e5", bad.ID, PayoutOutcomeFailure), http.Header{"X-Test-Signature": []string{"valid"}}); !errors.Is(err, ErrPayoutNotFound) {
		t.Fatalf("cross-partner err = %v", err)
	}
}

func TestPayoutHandler(t *testing.T) {
	f := newPayoutFixture(t)
	h := NewPayoutHandler(f.queue)
	r := chi.NewRouter()
	r.Route("/payouts", h.Routes())
	r.Route("/payout-callbacks", h.CallbackRoutes())

	do := func(method, path, key string, body any, headers http.Header) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest(method, path, bytes.NewReader(raw))
		req = req.WithContext(tenant.WithID(req.Context(), f.tenant))
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		for k, v := range headers {
			req.Header[k] = v
		}
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		return rec
	}
	body := submitBatchRequest{WalletID: f.wallet, Instructions: []PayoutInstruction{f.instruction("h-1", "flutterwave", "NGN", "5000", "USDC", "10")}}

	if rec := do(http.MethodPost, "/payouts/batches", "", body, nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("missing key: %d", rec.Code)
	}
	rec := do(http.MethodPost, "/payouts/batches", "idem-1", body, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body)
	}
	var created batchResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &created)
	if len(created.Payouts) != 1 || created.Payouts[0].DestinationAmount != "5000.00" || created.Payouts[0].SourceAmount != "10.0000000" || created.Payouts[0].Reservation != "held" {
		t.Fatalf("created = %+v", created)
	}
	if bytes.Contains(rec.Body.Bytes(), []byte("0123456789")) {
		t.Fatal("response leaks the beneficiary account number")
	}
	if rec := do(http.MethodPost, "/payouts/batches", "idem-1", body, nil); rec.Code != http.StatusOK {
		t.Fatalf("replay: %d", rec.Code)
	}
	body.Instructions[0].SourceAmount = decimal.NewFromInt(2000)
	if rec := do(http.MethodPost, "/payouts/batches", "idem-1", body, nil); rec.Code != http.StatusConflict {
		t.Fatalf("reused key: %d", rec.Code)
	}
	if rec := do(http.MethodPost, "/payouts/batches", "idem-2", body, nil); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("insufficient: %d %s", rec.Code, rec.Body)
	}
	if rec := do(http.MethodGet, "/payouts/batches/"+created.ID, "", nil, nil); rec.Code != http.StatusOK {
		t.Fatalf("get: %d", rec.Code)
	}
	if rec := do(http.MethodGet, "/payouts/batches/not-a-uuid", "", nil, nil); rec.Code != http.StatusNotFound {
		t.Fatalf("bad id: %d", rec.Code)
	}

	f.process(t, "flutterwave")
	payoutID := created.Payouts[0].ID
	signed := http.Header{"X-Test-Signature": []string{"valid"}}
	if rec := do(http.MethodPost, "/payout-callbacks/flutterwave", "", map[string]any{"event_id": "x", "payout_id": payoutID, "outcome": "success"}, nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unsigned callback: %d", rec.Code)
	}
	if rec := do(http.MethodPost, "/payout-callbacks/flutterwave", "", map[string]any{"event_id": "x", "payout_id": payoutID, "outcome": "success"}, signed); rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte(`"completed"`)) {
		t.Fatalf("callback: %d %s", rec.Code, rec.Body)
	}
	if rec := do(http.MethodPost, "/payout-callbacks/flutterwave", "", map[string]any{"event_id": "y", "payout_id": payoutID, "outcome": "failure"}, signed); rec.Code != http.StatusConflict {
		t.Fatalf("invalid transition: %d", rec.Code)
	}
}
