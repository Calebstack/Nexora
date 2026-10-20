package settlement

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/shopspring/decimal"
	stellarnetwork "github.com/stellar/go/network"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/txnbuild"
)

func TestFeePolicy_PollsHorizonFeeStats(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fee-stats" {
			t.Errorf("expected GET /fee-stats, got %s", r.URL.Path)
		}
		count := hits.Add(1)
		usage := "0.45"
		if count >= 2 {
			usage = "0.92"
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"last_ledger":           "512000",
			"last_ledger_base_fee":  "100",
			"ledger_capacity_usage": usage,
			"fee_charged": map[string]string{
				"min":  "100",
				"mode": "250",
				"p10":  "100",
				"p50":  "250",
				"p90":  "800",
				"p95":  "1200",
				"p99":  "2500",
			},
		})
	}))
	defer srv.Close()

	// Verify default poll interval is 5 seconds
	defaultCfg := DefaultFeePolicyConfig()
	if defaultCfg.PollInterval != 5*time.Second {
		t.Fatalf("expected default PollInterval=5s, got %s", defaultCfg.PollInterval)
	}
	if defaultCfg.CongestionThreshold != 0.85 {
		t.Fatalf("expected default CongestionThreshold=0.85, got %v", defaultCfg.CongestionThreshold)
	}

	fetcher := NewHTTPFeeStatsFetcher(srv.URL, srv.Client())
	policy := NewFeePolicy(fetcher, FeePolicyConfig{
		PollInterval:             25 * time.Millisecond,
		CongestionThreshold:      0.85,
		ExponentialBase:          4.0,
		MinBaseFeeStroops:        100,
		MaxTransactionFeeStroops: 100_000,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy.Start(ctx)
	defer policy.Stop()

	// Wait for at least 2 poll cycles
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if policy.PollCount() >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if policy.PollCount() < 2 {
		t.Fatalf("expected at least 2 polls of /fee-stats, got %d", policy.PollCount())
	}

	snap, ok := policy.CurrentSnapshot()
	if !ok {
		t.Fatal("expected cached snapshot to be present")
	}
	if snap.LedgerCapacityUsage != 0.92 {
		t.Fatalf("expected updated LedgerCapacityUsage=0.92, got %v", snap.LedgerCapacityUsage)
	}
	if snap.MinFee != 100 || snap.ModeFee != 250 || snap.P99Fee != 2500 {
		t.Fatalf("unexpected parsed fee distribution: %+v", snap)
	}
}

func TestFeePolicy_ThreeTiersUnderNormalLoad(t *testing.T) {
	policy := NewFeePolicy(nil, DefaultFeePolicyConfig())
	policy.UpdateSnapshot(HorizonFeeStatsSnapshot{
		LastLedger:          1000,
		LastLedgerBaseFee:   100,
		LedgerCapacityUsage: 0.60, // 60% <= 85% threshold (no congestion multiplier)
		MinFee:              120,
		ModeFee:             350,
		P99Fee:              1800,
	})

	ctx := context.Background()

	// Economy -> min (120)
	econQuote, err := policy.QuoteFee(ctx, TierEconomy, 1)
	if err != nil {
		t.Fatalf("QuoteFee(TierEconomy) error: %v", err)
	}
	if econQuote.Congested || econQuote.Multiplier != 1.0 {
		t.Fatalf("expected no congestion at 60%% usage, got congested=%v multiplier=%v", econQuote.Congested, econQuote.Multiplier)
	}
	if econQuote.TotalFeeStroops != 120 {
		t.Fatalf("expected Economy tier fee=120, got %d", econQuote.TotalFeeStroops)
	}

	// Standard -> mode (350)
	stdQuote, err := policy.QuoteFee(ctx, TierStandard, 1)
	if err != nil {
		t.Fatalf("QuoteFee(TierStandard) error: %v", err)
	}
	if stdQuote.TotalFeeStroops != 350 {
		t.Fatalf("expected Standard tier fee=350, got %d", stdQuote.TotalFeeStroops)
	}

	// Priority -> p99 (1800)
	prioQuote, err := policy.QuoteFee(ctx, TierPriority, 1)
	if err != nil {
		t.Fatalf("QuoteFee(TierPriority) error: %v", err)
	}
	if prioQuote.TotalFeeStroops != 1800 {
		t.Fatalf("expected Priority tier fee=1800, got %d", prioQuote.TotalFeeStroops)
	}
}

func TestFeePolicy_ExponentialMultiplierAbove85PercentCapacity(t *testing.T) {
	policy := NewFeePolicy(nil, FeePolicyConfig{
		CongestionThreshold:      0.85,
		ExponentialBase:          4.0,
		MinBaseFeeStroops:        100,
		MaxTransactionFeeStroops: 500_000,
	})

	ctx := context.Background()

	// At exactly 85%, multiplier must still be 1.0
	policy.UpdateSnapshot(HorizonFeeStatsSnapshot{
		LastLedgerBaseFee:   100,
		LedgerCapacityUsage: 0.85,
		MinFee:              100,
		ModeFee:             500,
		P99Fee:              2000,
	})
	q85, err := policy.QuoteFee(ctx, TierStandard, 1)
	if err != nil {
		t.Fatalf("QuoteFee at 85%% error: %v", err)
	}
	if q85.Congested || q85.Multiplier != 1.0 || q85.TotalFeeStroops != 500 {
		t.Fatalf("at 85%% expected multiplier=1.0 and fee=500, got %+v", q85)
	}

	// At 90%, 95%, and 100%, multiplier must grow exponentially (convex growth)
	levels := []float64{0.90, 0.95, 1.00}
	quotes := make([]*TierFeeQuote, 0, len(levels))
	for _, usage := range levels {
		policy.UpdateSnapshot(HorizonFeeStatsSnapshot{
			LastLedgerBaseFee:   100,
			LedgerCapacityUsage: usage,
			MinFee:              100,
			ModeFee:             500,
			P99Fee:              2000,
		})
		q, err := policy.QuoteFee(ctx, TierStandard, 1)
		if err != nil {
			t.Fatalf("QuoteFee at %v error: %v", usage, err)
		}
		if !q.Congested || q.Multiplier <= 1.0 {
			t.Fatalf("expected congested with multiplier > 1.0 at usage=%v, got %+v", usage, q)
		}
		quotes = append(quotes, q)
	}

	// Verify strict monotonic increase: fee(85%) < fee(90%) < fee(95%) < fee(100%)
	if !(q85.TotalFeeStroops < quotes[0].TotalFeeStroops &&
		quotes[0].TotalFeeStroops < quotes[1].TotalFeeStroops &&
		quotes[1].TotalFeeStroops < quotes[2].TotalFeeStroops) {
		t.Fatalf("expected strictly increasing fees with congestion: 85%%=%d, 90%%=%d, 95%%=%d, 100%%=%d",
			q85.TotalFeeStroops, quotes[0].TotalFeeStroops, quotes[1].TotalFeeStroops, quotes[2].TotalFeeStroops)
	}

	// Verify exponential convexity: delta(95%->100%) > delta(90%->95%) > delta(85%->90%)
	delta1 := quotes[0].TotalFeeStroops - q85.TotalFeeStroops
	delta2 := quotes[1].TotalFeeStroops - quotes[0].TotalFeeStroops
	delta3 := quotes[2].TotalFeeStroops - quotes[1].TotalFeeStroops
	if !(delta3 > delta2 && delta2 > delta1) {
		t.Fatalf("expected exponential (convex) growth in fee deltas: d1=%d, d2=%d, d3=%d", delta1, delta2, delta3)
	}
}

func TestFeePolicy_HardMaximumCeilingPerTransaction(t *testing.T) {
	const ceiling = int64(10_000) // 10,000 stroops hard ceiling per transaction
	policy := NewFeePolicy(nil, FeePolicyConfig{
		CongestionThreshold:      0.85,
		ExponentialBase:          4.0,
		MinBaseFeeStroops:        100,
		MaxTransactionFeeStroops: ceiling,
	})

	// Extreme congestion: P99 = 8,000 stroops, capacity = 98% -> raw multiplied fee > 25,000 per op
	policy.UpdateSnapshot(HorizonFeeStatsSnapshot{
		LastLedgerBaseFee:   100,
		LedgerCapacityUsage: 0.98,
		MinFee:              500,
		ModeFee:             3000,
		P99Fee:              8000,
	})

	ctx := context.Background()

	// 1. Single-op Priority transaction must be capped at 10,000 stroops
	qSingle, err := policy.QuoteFee(ctx, TierPriority, 1)
	if err != nil {
		t.Fatalf("QuoteFee single op error: %v", err)
	}
	if !qSingle.CeilingCapped {
		t.Fatal("expected CeilingCapped=true for extreme priority fee")
	}
	if qSingle.TotalFeeStroops > ceiling {
		t.Fatalf("expected total fee <= ceiling (%d), got %d", ceiling, qSingle.TotalFeeStroops)
	}
	if qSingle.TotalFeeStroops != ceiling {
		t.Fatalf("expected single-op capped total fee=%d, got %d", ceiling, qSingle.TotalFeeStroops)
	}

	// 2. Multi-op transaction (4 operations) must also never exceed 10,000 total stroops (2,500 per op)
	qMulti, err := policy.QuoteFee(ctx, TierPriority, 4)
	if err != nil {
		t.Fatalf("QuoteFee multi op error: %v", err)
	}
	if !qMulti.CeilingCapped {
		t.Fatal("expected CeilingCapped=true for 4-op transaction")
	}
	if qMulti.TotalFeeStroops > ceiling {
		t.Fatalf("expected 4-op total fee <= %d, got %d", ceiling, qMulti.TotalFeeStroops)
	}
	if qMulti.PerOpFeeStroops != 2500 || qMulti.TotalFeeStroops != 10000 {
		t.Fatalf("expected per_op=2500 and total=10000, got per_op=%d total=%d", qMulti.PerOpFeeStroops, qMulti.TotalFeeStroops)
	}
}

func TestEngine_SubmitTransfer_UsesDynamicFeePolicy(t *testing.T) {
	src, dst := testWallets(t)
	tx := &domain.Transaction{
		ID:         "tx-prio-1",
		Status:     domain.StatusPending,
		FromWallet: src.ID,
		ToWallet:   dst.ID,
		Asset:      "XLM",
		Amount:     decimal.NewFromInt(25),
		Fee:        decimal.Zero,
		Tags:       []string{"priority"},
	}
	txRepo := newFakeTxRepo(tx)
	walletRepo := &fakeWalletRepo{wallets: map[string]*domain.Wallet{src.ID: src, dst.ID: dst}}

	var submittedBaseFee int64
	stl := &fakeStellarClient{
		submitFunc: func(stx *txnbuild.Transaction) (horizon.Transaction, error) {
			submittedBaseFee = stx.BaseFee()
			hash, _ := stx.HashHex(stellarnetwork.TestNetworkPassphrase)
			return horizon.Transaction{Hash: hash, Successful: true}, nil
		},
	}

	policy := NewFeePolicy(nil, FeePolicyConfig{
		CongestionThreshold:      0.85,
		ExponentialBase:          4.0,
		MinBaseFeeStroops:        100,
		MaxTransactionFeeStroops: 50_000,
	})
	// At 92.5% capacity (halfway between 85% and 100%), multiplier is 4^0.5 = 2.0
	// P99Fee = 1500 -> Priority fee = 1500 * 2.0 = 3000 stroops
	policy.UpdateSnapshot(HorizonFeeStatsSnapshot{
		LastLedgerBaseFee:   100,
		LedgerCapacityUsage: 0.925,
		MinFee:              100,
		ModeFee:             400,
		P99Fee:              1500,
	})

	e := newTestEngine(txRepo, walletRepo, nil, stl).WithFeePolicy(policy)
	if err := e.SubmitTransfer(context.Background(), "tx-prio-1"); err != nil {
		t.Fatalf("SubmitTransfer error: %v", err)
	}
	if submittedBaseFee != 3000 {
		t.Fatalf("expected submitted transaction BaseFee=3000 stroops under priority congestion, got %d", submittedBaseFee)
	}
}
