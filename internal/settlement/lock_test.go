package settlement

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/queue"
	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	stellarnetwork "github.com/stellar/go/network"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/txnbuild"
)

func newTestRedlockCluster(t *testing.T, nodeCount int, cfg RedlockConfig) (*RedlockManager, []*miniredis.Miniredis, []*redis.Client) {
	t.Helper()
	servers := make([]*miniredis.Miniredis, nodeCount)
	clients := make([]*redis.Client, nodeCount)

	for i := 0; i < nodeCount; i++ {
		srv := miniredis.RunT(t)
		servers[i] = srv
		clients[i] = redis.NewClient(&redis.Options{
			Addr:         srv.Addr(),
			DialTimeout:  50 * time.Millisecond,
			ReadTimeout:  50 * time.Millisecond,
			WriteTimeout: 50 * time.Millisecond,
			MaxRetries:   0,
		})
		t.Cleanup(func() {
			_ = clients[i].Close()
		})
	}

	mgr, err := NewRedlockManagerFromClients(clients, cfg)
	if err != nil {
		t.Fatalf("NewRedlockManagerFromClients failed: %v", err)
	}
	return mgr, servers, clients
}

func TestRedlockManager_MultiNodeQuorumAndMillisecondTTL(t *testing.T) {
	cfg := DefaultRedlockConfig()
	cfg.TTL = 2500 * time.Millisecond

	mgr, servers, _ := newTestRedlockCluster(t, 5, cfg)
	if mgr.NodeCount() != 5 || mgr.Quorum() != 3 {
		t.Fatalf("expected 5 nodes and quorum=3, got nodes=%d quorum=%d", mgr.NodeCount(), mgr.Quorum())
	}

	ctx := context.Background()
	lock, err := mgr.AcquireTransferLock(ctx, "tx-redlock-001")
	if err != nil {
		t.Fatalf("expected lock acquisition to succeed, got %v", err)
	}
	if lock.AcquiredNodes != 5 {
		t.Fatalf("expected lock acquired on all 5 nodes, got %d", lock.AcquiredNodes)
	}
	if lock.ValidityTime <= 0 || lock.ValidityTime > cfg.TTL {
		t.Fatalf("unexpected validity time: %v", lock.ValidityTime)
	}

	// Verify key and millisecond TTL exist on all 5 Redis instances
	key := mgr.LockKey("tx-redlock-001")
	for i, srv := range servers {
		if !srv.Exists(key) {
			t.Fatalf("expected key %s to exist on node %d", key, i)
		}
		ttl := srv.TTL(key)
		if ttl <= 0 || ttl > cfg.TTL {
			t.Fatalf("expected millisecond TTL in (0, %v] on node %d, got %v", cfg.TTL, i, ttl)
		}
	}

	// A second concurrent attempt for the same transfer ID must fail with ErrLockNotAcquired
	_, err = mgr.AcquireTransferLock(ctx, "tx-redlock-001")
	if !errors.Is(err, ErrLockNotAcquired) {
		t.Fatalf("expected ErrLockNotAcquired on duplicate lock attempt, got %v", err)
	}

	// Release lock and verify all 5 nodes are unlocked
	if err := lock.Release(ctx); err != nil {
		t.Fatalf("expected lock release to succeed, got %v", err)
	}
	for i, srv := range servers {
		if srv.Exists(key) {
			t.Fatalf("expected key %s to be deleted on node %d after Release", key, i)
		}
	}
}

func TestWorker_Redlock_PreventsDoubleSpendAcrossHorizontalScaleOut(t *testing.T) {
	cfg := DefaultRedlockConfig()
	cfg.TTL = 5000 * time.Millisecond
	mgr, _, _ := newTestRedlockCluster(t, 5, cfg)

	src, dst := testWallets(t)
	tx := &domain.Transaction{
		ID:         "tx-scaleout-1",
		Status:     domain.StatusPending,
		FromWallet: src.ID,
		ToWallet:   dst.ID,
		Asset:      "XLM",
		Amount:     decimal.NewFromInt(25),
		Fee:        decimal.Zero,
	}
	txRepo := newFakeTxRepo(tx)
	walletRepo := &fakeWalletRepo{wallets: map[string]*domain.Wallet{src.ID: src, dst.ID: dst}}

	var submitCount atomic.Int32
	stl := &fakeStellarClient{
		submitFunc: func(stx *txnbuild.Transaction) (horizon.Transaction, error) {
			// Simulate network broadcast time so all horizontal workers overlap
			time.Sleep(40 * time.Millisecond)
			submitCount.Add(1)
			hash, _ := stx.HashHex(stellarnetwork.TestNetworkPassphrase)
			return horizon.Transaction{
				Hash:       hash,
				Successful: true,
			}, nil
		},
	}

	engine := newTestEngine(txRepo, walletRepo, nil, stl)

	const numWorkers = 16
	workers := make([]*Worker, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers[i] = NewWorker(engine).WithLocker(mgr)
	}

	payloadBytes, _ := json.Marshal(queue.ProcessTransferPayload{TransactionID: tx.ID})
	task := asynq.NewTask(queue.TypeProcessTransfer, payloadBytes)

	var wg sync.WaitGroup
	var successWorkers atomic.Int32
	var lockDeniedWorkers atomic.Int32

	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		w := workers[i]
		go func() {
			defer wg.Done()
			err := w.HandleProcessTransfer(context.Background(), task)
			if err == nil {
				successWorkers.Add(1)
			} else if errors.Is(err, ErrLockNotAcquired) {
				lockDeniedWorkers.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := submitCount.Load(); got != 1 {
		t.Fatalf("expected exactly 1 Horizon submission across %d horizontal workers, got %d", numWorkers, got)
	}
	if got := successWorkers.Load(); got != 1 {
		t.Fatalf("expected exactly 1 worker to succeed, got %d", got)
	}
	if got := lockDeniedWorkers.Load(); got != numWorkers-1 {
		t.Fatalf("expected %d workers to be blocked by Redlock, got %d", numWorkers-1, got)
	}
	if got := txRepo.status(tx.ID); got != domain.StatusConfirmed {
		t.Fatalf("expected transaction status confirmed, got %s", got)
	}
}

func TestWorker_Redlock_AutoReleasesOnVerifiedInclusionAndTerminalFailure(t *testing.T) {
	cfg := DefaultRedlockConfig()
	cfg.TTL = 2000 * time.Millisecond
	mgr, servers, _ := newTestRedlockCluster(t, 5, cfg)

	src, dst := testWallets(t)
	txFail := &domain.Transaction{
		ID:         "tx-term-fail",
		Status:     domain.StatusPending,
		FromWallet: src.ID,
		ToWallet:   dst.ID,
		Asset:      "UNSUPPORTED_ASSET",
		Amount:     decimal.NewFromInt(10),
		Fee:        decimal.Zero,
	}
	txRepo := newFakeTxRepo(txFail)
	walletRepo := &fakeWalletRepo{wallets: map[string]*domain.Wallet{src.ID: src, dst.ID: dst}}
	engine := newTestEngine(txRepo, walletRepo, nil, &fakeStellarClient{})
	worker := NewWorker(engine).WithLocker(mgr)

	payloadBytes, _ := json.Marshal(queue.ProcessTransferPayload{TransactionID: txFail.ID})
	task := asynq.NewTask(queue.TypeProcessTransfer, payloadBytes)

	err := worker.HandleProcessTransfer(context.Background(), task)
	if err == nil {
		t.Fatal("expected terminal error for unsupported asset")
	}

	// Lock must be released automatically upon hard terminal failure
	lockKey := mgr.LockKey(txFail.ID)
	for i, srv := range servers {
		if srv.Exists(lockKey) {
			t.Fatalf("expected lock %s to be automatically released on node %d after hard terminal failure", lockKey, i)
		}
	}
}

func TestRedlockManager_ChaosNodeFailovers(t *testing.T) {
	cfg := DefaultRedlockConfig()
	cfg.TTL = 3000 * time.Millisecond
	cfg.PerNodeTimeout = 40 * time.Millisecond

	mgr, servers, _ := newTestRedlockCluster(t, 5, cfg)
	ctx := context.Background()

	// Chaos Phase 1: Crash 2 out of 5 Redis nodes (minority failure).
	// Quorum is 3/5, so lock acquisition and release must still succeed without error.
	servers[0].Close()
	servers[1].Close()

	lock1, err := mgr.AcquireTransferLock(ctx, "tx-chaos-minority")
	if err != nil {
		t.Fatalf("expected Redlock acquisition to survive 2/5 node failover, got: %v", err)
	}
	if lock1.AcquiredNodes != 3 {
		t.Fatalf("expected lock acquired on 3 surviving nodes, got %d", lock1.AcquiredNodes)
	}

	// Concurrent worker must still be blocked by the 3 surviving nodes
	if _, err := mgr.AcquireTransferLock(ctx, "tx-chaos-minority"); !errors.Is(err, ErrLockNotAcquired) {
		t.Fatalf("expected concurrent worker to be denied lock during 2/5 node failover, got %v", err)
	}

	if err := lock1.Release(ctx); err != nil {
		t.Fatalf("expected release to succeed with 3/5 nodes healthy, got %v", err)
	}

	// Chaos Phase 2: Crash a 3rd Redis node (3 out of 5 nodes down -> majority quorum lost).
	// Lock acquisition must fail and clean up any partial locks on the 2 surviving nodes.
	servers[2].Close()

	_, err = mgr.AcquireTransferLock(ctx, "tx-chaos-majority-down")
	if !errors.Is(err, ErrLockNotAcquired) {
		t.Fatalf("expected ErrLockNotAcquired when 3/5 nodes are down, got %v", err)
	}

	// Verify partial locks were cleaned up on the 2 surviving nodes (servers[3] and servers[4])
	majorityDownKey := mgr.LockKey("tx-chaos-majority-down")
	for _, idx := range []int{3, 4} {
		if servers[idx].Exists(majorityDownKey) {
			t.Fatalf("expected partial lock %s to be cleaned up on surviving node %d", majorityDownKey, idx)
		}
	}

	// Chaos Phase 3: Restart node 2 so 3/5 quorum is restored, and verify token safety
	// when a lock expires before an old holder calls Release().
	if err := servers[2].Restart(); err != nil {
		t.Fatalf("failed to restart miniredis node 2: %v", err)
	}

	lockA, err := mgr.AcquireTransferLockWithTTL(ctx, "tx-chaos-expiry", 150*time.Millisecond)
	if err != nil {
		t.Fatalf("expected lock acquisition after node recovery to succeed, got %v", err)
	}

	// Fast-forward time on the active miniredis nodes past lockA's 150ms TTL
	for _, idx := range []int{2, 3, 4} {
		servers[idx].FastForward(200 * time.Millisecond)
	}

	// Worker B acquires the lock for the same transfer ID after Worker A's TTL expired
	lockB, err := mgr.AcquireTransferLockWithTTL(ctx, "tx-chaos-expiry", 2000*time.Millisecond)
	if err != nil {
		t.Fatalf("expected Worker B to acquire lock after Worker A TTL expired, got %v", err)
	}

	// Delayed Worker A calls Release() — Lua token check must prevent deleting Worker B's lock!
	_ = lockA.Release(ctx)

	expiryKey := mgr.LockKey("tx-chaos-expiry")
	for _, idx := range []int{2, 3, 4} {
		val, err := servers[idx].Get(expiryKey)
		if err != nil || val != lockB.Token {
			t.Fatalf("expected Worker B's lock token %q to remain intact on node %d after stale Worker A release, got %q (err=%v)", lockB.Token, idx, val, err)
		}
	}

	if err := lockB.Release(ctx); err != nil {
		t.Fatalf("expected Worker B release to succeed, got %v", err)
	}
}
