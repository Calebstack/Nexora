package settlement

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

// --- fake repository ---

type fakeChannelRepo struct {
	mu       sync.Mutex
	accounts []*domain.ChannelAccount
}

func newFakeChannelRepo(n int) *fakeChannelRepo {
	accounts := make([]*domain.ChannelAccount, n)
	for i := range accounts {
		accounts[i] = &domain.ChannelAccount{
			ID:            fakeID(i),
			PublicKey:     fakePubKey(i),
			Status:        domain.ChannelAccountActive,
			NativeBalance: decimal.NewFromInt(100),
			CreatedAt:     time.Now().UTC(),
			UpdatedAt:     time.Now().UTC(),
		}
	}
	return &fakeChannelRepo{accounts: accounts}
}

func fakeID(i int) string {
	return "id-" + string(rune('a'+i))
}

func fakePubKey(i int) string {
	return "GCHANNEL" + string(rune('A'+i))
}

func (r *fakeChannelRepo) ListActive(_ context.Context) ([]*domain.ChannelAccount, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*domain.ChannelAccount, len(r.accounts))
	for i, a := range r.accounts {
		cp := *a
		out[i] = &cp
	}
	return out, nil
}

func (r *fakeChannelRepo) UpdateBalance(_ context.Context, id string, balance decimal.Decimal) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, a := range r.accounts {
		if a.ID == id {
			a.NativeBalance = balance
			now := time.Now().UTC()
			a.BalanceCheckAt = &now
		}
	}
	return nil
}

func (r *fakeChannelRepo) UpdateStatus(_ context.Context, id string, status domain.ChannelAccountStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, a := range r.accounts {
		if a.ID == id {
			a.Status = status
		}
	}
	return nil
}

func (r *fakeChannelRepo) MarkUsed(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, a := range r.accounts {
		if a.ID == id {
			now := time.Now().UTC()
			a.LastUsedAt = &now
		}
	}
	return nil
}

func (r *fakeChannelRepo) ListNeedingBalanceCheck(_ context.Context, _ time.Time, limit int) ([]*domain.ChannelAccount, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*domain.ChannelAccount
	for _, a := range r.accounts {
		if a.Status != domain.ChannelAccountActive {
			continue
		}
		cp := *a
		out = append(out, &cp)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// --- helpers ---

func newTestPool(t *testing.T, n int) (*ChannelPool, *fakeChannelRepo, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	repo := newFakeChannelRepo(n)
	cfg := DefaultChannelPoolConfig()
	cfg.MaxLeaseWait = 200 * time.Millisecond
	pool := NewChannelPool(repo, rdb, nil, cfg)
	ctx := context.Background()
	if err := pool.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	return pool, repo, mr
}

// --- tests ---

func TestChannelPool_AcquireAndRelease(t *testing.T) {
	pool, _, _ := newTestPool(t, 3)
	ctx := context.Background()

	lease, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if lease.Account.PublicKey == "" {
		t.Fatal("lease has empty public key")
	}

	// The same pool should hand out a different channel.
	lease2, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire second: %v", err)
	}
	if lease2.Account.PublicKey == lease.Account.PublicKey {
		t.Fatal("pool handed out the same channel twice")
	}

	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	// Double release is safe.
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("double release: %v", err)
	}
	if err := lease2.Release(ctx); err != nil {
		t.Fatalf("release second: %v", err)
	}

	m := pool.Metrics()
	if m.Acquired != 2 {
		t.Fatalf("acquired = %d, want 2", m.Acquired)
	}
	if m.Released != 2 {
		t.Fatalf("released = %d, want 2", m.Released)
	}
}

func TestChannelPool_ExhaustionAndRecovery(t *testing.T) {
	pool, _, _ := newTestPool(t, 2)
	ctx := context.Background()

	l1, _ := pool.Acquire(ctx)
	l2, _ := pool.Acquire(ctx)

	// Pool has 2 accounts, both leased — next acquire should fail.
	_, err := pool.Acquire(ctx)
	if err == nil {
		t.Fatal("expected exhaustion error")
	}
	if err != ErrPoolExhausted {
		t.Fatalf("got %v, want ErrPoolExhausted", err)
	}
	if pool.Metrics().Exhausted != 1 {
		t.Fatal("exhausted counter not incremented")
	}

	// Release one; the pool should recover.
	_ = l1.Release(ctx)
	lease, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	_ = lease.Release(ctx)
	_ = l2.Release(ctx)
}

func TestChannelPool_ConcurrentAcquire(t *testing.T) {
	const poolSize = 50
	const goroutines = 200

	pool, _, _ := newTestPool(t, poolSize)
	ctx := context.Background()

	var wg sync.WaitGroup
	var acquired atomic.Int64
	var exhausted atomic.Int64

	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			lease, err := pool.Acquire(ctx)
			if err != nil {
				exhausted.Add(1)
				return
			}
			acquired.Add(1)
			// Hold the lease briefly to simulate work.
			time.Sleep(time.Millisecond)
			_ = lease.Release(ctx)
		}()
	}
	wg.Wait()

	// At least poolSize should have succeeded (the first wave), and some
	// may have succeeded again after releases.
	if acquired.Load() < int64(poolSize) {
		t.Fatalf("only %d goroutines acquired a lease, want >= %d", acquired.Load(), poolSize)
	}
	t.Logf("acquired=%d exhausted=%d (pool=%d goroutines=%d)",
		acquired.Load(), exhausted.Load(), poolSize, goroutines)
}

func TestChannelPool_NilRedis(t *testing.T) {
	repo := newFakeChannelRepo(3)
	cfg := DefaultChannelPoolConfig()
	cfg.MaxLeaseWait = 100 * time.Millisecond
	pool := NewChannelPool(repo, nil, nil, cfg)
	ctx := context.Background()
	_ = pool.Refresh(ctx)

	lease, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire with nil redis: %v", err)
	}
	_ = lease.Release(ctx)
}

func TestChannelPool_ShutdownRejectsAcquire(t *testing.T) {
	pool, _, _ := newTestPool(t, 3)
	pool.Shutdown()

	_, err := pool.Acquire(context.Background())
	if err != ErrPoolClosed {
		t.Fatalf("got %v, want ErrPoolClosed", err)
	}
}

func TestChannelPool_Refresh(t *testing.T) {
	pool, repo, _ := newTestPool(t, 2)
	ctx := context.Background()

	if pool.Size() != 2 {
		t.Fatalf("pool size = %d, want 2", pool.Size())
	}

	// Add a third account and refresh.
	repo.mu.Lock()
	repo.accounts = append(repo.accounts, &domain.ChannelAccount{
		ID:            "id-new",
		PublicKey:     "GNEWCHANNEL",
		Status:        domain.ChannelAccountActive,
		NativeBalance: decimal.NewFromInt(50),
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
	})
	repo.mu.Unlock()

	if err := pool.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if pool.Size() != 3 {
		t.Fatalf("pool size after refresh = %d, want 3", pool.Size())
	}
}

func TestChannelPool_RecycleIdle(t *testing.T) {
	pool, repo, _ := newTestPool(t, 3)
	ctx := context.Background()

	// Mark one account as idle (last used long ago).
	old := time.Now().Add(-48 * time.Hour)
	repo.mu.Lock()
	repo.accounts[0].LastUsedAt = &old
	repo.mu.Unlock()
	_ = pool.Refresh(ctx)

	pool.recycleIdle(ctx)

	// The idle account should have been transitioned to draining.
	repo.mu.Lock()
	status := repo.accounts[0].Status
	repo.mu.Unlock()

	if status != domain.ChannelAccountDraining {
		t.Fatalf("expected draining, got %s", status)
	}
	if pool.Metrics().Recycled != 1 {
		t.Fatal("recycled counter not incremented")
	}
}

func TestChannelPool_MetricsPoolSizeAndLeases(t *testing.T) {
	pool, _, _ := newTestPool(t, 5)
	ctx := context.Background()

	m := pool.Metrics()
	if m.PoolSize != 5 {
		t.Fatalf("pool size = %d, want 5", m.PoolSize)
	}
	if m.ActiveLeases != 0 {
		t.Fatalf("active leases = %d, want 0", m.ActiveLeases)
	}

	leases := make([]*ChannelLease, 3)
	for i := range leases {
		l, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		leases[i] = l
	}

	m = pool.Metrics()
	if m.ActiveLeases != 3 {
		t.Fatalf("active leases = %d, want 3", m.ActiveLeases)
	}

	for _, l := range leases {
		_ = l.Release(ctx)
	}
	m = pool.Metrics()
	if m.ActiveLeases != 0 {
		t.Fatalf("active leases after release = %d, want 0", m.ActiveLeases)
	}
}
