package fx

import (
	"context"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/shopspring/decimal"
)

func TestRateLockExpirationWorker_MemoryBatch(t *testing.T) {
	memStore := &memoryLockRepository{
		locks:  make(map[string]*RateLock),
		events: make(map[string][]RateLockEvent),
	}
	worker := NewMemoryExpirationWorker(memStore)

	now := time.Now().UTC()

	// 1. Expired unconsumed lock
	memStore.locks["lock-expired-1"] = &RateLock{
		ID:         "lock-expired-1",
		TenantID:   "tenant-1",
		FromAsset:  "USDC",
		ToAsset:    "EURC",
		LockedRate: decimal.NewFromFloat(0.92),
		Amount:     decimal.NewFromInt(100),
		ExpiresAt:  now.Add(-10 * time.Minute),
		Status:     domain.RateLockStatusActive,
	}

	// 2. Expired unconsumed lock 2
	memStore.locks["lock-expired-2"] = &RateLock{
		ID:         "lock-expired-2",
		TenantID:   "tenant-1",
		FromAsset:  "USDC",
		ToAsset:    "EURC",
		LockedRate: decimal.NewFromFloat(0.92),
		Amount:     decimal.NewFromInt(200),
		ExpiresAt:  now.Add(-5 * time.Minute),
		Status:     domain.RateLockStatusActive,
	}

	// 3. Active unexpired lock (must NOT expire)
	memStore.locks["lock-active-future"] = &RateLock{
		ID:         "lock-active-future",
		TenantID:   "tenant-1",
		FromAsset:  "USDC",
		ToAsset:    "EURC",
		LockedRate: decimal.NewFromFloat(0.92),
		Amount:     decimal.NewFromInt(300),
		ExpiresAt:  now.Add(10 * time.Minute),
		Status:     domain.RateLockStatusActive,
	}

	// 4. Consumed lock (must NOT expire)
	consumedAt := now.Add(-15 * time.Minute)
	consumedBy := "conv-1"
	memStore.locks["lock-consumed"] = &RateLock{
		ID:         "lock-consumed",
		TenantID:   "tenant-1",
		FromAsset:  "USDC",
		ToAsset:    "EURC",
		LockedRate: decimal.NewFromFloat(0.92),
		Amount:     decimal.NewFromInt(400),
		ExpiresAt:  now.Add(-10 * time.Minute),
		ConsumedAt: &consumedAt,
		ConsumedBy: &consumedBy,
		Status:     domain.RateLockStatusConsumed,
	}

	ctx := context.Background()

	// Run expiration batch
	count, err := worker.ExpireBatch(ctx, 100)
	if err != nil {
		t.Fatalf("unexpected error expiring batch: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 expired locks, got %d", count)
	}

	// Verify locks in store
	if _, exists := memStore.locks["lock-expired-1"]; exists {
		t.Errorf("expected lock-expired-1 to be cleaned up")
	}
	if _, exists := memStore.locks["lock-expired-2"]; exists {
		t.Errorf("expected lock-expired-2 to be cleaned up")
	}
	if _, exists := memStore.locks["lock-active-future"]; !exists {
		t.Errorf("expected lock-active-future to remain active")
	}
	if _, exists := memStore.locks["lock-consumed"]; !exists {
		t.Errorf("expected lock-consumed to remain intact")
	}

	// Verify telemetry
	tel, err := worker.GetTelemetry(ctx)
	if err != nil {
		t.Fatalf("unexpected error getting telemetry: %v", err)
	}
	if tel.ExpiredCount != 2 {
		t.Errorf("expected telemetry expired count 2, got %d", tel.ExpiredCount)
	}
}

func TestRateLockExpirationWorker_BatchSizeLimit(t *testing.T) {
	memStore := &memoryLockRepository{
		locks:  make(map[string]*RateLock),
		events: make(map[string][]RateLockEvent),
	}
	worker := NewMemoryExpirationWorker(memStore)

	now := time.Now().UTC()
	for i := 0; i < 250; i++ {
		id := "lock-" + decimal.NewFromInt(int64(i)).String()
		memStore.locks[id] = &RateLock{
			ID:         id,
			TenantID:   "tenant-1",
			ExpiresAt:  now.Add(-time.Duration(i+1) * time.Minute),
			Status:     domain.RateLockStatusActive,
		}
	}

	ctx := context.Background()

	// Atomic batch of 100
	count1, err := worker.ExpireBatch(ctx, 100)
	if err != nil {
		t.Fatalf("batch 1 failed: %v", err)
	}
	if count1 != 100 {
		t.Fatalf("expected batch 1 to expire exactly 100 locks, got %d", count1)
	}

	// Second batch of 100
	count2, err := worker.ExpireBatch(ctx, 100)
	if err != nil {
		t.Fatalf("batch 2 failed: %v", err)
	}
	if count2 != 100 {
		t.Fatalf("expected batch 2 to expire exactly 100 locks, got %d", count2)
	}

	// Remaining 50
	count3, err := worker.ExpireBatch(ctx, 100)
	if err != nil {
		t.Fatalf("batch 3 failed: %v", err)
	}
	if count3 != 50 {
		t.Fatalf("expected batch 3 to expire remaining 50 locks, got %d", count3)
	}

	// Drain check
	count4, err := worker.ExpireBatch(ctx, 100)
	if err != nil {
		t.Fatalf("batch 4 failed: %v", err)
	}
	if count4 != 0 {
		t.Fatalf("expected 0 locks once drained, got %d", count4)
	}
}
