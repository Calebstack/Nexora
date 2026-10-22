package fx

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
)

func TestLockIsSingleUseUnderConcurrency(t *testing.T) {
	repo := NewLockRepository()
	now := time.Now()
	repo.Put(RateLock{ID: "q", ExpiresAt: now.Add(time.Second), MinAmountOut: 1})
	var wg sync.WaitGroup
	var mu sync.Mutex
	success := 0
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if repo.Consume("q", 1, now) == nil {
				mu.Lock()
				success++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if success != 1 {
		t.Fatalf("successful consumes=%d", success)
	}
}

func TestConsumeLockSingleUseUnder500Concurrency(t *testing.T) {
	repo := NewLockRepository()
	ctx := context.Background()
	now := time.Now().UTC()
	repo.Put(RateLock{
		ID:        "lock_500",
		ExpiresAt: now.Add(30 * time.Second),
	})

	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 0
	alreadyUsedCount := 0

	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			convID := "conv_req"
			err := repo.ConsumeLock(ctx, "lock_500", convID)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				successCount++
			} else if err == domain.ErrQuoteAlreadyUsed {
				alreadyUsedCount++
			}
		}(i)
	}
	wg.Wait()

	if successCount != 1 {
		t.Fatalf("expected exactly 1 successful consumption across 500 concurrent requests, got %d", successCount)
	}
	if alreadyUsedCount != 499 {
		t.Fatalf("expected exactly 499 ErrQuoteAlreadyUsed errors, got %d", alreadyUsedCount)
	}
}

func TestLockExpiresEvenOneMillisecondAfterDeadline(t *testing.T) {
	repo := NewLockRepository()
	deadline := time.Now().UTC()
	repo.Put(RateLock{
		ID:        "subsecond_lock",
		ExpiresAt: deadline,
	})

	// 1 millisecond after deadline must return ErrQuoteExpired
	afterDeadline := deadline.Add(1 * time.Millisecond)
	err := repo.Consume("subsecond_lock", 10.0, afterDeadline)
	if err != domain.ErrQuoteExpired {
		t.Fatalf("expected ErrQuoteExpired 1ms after deadline, got: %v", err)
	}

	// Verify expiration event was recorded
	events := repo.GetEvents("subsecond_lock")
	hasExpiredEvent := false
	for _, ev := range events {
		if ev.Type == LockEventExpired {
			hasExpiredEvent = true
			if ev.Timestamp.IsZero() {
				t.Fatal("expected non-zero timestamp on expiration event")
			}
		}
	}
	if !hasExpiredEvent {
		t.Fatal("expected LockEventExpired in event history")
	}
}

func TestLockEnforcesMinAmountOutSlippageProtection(t *testing.T) {
	repo := NewLockRepository()
	now := time.Now().UTC()
	repo.Put(RateLock{
		ID:           "slippage_lock",
		ExpiresAt:    now.Add(30 * time.Second),
		MinAmountOut: 100.0, // Caller requires at least 100
	})

	// Consuming with 99.99 must fail with ErrSlippageExceeded
	err := repo.Consume("slippage_lock", 99.99, now)
	if err != domain.ErrSlippageExceeded {
		t.Fatalf("expected ErrSlippageExceeded when amount < MinAmountOut, got: %v", err)
	}

	// Consuming with 100.00 must succeed
	err = repo.Consume("slippage_lock", 100.0, now)
	if err != nil {
		t.Fatalf("expected success when amount >= MinAmountOut, got: %v", err)
	}
}

func TestLockTracksMicrosecondTimestamps(t *testing.T) {
	repo := NewLockRepository()
	now := time.Now().UTC()
	repo.Put(RateLock{
		ID:        "microsecond_lock",
		ExpiresAt: now.Add(10 * time.Second),
	})

	err := repo.Consume("microsecond_lock", 50.0, now)
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}

	events := repo.GetEvents("microsecond_lock")
	if len(events) < 2 {
		t.Fatalf("expected at least 2 events (CREATED and CONSUMED), got: %d", len(events))
	}

	for _, ev := range events {
		if ev.Timestamp.Nanosecond()%1000 != 0 {
			t.Fatalf("timestamp %v is not truncated to microsecond precision", ev.Timestamp)
		}
	}
}

func TestLockReserveRecordsEventAndChecksExpiration(t *testing.T) {
	repo := NewLockRepository()
	now := time.Now().UTC()
	repo.Put(RateLock{
		ID:        "reservation_lock",
		TenantID:  "tenant_1",
		ExpiresAt: now.Add(10 * time.Second),
	})

	err := repo.Reserve(context.Background(), "reservation_lock", "tenant_1")
	if err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}

	events := repo.GetEvents("reservation_lock")
	foundReserved := false
	for _, ev := range events {
		if ev.Type == LockEventReserved {
			foundReserved = true
		}
	}
	if !foundReserved {
		t.Fatal("expected LockEventReserved event")
	}

	// Reserving after expiry fails
	repo.Put(RateLock{
		ID:        "expired_reserve_lock",
		TenantID:  "tenant_1",
		ExpiresAt: now.Add(-1 * time.Second),
	})
	err = repo.Reserve(context.Background(), "expired_reserve_lock", "tenant_1")
	if err != domain.ErrQuoteExpired {
		t.Fatalf("expected ErrQuoteExpired when reserving expired lock, got: %v", err)
	}
}


