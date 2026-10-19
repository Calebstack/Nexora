package fx

import (
	"sync"
	"testing"
	"time"
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
