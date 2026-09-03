package pipeline

import (
	"context"
	"testing"
	"time"
)

// TestTokenBucket_Allow_BasicBurst verifies that a bucket configured with a
// burst capacity of exactly 3 allows precisely 3 consecutive non-blocking calls
// before exhausting its token supply.
func TestTokenBucket_Allow_BasicBurst(t *testing.T) {
	// Rate of 1 token/sec, burst of 3 — starts completely full.
	tb := NewTokenBucket(1.0, 3.0)

	// First 3 calls must succeed (tokens available from burst capacity).
	for i := 1; i <= 3; i++ {
		if !tb.Allow() {
			t.Errorf("Call %d: expected Allow() = true (within burst capacity), got false", i)
		}
	}

	// 4th call must be rejected — bucket is now empty.
	if tb.Allow() {
		t.Error("Call 4: expected Allow() = false (bucket exhausted), got true")
	}
}

// TestTokenBucket_Refill_OverTime verifies that tokens regenerate correctly
// according to the configured rate after the bucket has been drained.
func TestTokenBucket_Refill_OverTime(t *testing.T) {
	// Rate of 10 tokens/sec, burst of 1 — drain it immediately.
	tb := NewTokenBucket(10.0, 1.0)

	// Drain the single starting token.
	if !tb.Allow() {
		t.Fatal("Initial Allow() should succeed on a full bucket")
	}

	// Bucket is now empty; immediate retry must fail.
	if tb.Allow() {
		t.Error("Allow() should fail immediately after draining the bucket")
	}

	// At 10 tokens/sec we must receive at least 1 new token after ~100ms.
	// Use 200ms for generous headroom against scheduler jitter.
	time.Sleep(200 * time.Millisecond)

	if !tb.Allow() {
		t.Error("Allow() should succeed after sufficient refill time has elapsed")
	}
}

// TestTokenBucket_Wait_ContextCancel verifies that Wait() returns the
// context's error immediately when the context is cancelled, even if no
// tokens are available.
func TestTokenBucket_Wait_ContextCancel(t *testing.T) {
	// Rate of 0.01 tokens/sec — effectively never refills during the test.
	tb := NewTokenBucket(0.01, 1.0)

	// Drain the single starting token so Wait() is forced to block.
	tb.Allow()

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately in a separate goroutine so Wait() has a chance to enter.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := tb.Wait(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("Wait() should return a non-nil error when context is cancelled")
	}
	// Should unblock well within 500ms — not hang until a full token refills.
	if elapsed > 500*time.Millisecond {
		t.Errorf("Wait() took too long to respect context cancellation: %v", elapsed)
	}
}

// TestTokenBucket_Wait_TokenAvailable verifies that Wait() returns nil and
// consumes a token when one is immediately available.
func TestTokenBucket_Wait_TokenAvailable(t *testing.T) {
	// Full bucket — Wait() should return immediately.
	tb := NewTokenBucket(1.0, 5.0)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if err := tb.Wait(ctx); err != nil {
		t.Errorf("Wait() returned unexpected error on a full bucket: %v", err)
	}
}

// TestTokenBucket_Concurrency exercises Allow() under heavy concurrent load to
// ensure the mutex correctly serialises access and no tokens are double-consumed.
func TestTokenBucket_Concurrency(t *testing.T) {
	burst := 50.0
	tb := NewTokenBucket(0.0, burst) // Rate 0 so no refill during the test.

	goroutines := 100
	results := make(chan bool, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			results <- tb.Allow()
		}()
	}

	accepted := 0
	for i := 0; i < goroutines; i++ {
		if <-results {
			accepted++
		}
	}

	// Exactly burst tokens should be consumed — no more, no less.
	if accepted != int(burst) {
		t.Errorf("Concurrency race: expected %d accepted calls, got %d", int(burst), accepted)
	}
}
