package pipeline

import (
	"context"
	"sync"
	"time"
)

// TokenBucket implements a high-performance, thread-safe rate limiter.
type TokenBucket struct {
	rate       float64    // How many tokens are added per second
	capacity   float64    // Maximum burst capacity of the bucket
	tokens     float64    // Current available tokens
	lastRefill time.Time  // Last timestamp when tokens were recalculated
	mu         sync.Mutex // Protects the bucket state across concurrent calls
}

// NewTokenBucket initializes a rate limiter with a defined rate and burst depth.
func NewTokenBucket(rate float64, capacity float64) *TokenBucket {
	return &TokenBucket{
		rate:       rate,
		capacity:   capacity,
		tokens:     capacity, // Start completely full
		lastRefill: time.Now(),
	}
}

// Allow checks if a single token can be consumed immediately (Non-blocking / Load-shedding).
func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	if tb.tokens >= 1.0 {
		tb.tokens -= 1.0
		return true
	}
	return false
}

// Wait blocks until a token becomes available or the context is cancelled.
func (tb *TokenBucket) Wait(ctx context.Context) error {
	for {
		tb.mu.Lock()
		tb.refill()

		if tb.tokens >= 1.0 {
			tb.tokens -= 1.0
			tb.mu.Unlock()
			return nil
		}

		// Calculate how long we must wait for at least 1 token to generate
		tokensNeeded := 1.0 - tb.tokens
		waitDuration := time.Duration(tokensNeeded / tb.rate * float64(time.Second))
		tb.mu.Unlock()

		// Sleep or exit if context closes down mid-wait
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitDuration):
		}
	}
}

// refill calculates token generation based on delta time elapsed since last access.
func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.lastRefill = now

	tb.tokens += elapsed * tb.rate
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
}
