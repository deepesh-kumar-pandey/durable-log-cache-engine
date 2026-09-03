package pipeline

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"log-cache-engine/cache"
)

// newTestPool is a helper that wires up a WAL + Cache and returns a WorkerPool
// pre-configured for testing (high rate limits unless overridden by the caller).
func newTestPool(wal *cache.WAL, workerCount, queueSize int, rate, burst float64) *WorkerPool {
	return NewWorkerPool(wal, cache.NewCache(1000), workerCount, queueSize, rate, burst)
}

// TestWorkerPool_ExecutionLifecycle verifies that workers successfully process
// submitted tasks, write state transitions to the WAL, and shut down cleanly.
func TestWorkerPool_ExecutionLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "pipeline_test.wal")

	wal, err := cache.NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to initialize WAL for pipeline testing: %v", err)
	}

	workerCount := 3
	queueSize := 10
	// Configured with high rates (100.0, 100.0) so the limiter doesn't artificially slow down execution lifecycle tests
	pool := newTestPool(wal, workerCount, queueSize, 100.0, 100.0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)

	taskCount := 5
	for i := 1; i <= taskCount; i++ {
		task := Task{
			ID:      fmt.Sprintf("TX-00%d", i),
			Payload: fmt.Appendf(nil, "payload_data_block_%d", i),
		}
		pool.Submit(task)
	}

	time.Sleep(500 * time.Millisecond)

	pool.Stop()
	cancel()

	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL instance: %v", err)
	}

	recoveryWAL, err := cache.NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to reopen WAL for tracking verification: %v", err)
	}
	defer recoveryWAL.Close()

	entries, err := recoveryWAL.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read back log records: %v", err)
	}

	startLogs := make(map[string]bool)
	commitLogs := make(map[string]bool)

	for _, entry := range entries {
		logStr := string(entry.Payload)
		if strings.HasPrefix(logStr, "START: ") {
			id := strings.TrimPrefix(logStr, "START: ")
			startLogs[id] = true
		} else if id, _, ok := ParseCommitRecord(logStr); ok {
			commitLogs[id] = true
		}
	}

	for i := 1; i <= taskCount; i++ {
		targetID := fmt.Sprintf("TX-00%d", i)

		if !startLogs[targetID] {
			t.Errorf("Pool tracking failure: Task %s never logged a START intent to disk", targetID)
		}
		if !commitLogs[targetID] {
			t.Errorf("Pool tracking failure: Task %s never logged a COMMIT state to disk", targetID)
		}
	}
}

// TestWorkerPool_PanicRecovery verifies that a poison-pill payload triggers a panic,
// records a CRASH state to the WAL, self-heals by replacing the dead worker,
// and gracefully completes subsequent healthy tasks.
func TestWorkerPool_PanicRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "pipeline_panic_test.wal")

	wal, err := cache.NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to initialize WAL for panic verification: %v", err)
	}

	// Configured with high rates (100.0, 100.0) to prevent throttling interference
	pool := newTestPool(wal, 2, 10, 100.0, 100.0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)

	pool.Submit(Task{ID: "TX-GOOD-1", Payload: fmt.Appendf(nil, "safe_payload_data")})
	pool.Submit(Task{ID: "TX-POISON", Payload: fmt.Appendf(nil, "TRIGGER_PANIC")})
	pool.Submit(Task{ID: "TX-GOOD-2", Payload: fmt.Appendf(nil, "post_recovery_payload")})

	time.Sleep(500 * time.Millisecond)

	pool.Stop()
	cancel()

	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL instance: %v", err)
	}

	recoveryWAL, err := cache.NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer recoveryWAL.Close()

	entries, err := recoveryWAL.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read back log records: %v", err)
	}

	var hasPoisonStart, hasPoisonCrash, hasGood2Commit bool

	for _, entry := range entries {
		logStr := string(entry.Payload)
		switch {
		case logStr == "START: TX-POISON":
			hasPoisonStart = true
		case logStr == "CRASH: TX-POISON":
			hasPoisonCrash = true
		case strings.HasPrefix(logStr, "COMMIT: TX-GOOD-2"):
			hasGood2Commit = true
		}
	}

	if !hasPoisonStart {
		t.Errorf("Resilience failure: Poisoned task was never registered into the WAL")
	}
	if !hasPoisonCrash {
		t.Errorf("Resilience failure: Worker crashed but failed to append a CRASH marker for TX-POISON")
	}
	if !hasGood2Commit {
		t.Errorf("Resilience failure: The pool completely died; replacement worker never processed the remainder of the queue")
	}
}

// TestWorkerPool_RateLimiting verifies that TrySubmit correctly drops bursts
// of tasks exceeding structural token capacities (Load Shedding).
func TestWorkerPool_RateLimiting(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "pipeline_rate_test.wal")

	wal, err := cache.NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to initialize WAL: %v", err)
	}
	defer wal.Close()

	// Configure a pool with a maximum burst capacity of exactly 3 tokens, refilling at 1 token/sec
	pool := newTestPool(wal, 1, 10, 1.0, 3.0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)
	defer pool.Stop()

	acceptedCount := 0
	rejectedCount := 0

	// Fire 6 swift bursts of requests. The first 3 should consume the burst depth, subsequent 3 must drop.
	for i := 1; i <= 6; i++ {
		task := Task{ID: fmt.Sprintf("TX-BURST-%d", i), Payload: fmt.Appendf(nil, "data")}
		if pool.TrySubmit(task) {
			acceptedCount++
		} else {
			rejectedCount++
		}
	}

	// We expect exactly 3 to leak through the bucket parameters and the rest to get shed safely.
	if acceptedCount != 3 {
		t.Errorf("Rate limiter error: Expected exactly 3 accepted tasks, got %d", acceptedCount)
	}
	if rejectedCount != 3 {
		t.Errorf("Rate limiter error: Expected exactly 3 rejected tasks, got %d", rejectedCount)
	}
}

// TestWorkerPool_TelemetryMetrics verifies that lock-free status tracking counters
// calculate processing metrics flawlessly across multiple concurrent worker loops.
func TestWorkerPool_TelemetryMetrics(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "pipeline_metrics_test.wal")

	wal, err := cache.NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to initialize WAL: %v", err)
	}
	defer wal.Close()

	// 2 workers, maximum rate of 2 tokens/sec, burst size 2
	pool := newTestPool(wal, 2, 10, 2.0, 2.0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	// Ingest combinations of safe, panic-inducing, and limited tasks
	pool.Submit(Task{ID: "TX-METRIC-OK", Payload: fmt.Appendf(nil, "clean")})
	pool.Submit(Task{ID: "TX-METRIC-PANIC", Payload: fmt.Appendf(nil, "TRIGGER_PANIC")})

	// Burst immediate requests to force a load shed event
	for i := 1; i <= 3; i++ {
		_ = pool.TrySubmit(Task{ID: fmt.Sprintf("TX-METRIC-SHED-%d", i), Payload: fmt.Appendf(nil, "burst")})
	}

	time.Sleep(200 * time.Millisecond)
	pool.Stop()

	stats := pool.GetStats()

	if stats.TasksProcessed != 1 {
		t.Errorf("Telemetry failure: Expected 1 successful process target, got %d", stats.TasksProcessed)
	}
	if stats.PanicIncidents != 1 {
		t.Errorf("Telemetry failure: Expected exactly 1 logged engine panic event, got %d", stats.PanicIncidents)
	}
	if stats.TasksShed == 0 {
		t.Errorf("Telemetry failure: Ingress flood failed to record any load shedding metrics")
	}
}
