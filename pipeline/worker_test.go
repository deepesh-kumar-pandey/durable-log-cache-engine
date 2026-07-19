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
	pool := NewWorkerPool(wal, workerCount, queueSize)

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
		} else if strings.HasPrefix(logStr, "COMMIT: ") {
			id := strings.TrimPrefix(logStr, "COMMIT: ")
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

	pool := NewWorkerPool(wal, 2, 10) // Small pool size to easily trace threads

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool.Start(ctx)

	// 1. Submit a normal task
	pool.Submit(Task{ID: "TX-GOOD-1", Payload: fmt.Appendf(nil, "safe_payload_data")})

	// 2. Submit the poison pill task designed to crash an active worker
	pool.Submit(Task{ID: "TX-POISON", Payload: fmt.Appendf(nil, "TRIGGER_PANIC")})

	// 3. Submit a subsequent task to confirm a replacement worker picked it up
	pool.Submit(Task{ID: "TX-GOOD-2", Payload: fmt.Appendf(nil, "post_recovery_payload")})

	// Allow the pool to step through execution, panic, write to disk, and restart a thread
	time.Sleep(500 * time.Millisecond)

	pool.Stop()
	cancel()

	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL instance: %v", err)
	}

	// Read log tracks to verify resilience compliance
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
		switch logStr {
		case "START: TX-POISON":
			hasPoisonStart = true
		case "CRASH: TX-POISON":
			hasPoisonCrash = true
		case "COMMIT: TX-GOOD-2":
			hasGood2Commit = true
		}
	}

	// Enforce assertions
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
