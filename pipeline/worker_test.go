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
	// 1. Create a transient sandbox directory for the underlying WAL
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "pipeline_test.wal")

	wal, err := cache.NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to initialize WAL for pipeline testing: %v", err)
	}

	// 2. Instantiate a pool with 3 parallel workers and a queue capacity of 10
	workerCount := 3
	queueSize := 10
	pool := NewWorkerPool(wal, workerCount, queueSize)

	// Create a cancelable context to control the background lifecycle
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 3. Fire up the background workers
	pool.Start(ctx)

	// 4. Submit 5 distinct tasks into the execution stream
	taskCount := 5
	for i := 1; i <= taskCount; i++ {
		task := Task{
			ID:      fmt.Sprintf("TX-00%d", i),
			Payload: []byte(fmt.Appendf(nil, "payload_data_block_%d", i)),
		}
		pool.Submit(task)
	}

	// Give the workers a brief moment to process the queue in memory
	time.Sleep(500 * time.Millisecond)

	// 5. Trigger a clean graceful shutdown
	pool.Stop()
	cancel() // Release context resources

	// 6. Close the log file so we can parse it from the beginning
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL instance: %v", err)
	}

	// 7. Verify the output state on disk using our verified recovery system
	recoveryWAL, err := cache.NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to reopen WAL for tracking verification: %v", err)
	}
	defer recoveryWAL.Close()

	entries, err := recoveryWAL.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read back log records: %v", err)
	}

	// Track seen START and COMMIT indicators
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

	// Validate that all 5 tasks went through the full durability state machine
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
