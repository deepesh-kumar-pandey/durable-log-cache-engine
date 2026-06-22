package cache

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestWAL_WriteAndReadAll verifies the basic happy path: opening a WAL,
// writing sequential records, closing it, and reading them back with 100% fidelity.
func TestWAL_WriteAndReadAll(t *testing.T) {
	// Create a temporary directory for our test file that cleans up automatically
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_lifecycle.wal")

	// 1. Initialize the WAL instance
	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to initialize NewWAL: %v", err)
	}

	// Define distinct test payloads
	payloads := [][]byte{
		[]byte("initialization_event"),
		[]byte("worker_task_dispatched_id_1024"),
		[]byte("metrics_snapshot_flush_success"),
	}

	// 2. Write payloads sequentially
	for _, p := range payloads {
		if err := wal.Write(p); err != nil {
			t.Fatalf("Failed to write payload to WAL: %v", err)
		}
	}

	// Force volatile Page Cache dirty pages down to disk blocks
	if err := wal.Sync(); err != nil {
		t.Fatalf("Failed to execute sync system call: %v", err)
	}

	// 3. Trigger a clean recovery scan on the open file handle
	entries, err := wal.ReadAll()
	if err != nil {
		t.Fatalf("Failed to execute ReadAll on active log: %v", err)
	}

	// Verify the entry count matches perfectly
	if len(entries) != len(payloads) {
		t.Errorf("Expected %d recovered entries, got %d", len(payloads), len(entries))
	}

	// Check that data payloads and temporal ordering match exactly
	for i, entry := range entries {
		if !bytes.Equal(entry.Payload, payloads[i]) {
			t.Errorf("Data corruption at index %d: expected %s, got %s", i, payloads[i], entry.Payload)
		}
		if entry.Timestamp <= 0 {
			t.Errorf("Invalid or unpopulated nanosecond timestamp at index %d: %d", i, entry.Timestamp)
		}
	}

	// 4. Tear down the file handle descriptor cleanly
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to release file descriptor handle: %v", err)
	}
}

// TestWAL_ConcurrentWriteStress runs a heavy multi-threaded race test.
// It spawns 50 concurrent goroutines writing simultaneously to ensure the sync.Mutex
// eliminates any interleaving or memory collisions.
func TestWAL_ConcurrentWriteStress(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_concurrency.wal")

	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	var wg sync.WaitGroup
	goroutinesCount := 50
	writesPerGoroutine := 20
	expectedTotalEntries := goroutinesCount * writesPerGoroutine

	payload := []byte("concurrent_stream_stress_block_payload")

	// Spawn workers concurrently using standard WaitGroup sync
	for i := 0; i < goroutinesCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < writesPerGoroutine; j++ {
				if err := wal.Write(payload); err != nil {
					t.Errorf("Concurrent Write failed: %v", err)
				}
			}
		}()
	}

	// Wait until all parallel writes finish execution
	wg.Wait()

	// Read everything back to inspect structural ordering consistency
	entries, err := wal.ReadAll()
	if err != nil {
		t.Fatalf("Failed to scan entries after heavy concurrent writes: %v", err)
	}

	if len(entries) != expectedTotalEntries {
		t.Fatalf("Data loss or race condition detected! Expected %d entries, but only recovered %d", expectedTotalEntries, len(entries))
	}

	// Ensure none of the interleaved frames are corrupt
	for _, entry := range entries {
		if !bytes.Equal(entry.Payload, payload) {
			t.Fatalf("Memory corruption caught! Concurrently written block contents got interleaved or modified on disk.")
		}
	}
}

// TestWAL_CorruptedFileBoundary handles defensive testing: verifying how our
// recovery path responds when it encounters a partial or truncated frame payload.
func TestWAL_CorruptedFileBoundary(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test_corrupted.wal")

	wal, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to instantiate WAL: %v", err)
	}

	// Write one healthy 12-byte header + data frame
	healthyPayload := []byte("healthy_frame")
	if err := wal.Write(healthyPayload); err != nil {
		t.Fatalf("Failed to write baseline frame: %v", err)
	}
	wal.Close()

	// Manually open the file back up behind the engine's back to append broken garbage bytes
	rawFile, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("Failed to intercept raw file: %v", err)
	}

	// Append a broken 4-byte segment (a partial header frame that drops off abruptly)
	corruptBytes := []byte{0x00, 0x00, 0x00, 0xFF}
	if _, err := rawFile.Write(corruptBytes); err != nil {
		t.Fatalf("Failed to inject raw corruption payload: %v", err)
	}
	rawFile.Close()

	// Re-open our clean WAL handle layer to check engine resistance
	recoveredWAL, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to reopen engine handle over corrupted logs: %v", err)
	}
	defer recoveredWAL.Close()

	// Sweeping the log should immediately fail at the bad frame boundary instead of looping infinitely
	_, err = recoveredWAL.ReadAll()
	if err == nil {
		t.Fatalf("Security failure: ReadAll should have thrown an unexpected EOF error on truncated logs.")
	}
}
