package cache

import (
	"encoding/binary" // Used for low-overhead, zero-allocation binary parsing (BigEndian encoding)
	"fmt"             // Used for wrapping errors cleanly using structural format verbs like %w
	"io"              // Used strictly to catch the standard io.EOF token during recovery loops
	"os"              // Low-level operating system interface for managing file descriptors and syscalls
	"sync"            // Mutex primitives to ensure thread safety across multi-core worker pipelines
	"time"            // High-precision tracking primitives for nanosecond logging timestamps
)

const (
	// Header size configuration:
	// - 4 bytes: Unsigned 32-bit integer (uint32) to track the dynamic payload length prefix.
	// - 8 bytes: Signed 64-bit integer (int64) to preserve the incoming nanosecond timestamp.
	// Totaling exactly 12 bytes. This fixed layout eliminates any string parsing/scanning overhead.
	headerSize = 12
)

// WAL (Write-Ahead Log) coordinates thread-safe, low-overhead sequential log persistence.
// It maps application-layer log streams down to atomic append-only disk frames.
type WAL struct {
	mu   sync.Mutex // Protects the underlying file descriptor and append offset from concurrent races
	file *os.File   // Pointer to the operating system's active file descriptor tracking handle
}

// LogEntry encapsulates an unmarshaled, high-fidelity log record decoded from the WAL on disk.
// This structure acts as a temporary data container used exclusively during crash recovery.
type LogEntry struct {
	Timestamp int64  // The 64-bit nanosecond Unix epoch extracted from bytes [4..11] of the frame header
	Payload   []byte // The raw, uncopied application event message extracted sequentially from the disk media
}

// NewWAL initializes a fresh log file or hooks into an existing write-ahead log stream at the specified path.
// It explicitly configures the file descriptor flags at the kernel level.
func NewWAL(path string) (*WAL, error) {
	// Flag Breakdown:
	// - os.O_CREATE: Instructs the kernel to generate a new file if it is missing, leaving existing files unharmed.
	// - os.O_WRONLY: Optimizes performance by opening the file handle in write-only territory.
	// - os.O_APPEND: Forces the OS kernel to atomically jump the file offset pointer to EOF before every single write execution.
	// Permission 0644 (Octal): Owner can Read/Write, Groups/Others can strictly Read. Protects logs from erratic modification.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		// %w wraps the raw operating system error descriptor, allowing underlying inspection via errors.Is() later
		return nil, fmt.Errorf("failed to open/create WAL file: %w", err)
	}

	// Return a pointer to the instantiated struct to ensure a single immutable file descriptor state is shared
	return &WAL{
		file: file,
	}, nil
}

// Write intercepts the raw log payload, packs it into our strict binary frame format,
// and triggers an atomic, sequential write down into the active file descriptor.
func (w *WAL) Write(payload []byte) error {
	payloadLen := uint32(len(payload)) // Standardize length size to 4 bytes (supports individual logs up to 4GB)
	timestamp := time.Now().UnixNano() // Capture current monotonic nanosecond count (8 bytes)

	// Memory Optimization: Allocate a single, contiguous slice layout matching the total frame dimensions.
	// By not escaping this function scope, the Go compiler allocates this entire frame block directly on the
	// ultra-fast execution stack, completely avoiding heap allocations and bypassing the Garbage Collector.
	frame := make([]byte, headerSize+payloadLen)

	// 1. Pack Payload Length into bytes [0..3] using fixed BigEndian bit-shifting
	binary.BigEndian.PutUint32(frame[0:4], payloadLen)

	// 2. Pack Timestamp into bytes [4..11] immediately following the size prefix
	binary.BigEndian.PutUint64(frame[4:12], uint64(timestamp))

	// 3. Perform a rapid userspace memory copy to paste the raw log contents past the 12-byte header barrier
	copy(frame[headerSize:], payload)

	// Concurrency Guard: Block concurrent worker threads from entering the file descriptor execution pipeline
	w.mu.Lock()
	defer w.mu.Unlock() // Guarantees lock release at final scope return, neutralizing deadlock risks

	// Dispatch the complete, linear frame down to the OS write path via a single contiguous system call
	_, err := w.file.Write(frame)
	if err != nil {
		return fmt.Errorf("failed appending binary frame to WAL: %w", err)
	}

	return nil
}

// ReadAll performs a complete sequential sweep of the WAL from byte index 0x00 to EOF.
// It parses the binary architecture frames step-by-step to recover historical records into memory.
func (w *WAL) ReadAll() ([]LogEntry, error) {
	// Concurrency Guard: Lock out active writes and updates to safely read the underlying file descriptor states
	w.mu.Lock()
	defer w.mu.Unlock()

	// System Call: Force the file offset descriptor's read pointer back to the absolute start of the file (offset 0)
	// Relative reference '0' denotes SEEK_SET in POSIX, overriding the append-only pointer state.
	_, err := w.file.Seek(0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to start of WAL: %w", err)
	}

	var entries []LogEntry

	// Pre-allocate a 12-byte recycling stack buffer to pull headers sequentially without allocating heap cells inside the loop
	headerBuf := make([]byte, headerSize)

	for {
		// 1. Fetch exactly the next 12 bytes of header data metadata from disk
		_, err := w.file.Read(headerBuf)
		if err != nil {
			// io.EOF is our explicit clean exit token indicating that the recovery path has safely matched the end of the byte stream
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("error reading WAL header: %w", err)
		}

		// 2. Decode the header variables via reverse bit extraction offsets
		payloadLen := binary.BigEndian.Uint32(headerBuf[0:4])
		timestamp := binary.BigEndian.Uint64(headerBuf[4:12])

		// 3. Dynamically set a boundary block matching this log entry's exact payload requirements
		payloadBuf := make([]byte, payloadLen)

		// 4. Read the exact payload bytes. The file descriptor picks up right where it left off, past byte index 12.
		_, err = w.file.Read(payloadBuf)
		if err != nil {
			// Triggered if the file is abruptly truncated or a write operation failed mid-transit during system failure
			return nil, fmt.Errorf("unexpected EOF reading WAL payload: %w", err)
		}

		// 5. Build our structured history node and store it inside our collection index array
		entries = append(entries, LogEntry{
			Timestamp: int64(timestamp),
			Payload:   payloadBuf,
		})
	}

	return entries, nil // Return the completely reconstructed slice history back up to the cache memory index
}

// Sync forces the operating system kernel to flush its volatile RAM Page Cache buffers straight down
// into physical non-volatile storage hardware (SSD block layers), mimicking a POSIX fsync system call.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Ensures total durability (Persistence) by blocking until the storage controller acknowledges safe hardware sector updates
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to execute kernel storage synchronization: %w", err)
	}
	return nil
}

// Close gracefully tears down our system footprints, flushing memory residuals and returning the open
// File Descriptor handle back to the operating system table to protect against systemic file descriptor leaks.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Releases the kernel tracking handle. Any trailing attempts to read/write to this instance will fail with os.ErrClosed.
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file handle: %w", err)
	}
	return nil
}
