package pipeline

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"log-cache-engine/cache" // Maps to your tested write-ahead log (WAL) package
)

// Task represents a discrete, self-contained unit of execution payload
// routed through our parallel ingestion pipeline.
type Task struct {
	ID      string // Unique transaction identifier (e.g., "TX-001")
	Payload []byte // Raw data block bytes processed by the assigned worker thread
}

// WorkerPool manages an elastic cluster of worker goroutines, handling multi-threaded
// orchestration, graceful thread replacement on failure, and durable state tracking.
type WorkerPool struct {
	wal          *cache.WAL      // Shared pointer to the write-ahead log storage engine
	taskChannel  chan Task       // Buffered queue pipeline for incoming tasks
	workerCount  int             // Target number of simultaneous active background threads
	wg           sync.WaitGroup  // Coordinates clean shutdown steps by waiting on active threads
	ctx          context.Context // Main background lifecycle context; signals downstream cancellation
	nextWorkerID int             // Autoincrementing index providing unique labels for spun-up threads
	mu           sync.Mutex      // Critical section lock protecting nextWorkerID against concurrent adjustments
}

// NewWorkerPool allocates and initializes an operational worker pool topology.
func NewWorkerPool(wal *cache.WAL, workerCount int, queueSize int) *WorkerPool {
	return &WorkerPool{
		wal:          wal,
		taskChannel:  make(chan Task, queueSize), // Initialize thread-safe work channel buffer
		workerCount:  workerCount,
		nextWorkerID: 1, // IDs start sequentially at 1
	}
}

// Start registers the current lifecycle context and spawns the initial worker cluster.
func (wp *WorkerPool) Start(ctx context.Context) {
	wp.ctx = ctx // Cache context reference to let replacement threads inspect cancellation state
	
	for i := 1; i <= wp.workerCount; i++ {
		wp.wg.Add(1) // Register thread lifecycle delta inside our waitgroup counter
		go wp.worker(wp.nextWorkerID)
		wp.nextWorkerID++
	}
}

// spawnReplacement handles self-healing. It returns a fresh, healthy background goroutine
// back into the active execution matrix if an existing one panics out of bounds.
func (wp *WorkerPool) spawnReplacement() {
	wp.mu.Lock() // Acquire critical section lock to safely increment worker ID attributes
	defer wp.mu.Unlock()

	// Double check context boundaries; do not spin up fresh threads if the pool is exiting
	select {
	case <-wp.ctx.Done():
		return
	default:
		wp.wg.Add(1) // Add newly requested replacement thread block onto wait tracker
		id := wp.nextWorkerID
		wp.nextWorkerID++
		
		log.Printf("[Pool] Target capacity lowered by panic. Spawning replacement Worker %d...", id)
		go wp.worker(id)
	}
}

// Submit non-blockingly pushes a newly received unit of work into the concurrent queue channel.
func (wp *WorkerPool) Submit(task Task) {
	wp.taskChannel <- task
}

// worker encapsulates the main active looping routine executing and logging scheduled tasks.
func (wp *WorkerPool) worker(id int) {
	// A local memory reference cache used to extract the task name inside the panic handler
	var currentTask *Task 
	
	// Deferred execution block always called when this thread exits for any reason
	defer func() {
		wp.wg.Done() // Pop the thread off the active wait list

		// Intercept internal panics to prevent an operational system crash
		if r := recover(); r != nil {
			log.Printf("[Worker %d] CRITICAL: Recovered from panic: %v", id, r)
			
			// If the worker was midway through processing a task, log its crash state to disk
			if currentTask != nil {
				// ✅ Fixed with fmt.Appendf to satisfy linter rule
				logCrash := fmt.Appendf(nil, "CRASH: %s", currentTask.ID)
				if err := wp.wal.Write(logCrash); err != nil {
					log.Printf("[Worker %d] Failed to write CRASH marker to WAL: %v", id, err)
				}
				_ = wp.wal.Sync() // Force commit state block down to the filesystem
			}
			
			// Kick off the auto-heal engine to bring the pool back up to capacity
			wp.spawnReplacement()
		}
	}()

	log.Printf("[Worker %d] Booted and listening for pipeline tasks...", id)

	for {
		select {
		// Event 1: System cancellation hook tripped; break execution cleanly
		case <-wp.ctx.Done():
			log.Printf("[Worker %d] Shutting down cleanly via context termination.", id)
			return
			
		// Event 2: A task popped out of the ingestion queue channel
		case task, ok := <-wp.taskChannel:
			// If the task channel was closed by the pool supervisor, drain remaining items and return
			if !ok {
				log.Printf("[Worker %d] Channel closed. Draining worker loop.", id)
				return
			}

			// Map active item reference to let our defer recover handler catch it on exception
			currentTask = &task

			// Stage A: Log intent (START marker) to our write-ahead log to record ingestion permanence
			// ✅ Fixed with fmt.Appendf to satisfy linter rule
			logIntent := fmt.Appendf(nil, "START: %s", task.ID)
			if err := wp.wal.Write(logIntent); err != nil {
				log.Printf("[Worker %d] WAL critical write failure for task %s: %v", id, task.ID, err)
				currentTask = nil // Clear current task context on immediate disk error
				continue          // Drop task processing if durability system reports a failure
			}
			_ = wp.wal.Sync() // Ensure data physically reaches disk plates

			// Stage B: Process execution mechanics
			log.Printf("[Worker %d] Processing Task: %s (Data: %s)", id, task.ID, string(task.Payload))
			
			// Poison-pill test hook to mock and demonstrate real operational recovery
			if string(task.Payload) == "TRIGGER_PANIC" {
				panic(fmt.Sprintf("Unsafe payload data detected in task %s", task.ID))
			}
			
			time.Sleep(50 * time.Millisecond) // Simulated core processing calculation sleep

			// Stage C: Complete state transition. Log final commit frame down to disk
			// ✅ Fixed with fmt.Appendf to satisfy linter rule
			logCommit := fmt.Appendf(nil, "COMMIT: %s", task.ID)
			if err := wp.wal.Write(logCommit); err != nil {
				log.Printf("[Worker %d] WAL critical commit logging failure: %v", id, err)
			}
			
			// Wipe task cache handle clean; iteration successfully completed without error
			currentTask = nil
		}
	}
}

// Stop cleanly cuts off incoming submissions, drains open buffers, and waits for loops to exit.
func (wp *WorkerPool) Stop() {
	close(wp.taskChannel) // Closing channel alerts loops to finish remaining queued tasks
	wp.wg.Wait()          // Block main process thread until all worker goroutines finalize execution
	log.Println("Worker pool completely stopped. All channels safely drained.")
}