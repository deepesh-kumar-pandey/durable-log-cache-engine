package pipeline

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic" // Added for lock-free, high-performance counters
	"time"

	"log-cache-engine/cache"
)

type Task struct {
	ID      string
	Payload []byte
}

// EngineStats maps out the internal state snapshot of our system engine.
type EngineStats struct {
	TasksProcessed uint64
	PanicIncidents uint64
	TasksShed      uint64
	CacheSize      int // Number of committed records currently held in the volatile index
}

type WorkerPool struct {
	wal          *cache.WAL
	cache        *cache.Cache // Volatile in-memory index; populated during WAL recovery and kept hot by workers
	taskChannel  chan Task
	workerCount  int
	wg           sync.WaitGroup
	ctx          context.Context
	nextWorkerID int
	mu           sync.Mutex
	limiter      *TokenBucket

	// Atomic Metrics Counters
	tasksProcessed uint64
	panicIncidents uint64
	tasksShed      uint64
}

func NewWorkerPool(wal *cache.WAL, c *cache.Cache, workerCount int, queueSize int, maxRate float64, burstSize float64) *WorkerPool {
	return &WorkerPool{
		wal:          wal,
		cache:        c,
		taskChannel:  make(chan Task, queueSize),
		workerCount:  workerCount,
		nextWorkerID: 1,
		limiter:      NewTokenBucket(maxRate, burstSize),
	}
}

// ParseCommitRecord decodes a WAL COMMIT entry of the form "COMMIT: <id>|<base64(payload)>".
// Returns (taskID, payloadBytes, true) on success; ("" , nil, false) if the record is not a COMMIT entry.
// Handles the legacy format "COMMIT: <id>" (no payload) by returning nil payload bytes.
func ParseCommitRecord(record string) (id string, payload []byte, ok bool) {
	if !strings.HasPrefix(record, "COMMIT: ") {
		return "", nil, false
	}
	rest := strings.TrimPrefix(record, "COMMIT: ")
	parts := strings.SplitN(rest, "|", 2)
	id = parts[0]
	if len(parts) == 2 {
		decoded, err := base64.StdEncoding.DecodeString(parts[1])
		if err == nil {
			return id, decoded, true
		}
	}
	// Legacy format or decode error — return ID with nil payload.
	return id, nil, true
}

func (wp *WorkerPool) Start(ctx context.Context) {
	wp.ctx = ctx
	for i := 1; i <= wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(wp.nextWorkerID)
		wp.nextWorkerID++
	}
}

func (wp *WorkerPool) spawnReplacement() {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	select {
	case <-wp.ctx.Done():
		return
	default:
		wp.wg.Add(1)
		id := wp.nextWorkerID
		wp.nextWorkerID++
		log.Printf("[Pool] Target capacity lowered by panic. Spawning replacement Worker %d...", id)
		go wp.worker(id)
	}
}

func (wp *WorkerPool) Submit(task Task) {
	if err := wp.limiter.Wait(wp.ctx); err != nil {
		log.Printf("[Pool] Task %s rejected: engine lifecycle terminating", task.ID)
		return
	}
	wp.taskChannel <- task
}

func (wp *WorkerPool) TrySubmit(task Task) bool {
	if !wp.limiter.Allow() {
		atomic.AddUint64(&wp.tasksShed, 1) // Increment shed metric on drop
		return false
	}
	wp.taskChannel <- task
	return true
}

// GetStats returns a thread-safe telemetry snapshot of the pipeline metrics.
func (wp *WorkerPool) GetStats() EngineStats {
	return EngineStats{
		TasksProcessed: atomic.LoadUint64(&wp.tasksProcessed),
		PanicIncidents: atomic.LoadUint64(&wp.panicIncidents),
		TasksShed:      atomic.LoadUint64(&wp.tasksShed),
		CacheSize:      wp.cache.Len(),
	}
}

func (wp *WorkerPool) worker(id int) {
	var currentTask *Task

	defer func() {
		wp.wg.Done()

		if r := recover(); r != nil {
			log.Printf("[Worker %d] CRITICAL: Recovered from panic: %v", id, r)
			atomic.AddUint64(&wp.panicIncidents, 1) // Increment panic count metric

			if currentTask != nil {
				logCrash := fmt.Appendf(nil, "CRASH: %s", currentTask.ID)
				if err := wp.wal.Write(logCrash); err != nil {
					log.Printf("[Worker %d] Failed to write CRASH marker to WAL: %v", id, err)
				}
				_ = wp.wal.Sync()
			}
			wp.spawnReplacement()
		}
	}()

	log.Printf("[Worker %d] Booted and listening for pipeline tasks...", id)

	for {
		select {
		case <-wp.ctx.Done():
			log.Printf("[Worker %d] Shutting down cleanly via context termination.", id)
			return
		case task, ok := <-wp.taskChannel:
			if !ok {
				log.Printf("[Worker %d] Channel closed. Draining worker loop.", id)
				return
			}

			currentTask = &task

			logIntent := fmt.Appendf(nil, "START: %s", task.ID)
			if err := wp.wal.Write(logIntent); err != nil {
				log.Printf("[Worker %d] WAL critical write failure for task %s: %v", id, task.ID, err)
				currentTask = nil
				continue
			}
			_ = wp.wal.Sync()

			log.Printf("[Worker %d] Processing Task: %s (Data: %s)", id, task.ID, string(task.Payload))
			if string(task.Payload) == "TRIGGER_PANIC" {
				panic(fmt.Sprintf("Unsafe payload data detected in task %s", task.ID))
			}

			time.Sleep(50 * time.Millisecond)

			// Embed the base64-encoded payload inside the COMMIT record so crash
			// recovery can restore the original data bytes on restart.
			encoded := base64.StdEncoding.EncodeToString(task.Payload)
			logCommit := fmt.Appendf(nil, "COMMIT: %s|%s", task.ID, encoded)
			if err := wp.wal.Write(logCommit); err != nil {
				log.Printf("[Worker %d] WAL critical commit logging failure: %v", id, err)
			}

			// Update the in-memory index so the hot read path reflects committed state.
			wp.cache.Set(task.ID, task.Payload)

			atomic.AddUint64(&wp.tasksProcessed, 1) // Increment successful completion metric
			currentTask = nil
		}
	}
}

func (wp *WorkerPool) Stop() {
	close(wp.taskChannel)
	wp.wg.Wait()
	log.Println("Worker pool completely stopped. All channels safely drained.")
}
