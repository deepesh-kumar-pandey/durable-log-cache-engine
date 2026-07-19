package pipeline

import (
	"context"
	"fmt"
	"log"
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
}

type WorkerPool struct {
	wal          *cache.WAL
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

func NewWorkerPool(wal *cache.WAL, workerCount int, queueSize int, maxRate float64, burstSize float64) *WorkerPool {
	return &WorkerPool{
		wal:          wal,
		taskChannel:  make(chan Task, queueSize),
		workerCount:  workerCount,
		nextWorkerID: 1,
		limiter:      NewTokenBucket(maxRate, burstSize),
	}
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

			logCommit := fmt.Appendf(nil, "COMMIT: %s", task.ID)
			if err := wp.wal.Write(logCommit); err != nil {
				log.Printf("[Worker %d] WAL critical commit logging failure: %v", id, err)
			}

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
