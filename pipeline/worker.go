package pipeline

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"log-cache-engine/cache" // Maps to your tested WAL package
)

// Task represents a discrete unit of work for our systems engine.
type Task struct {
	ID      string
	Payload []byte
}

// WorkerPool coordinates thread-safe job scheduling with durability tracking.
type WorkerPool struct {
	wal         *cache.WAL
	taskChannel chan Task
	workerCount int
	wg          sync.WaitGroup
}

// NewWorkerPool instantiates an operational execution cluster.
func NewWorkerPool(wal *cache.WAL, workerCount int, queueSize int) *WorkerPool {
	return &WorkerPool{
		wal:         wal,
		taskChannel: make(chan Task, queueSize),
		workerCount: workerCount,
	}
}

// Start spawns the background worker goroutines.
func (wp *WorkerPool) Start(ctx context.Context) {
	for i := 1; i <= wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx, i)
	}
}

// Submit enqueues work into the pipeline channel.
func (wp *WorkerPool) Submit(task Task) {
	wp.taskChannel <- task
}

// worker is the multi-threaded loop executing the tasks.
func (wp *WorkerPool) worker(ctx context.Context, id int) {
	defer wp.wg.Done()
	log.Printf("[Worker %d] Booted and listening for pipeline tasks...", id)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Worker %d] Shutting down cleanly via context termination.", id)
			return
		case task, ok := <-wp.taskChannel:
			if !ok {
				log.Printf("[Worker %d] Channel closed. Draining worker loop.", id)
				return
			}

			// 1. Durability Layer: Commit intent to the WAL before handling work
			logIntent := []byte(fmt.Sprintf("START: %s", task.ID))
			if err := wp.wal.Write(logIntent); err != nil {
				log.Printf("[Worker %d] WAL critical write failure for task %s: %v", id, task.ID, err)
				continue
			}
			_ = wp.wal.Sync() // Force commit to disk blocks

			// 2. Simulate Execution Work
			log.Printf("[Worker %d] Processing Task: %s (Data: %s)", id, task.ID, string(task.Payload))
			time.Sleep(50 * time.Millisecond) // Simulated computational latency

			// 3. Mark Complete in WAL
			logCommit := []byte(fmt.Sprintf("COMMIT: %s", task.ID))
			if err := wp.wal.Write(logCommit); err != nil {
				log.Printf("[Worker %d] WAL critical commit logging failure: %v", id, err)
			}
		}
	}
}

// Stop securely drains the channel and waits for workers to complete remaining tasks.
func (wp *WorkerPool) Stop() {
	close(wp.taskChannel)
	wp.wg.Wait()
	log.Println("Worker pool completely stopped. All channels safely drained.")
}
