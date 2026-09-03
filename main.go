package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"log-cache-engine/cache"
	"log-cache-engine/pipeline"
)

var (
	pool      *pipeline.WorkerPool
	memCache  *cache.Cache
	startTime time.Time
)

// ── Configuration ─────────────────────────────────────────────────────────────

type engineConfig struct {
	port         string
	walPath      string
	workers      int
	queueSize    int
	rate         float64
	burst        float64
	cacheMaxSize int
}

func loadConfig() engineConfig {
	return engineConfig{
		port:         envStr("ENGINE_PORT", "8080"),
		walPath:      envStr("ENGINE_WAL_PATH", "engine_runtime.wal"),
		workers:      envInt("ENGINE_WORKERS", 4),
		queueSize:    envInt("ENGINE_QUEUE_SIZE", 50),
		rate:         envFloat("ENGINE_RATE", 5.0),
		burst:        envFloat("ENGINE_BURST", 10.0),
		cacheMaxSize: envInt("ENGINE_CACHE_MAX", 10_000),
	}
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

// ── Middleware ─────────────────────────────────────────────────────────────────

// loggingMiddleware wraps an HTTP handler with structured request/response logging.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		log.Printf("[API] %s %s → %d (%s)", r.Method, r.URL.Path, rw.status, time.Since(start).Round(time.Microsecond))
	})
}

// responseWriter wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// ── Handlers ──────────────────────────────────────────────────────────────────

// healthHandler is a liveness probe for container orchestrators (Kubernetes, ECS).
// GET /health → {"status":"ok","uptime_seconds":N,"cache_size":N}
func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":         "ok",
		"uptime_seconds": int(time.Since(startTime).Seconds()),
		"cache_size":     memCache.Len(),
	})
}

// metricsHandler exposes lock-free telemetry snapshots including cache hit rate.
// GET /metrics → {"TasksProcessed":N,"PanicIncidents":N,"TasksShed":N,"CacheSize":N,"CacheHitRate":N}
func metricsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	stats := pool.GetStats()
	type metricsResponse struct {
		pipeline.EngineStats
		CacheHitRate float64 `json:"CacheHitRate"`
	}
	json.NewEncoder(w).Encode(metricsResponse{
		EngineStats:  stats,
		CacheHitRate: memCache.HitRate(),
	})
}

// submitTaskHandler ingests external log tasks into the pipeline.
// POST /submit  body: {"id":"TX-001","payload":"event data"}
// → 202 accepted | 429 rate limited
func submitTaskHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		ID      string `json:"id"`
		Payload string `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Bad request body", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(body.ID) == "" {
		http.Error(w, `{"error":"id is required"}`, http.StatusBadRequest)
		return
	}
	task := pipeline.Task{ID: body.ID, Payload: []byte(body.Payload)}
	if pool.TrySubmit(task) {
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintln(w, `{"status":"accepted"}`)
	} else {
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprintln(w, `{"status":"shedded","reason":"rate_limit_exceeded"}`)
	}
}

// lookupHandler performs a point read against the in-memory cache index.
// GET /lookup/{id}
// → 200 {"id":"TX-001","payload":"hello","recovered":false}
// → 404 {"error":"not found","id":"TX-001"}
func lookupHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/lookup/")
	if id == "" {
		http.Error(w, `{"error":"id is required"}`, http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	entry, ok := memCache.Get(id)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "not found", "id": id})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{
		"id":        id,
		"payload":   string(entry.Value),
		"recovered": entry.Recovered,
	})
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	startTime = time.Now()
	cfg := loadConfig()

	log.Printf("[Engine] Starting with config: port=%s workers=%d queue=%d rate=%.1f burst=%.1f cache_max=%d",
		cfg.port, cfg.workers, cfg.queueSize, cfg.rate, cfg.burst, cfg.cacheMaxSize)

	// ── 1. Storage Layer ───────────────────────────────────────────────────────
	log.Println("[Engine] Initializing storage WAL layer...")
	wal, err := cache.NewWAL(cfg.walPath)
	if err != nil {
		log.Fatalf("Critical system failure initializing WAL: %v", err)
	}

	// ── 2. Crash Recovery ─────────────────────────────────────────────────────
	// Scan the WAL from 0x00 → EOF and restore all committed entries into the
	// in-memory cache before the network gateway opens (Crash_recovery_protocol.md).
	log.Println("[Engine] Scanning WAL for historical records (crash recovery sweep)...")
	memCache = cache.NewCache(cfg.cacheMaxSize)

	entries, err := wal.ReadAll()
	if err != nil {
		log.Fatalf("[Engine] WAL recovery scan failed: %v", err)
	}

	recovered := 0
	for _, entry := range entries {
		id, payload, ok := pipeline.ParseCommitRecord(string(entry.Payload))
		if !ok {
			continue // START or CRASH entry — skip incomplete work
		}
		if payload == nil {
			// Legacy COMMIT without payload — store sentinel preserving the key.
			payload = fmt.Appendf(nil, "recovered@%d", entry.Timestamp)
		}
		memCache.SetRecovered(id, payload)
		recovered++
	}
	log.Printf("[Engine] Recovery complete. Replayed %d committed records into memory index.", recovered)

	// ── 3. WAL Compaction ─────────────────────────────────────────────────────
	// Truncate committed records from the WAL now that they live in the cache.
	// Uncommitted (START/CRASH) entries represent work that failed and need not
	// be replayed — they are discarded as part of the compaction.
	if len(entries) > 0 {
		if err := wal.Compact(); err != nil {
			log.Printf("[Engine] WAL compaction warning: %v", err)
		}
	}

	// ── 4. Worker Pool ────────────────────────────────────────────────────────
	log.Println("[Engine] Spawning worker pool nodes and rate limit buckets...")
	pool = pipeline.NewWorkerPool(wal, memCache, cfg.workers, cfg.queueSize, cfg.rate, cfg.burst)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)

	// ── 5. HTTP Server ────────────────────────────────────────────────────────
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/metrics", metricsHandler)
	mux.HandleFunc("/submit", submitTaskHandler)
	mux.HandleFunc("/lookup/", lookupHandler)

	server := &http.Server{
		Addr:    ":" + cfg.port,
		Handler: loggingMiddleware(mux),
	}

	stopSignals := make(chan os.Signal, 1)
	signal.Notify(stopSignals, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("[API] Engine online at http://localhost:%s", cfg.port)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("[API] Server failed unexpectedly: %v", err)
		}
	}()

	<-stopSignals
	log.Println("\n[Engine] Shutdown signal captured. Terminating gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)

	pool.Stop()
	cancel()
	_ = wal.Close()

	log.Println("[Engine] Core lifecycle terminated cleanly. Subsystems offline.")
}
