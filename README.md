# Durable Log Cache Engine

A production-grade, crash-resilient log ingestion and caching engine written in Go. Provides **ultra-low-latency log ingestion** with **zero data loss guarantees** via a Write-Ahead Log (WAL), backed by a concurrent worker pipeline, LRU in-memory cache, and token-bucket rate limiter.

---

## Features

| Feature | Details |
|---|---|
| **Write-Ahead Log (WAL)** | Binary-framed, `fsync`-backed append-only ledger — data is durable before any acknowledgement |
| **Crash Recovery** | On boot, replays the WAL from `0x00` → `EOF`, restores original payloads into the LRU cache, then compacts the WAL |
| **LRU Cache Index** | Bounded in-memory hot-read layer with O(1) eviction; exposes hit rate telemetry |
| **Worker Pool** | Configurable concurrent workers with automatic panic recovery and self-healing replacement |
| **Token Bucket Rate Limiter** | Configurable rate + burst depth; excess tasks are shed with `429` responses |
| **HTTP API** | `/submit`, `/lookup/{id}`, `/metrics`, `/health` |
| **Logging Middleware** | Every request logged with method, path, status, and latency |
| **Environment Config** | All runtime parameters configurable via env vars with sensible defaults |
| **Docker Ready** | Multi-stage minimal Alpine image running as `nobody` |

---

## Architecture

```
┌──────────┐
│  Client  │
└────┬─────┘
     │  POST /submit  │  GET /lookup/{id}
     ▼
┌─────────────────────────┐
│  HTTP API + Middleware   │  main.go  (logging, routing)
│  Token Bucket Gate       │  pipeline/rate_limiter.go
└──────────┬──────────────┘
           │  Enqueue Task
           ▼
┌─────────────────────────┐
│   Worker Pool           │  pipeline/worker.go
│   (N goroutines)        │
└──────┬──────────────────┘
       │  Write START frame
       │  Process task (50ms)
       │  Write COMMIT: id|base64(payload)
       ▼
┌─────────────────────────┐
│   WAL (binary frames)   │  cache/wal.go
│   fsync to disk         │  [len:4][ts:8][payload:N]
└──────┬──────────────────┘
       │  cache.Set(id, payload)
       ▼
┌─────────────────────────┐
│   LRU Cache Index       │  cache/cache.go
│   map + doubly-linked   │  bounded; O(1) eviction
└─────────────────────────┘
```

### WAL Binary Frame Layout

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|              Payload Length  (4 bytes / uint32)               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                    Timestamp  (8 bytes / int64 ns)            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|              Raw Message Payload  (variable length)           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### COMMIT Record Format

Workers write a structured COMMIT entry that embeds the original payload:

```
COMMIT: <task-id>|<base64(payload)>
```

This allows crash recovery to restore the exact original bytes — not a sentinel.

### Crash Recovery Flow

```
[ Boot ] ──► WAL exists? ──► NO  ──► Create fresh WAL ──► Open API
                │
                ▼ YES
        ReadAll() from 0x00
                │
        For each frame:
          COMMIT: id|b64  ──► cache.SetRecovered(id, payload)
          START / CRASH   ──► skip (incomplete / failed work)
                │
                ▼ EOF
        WAL.Compact()  ──► truncate WAL to 0 bytes (state is in cache)
                │
                ▼
        Open API (network gateway unlocked)
```

---

## Performance Benchmarks

Engine performance measured on an AMD Ryzen 5 5600H via standard Go benchmarks (`go test -bench .`):

| Component | Operation | Ops / Sec | Latency / Op | Allocations |
| :--- | :--- | :--- | :--- | :--- |
| **LRU Cache** | `Get` (Hit) | **~5.4 Million** | `183 ns` | `13 B` (1 alloc) |
| **LRU Cache** | `Set` (Write) | **~1.7 Million** | `583 ns` | `112 B` (4 allocs) |
| **LRU Cache** | `Set/Get` (Concurrent) | **~1.6 Million** | `606 ns` | `38 B` (2 allocs) |
| **WAL (Disk I/O)** | `Write` (Append) | **~523,000** | `1.9 µs` | `48 B` (1 alloc) |
| **WAL (Disk I/O)** | `Write + Sync` | **~348,000** | `2.8 µs` | `48 B` (1 alloc) |
| **Rate Limiter** | `Allow` (1 Thread) | **~8.2 Million** | `121 ns` | `0 B` (0 allocs) |
| **Rate Limiter** | `Allow` (Concurrent) | **~5.2 Million** | `189 ns` | `0 B` (0 allocs) |

- **Zero-Allocation Gateway:** The Token Bucket rate limiter operates lock-free on the fast path with zero memory allocations, handling >5 million checks per second.
- **Microsecond Durability:** Even when forcing kernel `fsync` blocks for absolute zero-data-loss durability, the sequential binary WAL ingests over 300,000 log events per second.
- **Nanosecond Reads:** The in-memory LRU handles millions of reads per second, serving the `/lookup` API instantly.

---

## Getting Started

### Prerequisites

- Go 1.21+
- (Optional) Docker

### Build & Run

```bash
git clone https://github.com/deepesh-kumar-pandey/durable-log-cache-engine
cd durable-log-cache-engine

go build -o log-cache-engine .
./log-cache-engine
```

### Run with Docker

```bash
docker build -t log-cache-engine .

# Mount a volume to persist the WAL across container restarts
docker run -p 8080:8080 \
  -v $(pwd)/data:/app \
  -e ENGINE_WORKERS=8 \
  log-cache-engine
```

---

## Configuration

All parameters are read from environment variables. Defaults are production-safe.

| Env Var | Default | Description |
|---|---|---|
| `ENGINE_PORT` | `8080` | HTTP listen port |
| `ENGINE_WAL_PATH` | `engine_runtime.wal` | WAL file path |
| `ENGINE_WORKERS` | `4` | Worker goroutine count |
| `ENGINE_QUEUE_SIZE` | `50` | Task channel buffer capacity |
| `ENGINE_RATE` | `5.0` | Token refill rate (tasks/second) |
| `ENGINE_BURST` | `10.0` | Burst depth before load shedding |
| `ENGINE_CACHE_MAX` | `10000` | Max entries in the LRU cache |

---

## API Reference

### `POST /submit` — Ingest a log task

```bash
curl -X POST http://localhost:8080/submit \
  -H "Content-Type: application/json" \
  -d '{"id":"TX-001","payload":"user_login_event"}'
```

| Status | Body | Meaning |
|--------|------|---------|
| `202 Accepted` | `{"status":"accepted"}` | Task queued; WAL write in flight |
| `429 Too Many Requests` | `{"status":"shedded","reason":"rate_limit_exceeded"}` | Token bucket exhausted |
| `400 Bad Request` | — | Missing or malformed body |

---

### `GET /lookup/{id}` — Read a committed log entry

```bash
curl http://localhost:8080/lookup/TX-001
```

**Hit:**
```json
{"id":"TX-001","payload":"user_login_event","recovered":false}
```
`recovered: true` means the entry was restored from WAL crash recovery rather than written by this process instance.

**Miss:**
```json
{"error":"not found","id":"TX-001"}
```

| Status | Meaning |
|--------|---------|
| `200 OK` | Cache hit |
| `404 Not Found` | Task not committed yet or evicted by LRU |

---

### `GET /metrics` — Live telemetry snapshot

```bash
curl http://localhost:8080/metrics
```

```json
{
  "TasksProcessed": 42,
  "PanicIncidents": 1,
  "TasksShed": 7,
  "CacheSize": 41,
  "CacheHitRate": 94.7
}
```

`CacheHitRate` is the percentage of `/lookup` calls satisfied from cache since process start.

---

### `GET /health` — Liveness probe

```bash
curl http://localhost:8080/health
```

```json
{"status":"ok","uptime_seconds":120,"cache_size":41}
```

---

## Testing

```bash
# Run all tests
go test ./... -v

# With race detector (recommended)
go test ./... -v -race
```

### Test Matrix — 20 tests total

| Package | Test | Coverage |
|---------|------|----------|
| `cache` | `TestCache_SetAndGet` | Basic write → read round-trip |
| `cache` | `TestCache_SetRecovered` | Recovery flag propagation |
| `cache` | `TestCache_Get_Miss` | Cache miss returns `(zero, false)` |
| `cache` | `TestCache_LRU_Eviction` | LRU evicts coldest entry at capacity |
| `cache` | `TestCache_LRU_AccessPromotes` | Get promotes entry, protecting it from eviction |
| `cache` | `TestCache_HitRate` | Hit/miss counter accuracy |
| `cache` | `TestCache_HitRate_NoLookups` | Zero-division guard |
| `cache` | `TestCache_Len` | Size tracking |
| `cache` | `TestCache_Concurrent` | Race-free under 200 concurrent goroutines |
| `cache` | `TestWAL_WriteAndReadAll` | Happy-path write → read |
| `cache` | `TestWAL_ConcurrentWriteStress` | 50 goroutines × 20 writes, zero data loss |
| `cache` | `TestWAL_CorruptedFileBoundary` | Truncated frame detection |
| `pipeline` | `TestWorkerPool_ExecutionLifecycle` | START → COMMIT WAL trace |
| `pipeline` | `TestWorkerPool_PanicRecovery` | Poison pill → CRASH marker → replacement |
| `pipeline` | `TestWorkerPool_RateLimiting` | Burst depth enforcement |
| `pipeline` | `TestWorkerPool_TelemetryMetrics` | Atomic counter accuracy |
| `pipeline` | `TestTokenBucket_Allow_BasicBurst` | Burst exhaustion |
| `pipeline` | `TestTokenBucket_Refill_OverTime` | Time-based token regeneration |
| `pipeline` | `TestTokenBucket_Wait_ContextCancel` | Context cancellation unblocks `Wait()` |
| `pipeline` | `TestTokenBucket_Wait_TokenAvailable` | Immediate return on full bucket |
| `pipeline` | `TestTokenBucket_Concurrency` | Race-free under 100 goroutines |

---

## Project Structure

```
.
├── main.go                        # Entrypoint: config, recovery, WAL compaction, HTTP server
├── cache/
│   ├── wal.go                     # WAL: binary framing, fsync, ReadAll, Compact
│   ├── wal_test.go                # WAL tests: round-trip, concurrency, corruption
│   ├── cache.go                   # LRU cache index: O(1) eviction, hit rate, recovered flag
│   └── cache_test.go              # Cache tests: eviction order, hit rate, concurrency
├── pipeline/
│   ├── worker.go                  # WorkerPool: panic recovery, COMMIT format, ParseCommitRecord
│   ├── worker_test.go             # Integration tests: lifecycle, panic, rate, metrics
│   ├── rate_limiter.go            # Token Bucket: Allow() / Wait()
│   └── rate_limiter_test.go       # Unit tests: burst, refill, context cancel, concurrency
├── Dockerfile                     # Multi-stage Alpine build (non-root runtime)
├── Architecture.md                # System design reference
├── Storage_layer_architecture.md  # WAL binary wire format spec
├── Crash_recovery_protocol.md     # Boot-time WAL replay algorithm
└── Component_Interaction_FLow.md  # Data flow diagram
```

---

## Design Decisions

**LRU over a plain map** — `cache.Cache` uses a doubly-linked list + map giving O(1) eviction. Without bounded eviction a log engine that runs for weeks would silently OOM.

**Payload embedded in COMMIT records** — writing `COMMIT: id|base64(payload)` to the WAL means crash recovery can restore the exact original bytes. A COMMIT-only record (old design) meant a restart lost all payload data, making the cache useless after any crash.

**WAL compaction after recovery** — once committed state is safely in memory, the WAL is truncated to zero. Without this, a long-running engine accumulates unbounded disk usage.

**`sync/atomic` for metrics** — counters on every task completion path; atomic avoids lock contention across N workers on the hot path.

**Token bucket over leaky bucket** — native burst support means short spikes are absorbed rather than shed, while sustained overload is still rate-limited.