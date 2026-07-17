# Performance Tuning & Troubleshooting (Q&A)

## How many workers are we using?

**6 queues, each running in its own Docker container with its own OS process and event loop.** Activity concurrency varies per queue (configurable via env vars):

| Queue | Process | Role |
|-------|---------|------|
| `xml-feed-workflow-queue` | 1 `workflow-worker` container | Orchestrates batches, spawns UrlWorkflow children |
| `xml-feed-url-workflow-queue` | 1 `url-workflow-worker` container | Runs UrlWorkflow (fetch→parse→enrich→summarize pipeline) |
| `xml-feed-fetch-queue` | N `fetch-worker` containers | Fetches RSS/XML URLs (I/O-bound) |
| `xml-feed-parse-queue` | N `parse-worker` containers | Parses XML, saves record metadata (fast, ~2-3s) |
| `xml-feed-enrichment-queue` | N `enrichment-worker` containers | Per-article full content fetch + trafilatura + S3 upload; also runs EnrichmentWorkflow |
| `xml-feed-summarize-queue` | N `summarize-worker` containers | Generates summaries, updates task/job status (CPU-bound) |

Activity concurrency per worker is configurable via `FETCH_WORKERS`, `PARSE_WORKERS`, `ENRICHMENT_WORKERS`, `SUMMARIZE_WORKERS` env vars. Docker defaults: fetch=50, parse=50, enrichment=10, summarize=10. Code defaults: 5 for fetch/parse/enrichment, `os.cpu_count()` for summarize.

This gives **independent scaling per stage** — a fetch backlog won't starve summarize workers, and each stage can be scaled to its own bottleneck:
```bash
docker compose up -d --scale fetch-worker=3 --scale parse-worker=2 --scale enrichment-worker=2 --scale summarize-worker=2
```

The DB connection pool is `pool_size=30` with `max_overflow=60` — up to **90 concurrent connections per worker pod** (see [PgBouncer section](#why-does-each-worker-consume-30-db-connections-what-is-pgbouncer) for the math at scale). The aiohttp connector allows `limit=100` total connections with `limit_per_host=10`.

---

## Why do some YouTube XML feeds fail and get retried?

**Short answer:** Most YouTube RSS URLs (`youtube.com/feeds/videos.xml?channel_id=...`) in the test payload point to invalid/deleted channels that return **HTTP 404**. The fetcher correctly identifies these as permanent failures, but the old error handling in `task_processor.py` caused the activity to re-raise the exception, making Temporal retry 3 times.

**Fix in `task_processor.py:87-94`:**
Permanent failures (`RuntimeError("Permanent failure HTTP 404...")`) are now caught, persisted to DB, and **returned** — no exception propagates, so Temporal does not retry. Saves ~5-8 min on 34 YouTube URLs.

**Behavior breakdown:**
| Error type | Fetcher retries | Temporal retries | Example |
|------------|----------------|-------------------|---------|
| Permanent (404/403/410) | 0 (immediate fail) | **0** | Invalid YouTube channel |
| Transient (timeout, 429, connection) | 3 (1s, 2s backoff) | **3** (1s, 2s, 4s backoff) | Network blip, rate limit |
| Cancelled | N/A | **0** | Worker shutdown |

---

## Why does processing 101 URLs take 10 minutes?

| Bottleneck | Time | Why |
|------------|------|-----|
| Article content fetching | ~12-20s per feed | 20 articles × per-domain Semaphore(2) + 1s throttle |
| CPU-bound summarization | ~2-5s per article | TextRank + TF-IDF + LSA |
| YouTube 404 failures | ~3-5s per URL | Network round-trip + 3 retries |
| Single worker concurrency | N/A | Only 5 concurrent activities per queue process |

**Total for 101 feeds:** ~10-15 minutes.

---

## How can we speed it up?

### ✅ 1. Reduce `max_articles` per feed (20 → 10)

Halves the number of article fetches and summarizations per feed. Most RSS feeds show ~10-15 recent items, so 10 captures the vast majority of useful content.

**Change:** `task_processor.py:49` — `max_articles: int = 10`

**Impact:** Per-feed time drops from ~12-20s to ~6-10s. Total time: ~10 min → ~6-7 min.

---

### ✅ 2. Increase DB connection pool (10 → 30)

With a single worker processing up to 200 concurrent activities (Temporal default), the DB pool of 10 becomes the bottleneck. Each activity needs a session for persisting results.

**Change:** `session.py:16` — `pool_size=30, max_overflow=60`

**Impact:** More activities can proceed in parallel without waiting for DB connections.

---

### ✅ 3. Scale worker processes per queue

Instead of 1 process per queue, run multiple containers per queue. Each container is a separate OS process with its own event loop — true CPU parallelism without thread pools.

```bash
# Scale fetch to 3, parse to 2, enrichment to 2, summarize to 2
docker compose up -d --scale fetch-worker=3 --scale parse-worker=2 --scale enrichment-worker=2 --scale summarize-worker=2
```

**Impact:** Near-linear speedup for I/O-bound work. 3 fetch + 2 parse + 2 enrichment + 2 summarize = ~2-3 min total.

---

### ✅ 4. Fast-fail invalid YouTube URLs

Detect YouTube RSS URLs before making the HTTP request. YouTube channel URLs returning 404 can be identified by the channel ID pattern, or simply by checking the domain and using a shorter timeout.

**Change:** `aiohttp_fetcher.py` — added YouTube-specific timeout override and early 404 detection.

**Impact:** Saves ~2-3 min on 34 YouTube URLs.

---

## Wont Temporal retry for transient errors like 429?

**Yes.** The current fix only suppresses Temporal retries for **permanent failures** (404/403/410). Transient errors like 429 rate limiting, connection timeouts, and DNS failures still propagate as exceptions. However, with `MAX_ACTIVITY_RETRIES=1` (i.e., `maximum_attempts=1`), Temporal does **not** retry — the activity fails immediately on the first attempt. The fetcher itself handles retries internally (3 attempts with backoff).

The retry chain for transient errors:
1. **Fetcher level** (aiohttp): 3 retries with 1s/2s backoff
2. **Temporal level** (workflow): 0 retries (`maximum_attempts=1`)

This gives up to **3 total attempts** for transient failures at the fetcher level — enough to handle temporary network or server issues without wasting time on permanently dead URLs.

**Timeout math:** `ACTIVITY_TIMEOUT_SECONDS` default is **60s**, `MAX_ACTIVITY_RETRIES` is **1** (1 attempt, no retries). Activity budget = 60 × 1 × 3 = 180s. Adding `ENRICHMENT_TIMEOUT_BUDGET = 300s` (max 20 articles × 15s) + 60s safety margin → `CHILD_WORKFLOW_TIMEOUT = 540s` (9 minutes). This ensures the parent `UrlWorkflow` never kills the enrichment child before it completes.

---

### ❌ 5. Skip summarization for very short content

Already handled by `_is_garbage()` detection — content under 50 chars is skipped.

---

### 6. No thread pool needed — each queue is its own process

With each queue running in its own OS process, blocking CPU calls (`trafilatura.extract`, `generate_summary`) don't stall other queues. Each process has its own event loop and runs independently.

**Impact:** Simpler code, true parallelism, no GIL contention between queues, no thread pool management overhead.

---

## Why does each worker consume 30 DB connections? What is PgBouncer?

**The math:** `src/infrastructure/db/session.py:16` sets `pool_size=30, max_overflow=60` = up to **90 concurrent DB connections per worker**. Each activity borrows a session from the pool; if all 90 are in use, subsequent activities queue up.

**The problem at scale:** 101 workers × 90 connections = 9,090 concurrent DB connections. PostgreSQL out-of-the-box is configured for ~100-200 connections. Beyond that, each backend process (fork, auth, memory) becomes the bottleneck — performance degrades sharply.

**PgBouncer** is a lightweight connection pooler that sits between workers and PostgreSQL:

```
Worker 1 ──┐
Worker 2 ──┤
Worker 3 ──┤── PgBouncer (50-100 conns) ──── PostgreSQL
   ...     │
Worker 101 ┘
```

- **Workers** open/close connections fast (cheap, no auth/backend fork per open)
- **PgBouncer** reuses a small set of real DB connections across all workers
- **Result:** Postgres sees only 50-100 connections instead of 9,090
- **Trade-off:** Transaction-pooling mode means sessions can't span multiple transactions — not an issue here since each `get_db_session()` is scoped to one activity

**Alternatives:**
| Approach | Pro | Con |
|----------|-----|-----|
| PgBouncer | No code changes, works with any client | Extra infra to manage |
| RDS Proxy | Managed AWS service | AWS-only, extra cost |
| Reduce per-worker pool | Simple (e.g. `pool_size=2, max_overflow=2`) | 4 × 101 = 404 connections — still high |
| Connection pooling in app | No extra infra | Same perf as PgBouncer, less flexible |

---

## What causes "grpc: received message larger than max" and "PayloadSizeWarning"?

**Short answer:** Activity results or arguments exceed Temporal's **4 MB gRPC message size limit**. This is not a history-size or Continue-As-New problem.

### What it looks like in logs:

```text
PayloadSizeWarning
Size: 9496702 bytes
Limit: 524288 bytes

grpc: received message larger than max (9496960 vs. 4194304)
```

### What's happening:

1. FetchActivity returns raw RSS/XML content as `{"task_id": "xxx", "raw_xml": "<rss>...</rss>"}`
2. The UrlWorkflow receives this result and passes `raw_xml` as an argument to ParseActivity
3. Both the activity result and the activity argument are serialized into Temporal's event history
4. If the raw XML is ~9.5 MB, it exceeds the 4 MB gRPC limit
5. The activity completion fails **before history is written**

### What does NOT fix this:

| ❌ Won't fix | Why |
|---|---|
| Continue-As-New | Error occurs before history is written |
| Child workflows | Same gRPC limit applies |
| More workers | Not a throughput problem |
| Increasing BATCH_SIZE | Not related to batching |

### The real fix:

Stop passing large payloads through Temporal. Store content in the database and pass only references:

**Anti-pattern (current):**
```
FetchActivity → returns raw_xml → workflow → passes raw_xml to ParseActivity
```

**Correct:**
```
FetchActivity → stores raw_xml in DB → returns task_id → workflow → passes task_id to ParseActivity → reads raw_xml from DB
```

See `docs/ARCHITECTURE.md#payload-size-anti-pattern-raw-xml-through-temporal` for full details.

---

## What about CPU-bound summarization?

`TextRank + TF-IDF + LSA` summarization runs synchronously inside the summarize worker's event loop. Since each queue is its own **OS process**, a blocking CPU call in summarize doesn't affect fetch or parse workers. The main mitigation is **reducing `max_articles`** and **scaling summarize workers** independently with `--scale summarize-worker=N`.

---

## Can we skip YouTube feeds entirely?

If you know certain URLs will always fail (e.g., specific deleted YouTube channels), you could filter them before submitting the job. This is a client-side optimization — the system already handles them gracefully by marking them as failed.

---

## What is the expected performance after these fixes?

| Metric | Before | After (expected) |
|--------|--------|------------------|
| 101 URLs, 1 process per queue | ~10-15 min | ~2-3 min |
| YouTube URL failure time | ~3-5s each | ~1-2s each |
| DB connections per worker pod | 30 max | 90 max |
| Articles processed per feed | 20 | 10 |
| Worker queues | 4 | 6 (workflow, url-workflow, fetch, parse, enrichment, summarize) |
| Job status derivation | Incremental DB update | Derived from task counts at query time (safety net) |
| Thread pool | `asyncio.to_thread` used | None needed (per-process isolation) |
