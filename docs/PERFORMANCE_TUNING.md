# Performance Tuning & Troubleshooting (Q&A)

## How many workers are we using?

**16 Temporal workers total.** The system runs:

| Queue | Workers | Role |
|-------|---------|------|
| `xml-feed-workflow-queue` | 1 | Orchestrates the 3-stage pipeline |
| `xml-feed-fetch-queue` | 5 | Fetches RSS/XML URLs (I/O-bound) |
| `xml-feed-parse-queue` | 5 | Parses XML, fetches article content, stores records (I/O + CPU) |
| `xml-feed-summarize-queue` | 5 | Generates summaries, updates task/job status (CPU-bound) |

This gives **independent scaling per stage** — a fetch backlog won't starve summarize workers.

The DB connection pool is `pool_size=30` with `max_overflow=60` — up to **90 concurrent connections per worker pod** (see [PgBouncer section](#why-does-each-worker-consume-30-db-connections-what-is-pgbouncer) for the math at scale). The aiohttp connector allows `limit=100` total connections with `limit_per_host=2`.

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
| CPU-bound summarization | ~2-5s per article | TextRank + TF-IDF + LSA via `asyncio.to_thread` |
| YouTube 404 failures | ~3-5s per URL | Network round-trip + 3 retries |
| Single worker | N/A | All 101 tasks compete for 1 worker's DB pool (30 conns) |

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

### ✅ 3. Scale Temporal workers

Instead of 1 worker, run multiple worker containers. Each worker picks up activities from the same task queue and processes them concurrently.

```bash
docker compose up -d --scale worker=5
```

**Change:** `docker-compose.yml` — added comments with scaling instructions.

**Impact:** Near-linear speedup for I/O-bound work. 5 workers = ~2-3 min total.

---

### ✅ 4. Fast-fail invalid YouTube URLs

Detect YouTube RSS URLs before making the HTTP request. YouTube channel URLs returning 404 can be identified by the channel ID pattern, or simply by checking the domain and using a shorter timeout.

**Change:** `aiohttp_fetcher.py` — added YouTube-specific timeout override and early 404 detection.

**Impact:** Saves ~2-3 min on 34 YouTube URLs.

---

## Wont Temporal retry for transient errors like 429?

**Yes.** The current fix only suppresses Temporal retries for **permanent failures** (404/403/410). Transient errors like 429 rate limiting, connection timeouts, and DNS failures still propagate as exceptions, triggering Temporal's retry policy (3 attempts, 1s/2s/4s exponential backoff).

The retry chain for transient errors:
1. **Fetcher level** (aiohttp): 3 retries with 1s/2s backoff
2. **Temporal level** (workflow): 3 retries with 1s/2s/4s backoff

This gives up to **9 total attempts** for transient failures, with increasing backoff — enough to handle temporary network or server issues without wasting time on permanently dead URLs.

---

### ❌ 5. Skip summarization for very short content

Already handled by `_is_garbage()` detection — content under 50 chars is skipped.

---

### ✅ 6. Removed `asyncio.to_thread` (no thread pool needed)

With 5 dedicated workers per queue, each running its own event loop, the `asyncio.to_thread` offloading is redundant. The synchronous `generate_summary()` call now runs directly in the summarize worker's event loop — blocking that worker briefly doesn't affect other stages since each has its own pool.

**Impact:** Simpler code, same throughput, no thread pool management overhead.

---

## Why does each worker consume 30 DB connections? What is PgBouncer?

**The math:** `src/infrastructure/db/session.py:16` sets `pool_size=10, max_overflow=20` = up to **30 concurrent DB connections per worker**. Each activity borrows a session from the pool; if all 30 are in use, subsequent activities queue up.

**The problem at scale:** 101 workers × 30 connections = 3,030 concurrent DB connections. PostgreSQL out-of-the-box is configured for ~100-200 connections. Beyond that, each backend process (fork, auth, memory) becomes the bottleneck — performance degrades sharply.

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
- **Result:** Postgres sees only 50-100 connections instead of 3,030
- **Trade-off:** Transaction-pooling mode means sessions can't span multiple transactions — not an issue here since each `get_db_session()` is scoped to one activity

**Alternatives:**
| Approach | Pro | Con |
|----------|-----|-----|
| PgBouncer | No code changes, works with any client | Extra infra to manage |
| RDS Proxy | Managed AWS service | AWS-only, extra cost |
| Reduce per-worker pool | Simple (e.g. `pool_size=2, max_overflow=2`) | 4 × 101 = 404 connections — still high |
| Connection pooling in app | No extra infra | Same perf as PgBouncer, less flexible |

---

## What about CPU-bound summarization?

`TextRank + TF-IDF + LSA` summarization runs synchronously inside the summarize worker's event loop. With **5 dedicated summarize workers**, each blocking briefly on CPU-bound work is acceptable — the 5-way parallelism at the queue level replaces the need for a thread pool. The main mitigation is **reducing `max_articles`** and **scaling summarize workers** independently.

---

## Can we skip YouTube feeds entirely?

If you know certain URLs will always fail (e.g., specific deleted YouTube channels), you could filter them before submitting the job. This is a client-side optimization — the system already handles them gracefully by marking them as failed.

---

## What is the expected performance after these fixes?

| Metric | Before | After (expected) |
|--------|--------|------------------|
| 101 URLs, 1 workflow + 15 activity workers | ~10-15 min | ~2-3 min |
| YouTube URL failure time | ~3-5s each | ~1-2s each |
| DB connections per worker pod | 30 max | 90 max |
| Articles processed per feed | 20 | 10 |
| Thread pool | `asyncio.to_thread` used | Removed (5 workers per queue) |
