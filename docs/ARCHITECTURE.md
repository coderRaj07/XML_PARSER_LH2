# Architecture

## System Overview

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   FastAPI    │────►│   Temporal   │────►│  PostgreSQL │
│  (port 8000) │     │  (port 7233) │     │  (port 5432)│
└──────┬───────┘     └──────┬───────┘     └─────────────┘
       │                    │                     ▲
       │              ┌─────▼───────┐             │
       │              │   Worker    │─────────────┘
       │              │ (activities)│  (DB commits)
       │              └─────┬───────┘
       │                    │
       │              ┌─────▼───────┐
       │              │  aiohttp    │
       │              │  (fetcher)  │
       │              └─────┬───────┘
       │                    │
       │              ┌─────▼───────┐
       │              │   External  │
       └──────────────│  RSS Feeds  │
                      └─────────────┘
```

## Layer Architecture (Clean Architecture)

```
┌─────────────────────────────────────────────────────────┐
│                     API Layer (FastAPI)                   │
│  job_controller.py │ record_controller.py │ health.py     │
├─────────────────────────────────────────────────────────┤
│                  Application Layer                        │
│  services/ │ strategies/ │ interfaces/                    │
│  TaskProcessor │ SummaryService │ ParserStrategy          │
├─────────────────────────────────────────────────────────┤
│                    Domain Layer                           │
│  entities/ │ enums/ │ interfaces                          │
│  Task │ Record │ Summary │ Job                            │
├─────────────────────────────────────────────────────────┤
│                 Infrastructure Layer                      │
│  repositories/ │ fetchers/ │ temporal/ │ db/              │
│  Postgres*Repo │ AioHttpFetcher │ Worker │ Models         │
└─────────────────────────────────────────────────────────┘
```

## Concurrency Model

### Feed-Level: Temporal Workflow Fan-Out

```
┌──────────────────────────────────────────────────┐
│  JobWorkflow.run(job_id, tasks)                   │
│                                                    │
│  asyncio.gather(                                   │
│    execute_activity("process_url", task1, url1),   │
│    execute_activity("process_url", task2, url2),   │
│    ...  ← all dispatched concurrently             │
│  )                                                 │
└──────────────────────────────────────────────────┘
```

All feed URLs are dispatched concurrently via `asyncio.gather`. The Temporal worker processes up to `max_concurrent_activities` (default 100) at once.

### Article-Level: Per-Domain Concurrency

Inside a single feed's activity, article content fetching is now concurrent:

```
_fetch_full_contents(records=[r1, r2, r3, r4, ...])
  │
  ├── domain_a: Semaphore(2) ─── r1 (fetch + extract)
  │   └── domain_a: Semaphore(2) ─── r2 (fetch + extract)
  │
  ├── domain_b: Semaphore(2) ─── r3 (fetch + extract)
  │
  └── domain_c: Semaphore(2) ─── r4 (fetch + extract)
       (all domains run concurrently via asyncio.gather)
```

- **Different domains**: fully concurrent (no throttling between them)
- **Same domain**: up to 2 concurrent with 1s gap between start times
- **Config**: `CONCURRENT_PER_DOMAIN = 2`, throttle = 1.0s

### Activity-Level: Per-Activity DB Sessions

```
Worker
  ├── Activity 1: session₁ → fetch RSS → parse → fetch_full_contents → store → summarize → commit
  ├── Activity 2: session₂ → fetch RSS → parse → fetch_full_contents → store → summarize → commit
  └── Activity N: sessionₙ → ...
       Each activity gets a fresh AsyncSession from the pool (pool_size=10, max_overflow=20)
```

## Data Flow

```
POST /jobs {"urls": [...]}
  │
  ▼
Create Job + Tasks in DB
  │
  ▼
Start Temporal Workflow (JobWorkflow)
  │
  ▼
asyncio.gather(all tasks)
  │
  ├──► Activity: process_url(task_id, url, job_id)
  │      │
  │      ├── 1. Fetch RSS XML (aiohttp)
  │      ├── 2. Parse XML → Records (feedparser)
  │      ├── 3. Truncate to max_articles=20
  │      ├── 4. Fetch full article content (trafilatura) ← CONCURRENT
  │      │      - Gzip-compress full_content
  │      ├── 5. Store Records in DB
  │      └── 6. Generate Summaries (TextRank + TF-IDF + LSA)
  │
  └──► ... (repeat for each URL)

Worker commits DB session → Job progress updated
```

## Key Decisions

### Why Temporal?
- Durable execution with built-in retry + backoff
- Workflow-level fan-out via `asyncio.gather`
- State visibility via Temporal UI and API
- No need for Redis/RabbitMQ/SQS

### Why aiohttp?
- Native async HTTP client for asyncio
- Connection pooling with `limit_per_host`
- Cookie support via `CookieJar`
- Streaming response support

### Why trafilatura?
- Extracts article content from HTML (strips navigation, ads, etc.)
- More reliable than readability-lxml for diverse sites
- Python-native, no external service dependency

### Why per-activity DB sessions?
- Avoids session contention across concurrent activities
- Each activity commits independently (partial success)
- Rollback scope is per-activity, not per-job

## Fetcher Design

### HTTP Client Configuration

```
Session: aiohttp.ClientSession
  ├── Connector: TCPConnector(limit=100, limit_per_host=2)
  ├── Timeout: ClientTimeout(total=30s)
  ├── Headers: Chrome 134 browser headers
  │     ├── User-Agent: Mozilla/5.0 ... Chrome/134 ...
  │     ├── Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
  │     ├── Accept-Language: en-US,en;q=0.9
  │     ├── Sec-Fetch-Dest: document
  │     ├── Sec-Fetch-Mode: navigate
  │     ├── Sec-Fetch-Site: none
  │     ├── Sec-Fetch-User: ?1
  │     └── Upgrade-Insecure-Requests: 1
  └── CookieJar (implicit, per-session)
```

### Retry Policy

| Attempt | Backoff | Triggers |
|---------|---------|----------|
| 0 | immediate | Any non-2xx except 403/404/410/429 |
| 1 | 1s | Same |
| 2 | 2s | Last attempt, then raises `RuntimeError` |

**Special status codes:**
- **403/404/410 (Permanent)**: Raise `RuntimeError` immediately — no retry within activity, Temporal retries up to 3 times (~3s wasted per URL)
- **429 (Rate limited)**: Respect `Retry-After` header if present, else exponential backoff 5s/10s/20s. After exhaustion, raise `ClientResponseError`
- **5xx (Server error)**: Log warning, return body anyway — YouTube often returns 500 with valid XML body

## Performance

### Feed-Level Timings (20 articles, same domain)

| Mode | Per-Feed Time | Total (101 feeds) |
|-------|--------------|-------------------|
| Sequential article fetch (before) | ~40-60s | ~25-40 min |
| Concurrent article fetch (after) | ~12-20s | ~8-14 min |
| YouTube (0 articles, fail fast) | ~3-5s | ~2-3 min |

### Overall Job (101 URLs with all fixes)

| Metric | Value |
|--------|-------|
| Successful feeds | 67 |
| Permanently failed | 32 (30 YouTube 404 + 2 Cloudflare) |
| Fixed by patches (applied after run) | 12 (8 YouTube 500 body + 4 YouTube headers) |
| Total time (single worker) | ~10-15 min |
| Bottleneck | Article-level HTTP fetching + summarization |

## Failure Analysis

### Final Result: 67/101 succeed, 32 permanently fail, 12 fixed by patches

### Category Breakdown

| # | Category | Count | HTTP | Verdict |
|---|----------|-------|------|---------|
| 1 | Invalid YouTube channel | 30 | 404 | **Permanent** — channel deleted/never existed |
| 2 | YouTube with valid XML body | 8 | 500 | **Fixed** — return body regardless of status |
| 3 | YouTube missing browser headers | 4 | 500 | **Fixed** — added Sec-Fetch-* headers |
| 4 | Cloudflare WAF (tripwire.com, sony.com) | 2 | 403 | **Permanent** — needs headless browser |
| | **Total** | **44** | | **32 permanent + 12 fixed** |

### Error Handling Matrix

| Scenario | Behavior |
|----------|----------|
| RSS feed HTTP 404 | Permanent failure — `RuntimeError`, no retry within activity |
| RSS feed HTTP 403 | Permanent failure — same as 404 |
| RSS feed HTTP 500 with body | **Body returned anyway** — parser downstream validates content |
| RSS feed HTTP 429 | Retry with `Retry-After` or exponential backoff (5s/10s/20s) |
| RSS feed connection timeout | Retry with 1s/2s backoff, up to 3 attempts |
| Article link HTTP 404/403 | Permanent failure — skipped, other articles continue |
| Article link HTTP 500 | Body returned, trafilatura may extract nothing |
| Article link timeout | Logged as warning, article skipped |
| Trafilatura garbage content | `_is_garbage()` detection — not stored, summary falls back to description/title |
| DB connection failure | Activity rollback, retry by Temporal (1s/2s/4s/.../30s backoff) |
| Temporal cancellation during throttle sleep | CancelledError caught — partial results saved, activity marked cancelled |
| Worker restart mid-processing | In-flight activities fail, Temporal retries them
