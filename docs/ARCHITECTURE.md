# Architecture

## System Overview

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   FastAPI    │────►│   Temporal   │────►│  PostgreSQL │
│  (port 8000) │     │  (port 7233) │     │  (port 5432)│
└──────────────┘     └──────┬───────┘     └─────────────┘
                            │
                    ┌───────┴────────┐
                    │ Workflow Worker│
                    │ (1 instance)   │
                    └───────┬────────┘
                            │
          ┌─────────────────┼─────────────────┐
          │                 │                  │
  ┌───────▼────────┐ ┌──────▼────────┐ ┌──────▼────────┐
  │  Fetch Queue   │ │  Parse Queue  │ │  Summarize Q  │
  │ (5 workers)    │ │ (5 workers)   │ │ (5 workers)   │
  └───────┬────────┘ └──────┬────────┘ └──────┬────────┘
          │                 │                  │
    ┌─────▼───────┐   ┌─────▼───────┐   ┌─────▼───────┐
    │  aiohttp    │   │  Postgres   │   │  sumy       │
    │  (fetcher)  │   │  (records)  │   │ (summaries) │
    └─────┬───────┘   └─────────────┘   └─────────────┘
          │
    ┌─────▼───────┐
    │   External  │
    │  RSS Feeds  │
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

### Feed-Level: 3-Stage Pipeline with Dedicated Queues

```
┌──────────────────────────────────────────────────────────┐
│  JobWorkflow.run(job_id, tasks)                           │
│                                                            │
│  Stage 1: Fetch all URLs (xml-feed-fetch-queue)           │
│    asyncio.gather(                                         │
│      execute_activity("fetch_url", t1, u1, j1),           │
│      execute_activity("fetch_url", t2, u2, j2),           │
│      ...                                                   │
│    )                                                       │
│                                                            │
│  Stage 2: Parse all successfully fetched XMLs              │
│           (xml-feed-parse-queue)                           │
│    asyncio.gather(                                         │
│      execute_activity("parse_records", t1, raw_xml1, j1), │
│      execute_activity("parse_records", t2, raw_xml2, j2), │
│      ...                                                   │
│    )                                                       │
│                                                            │
│  Stage 3: Summarize all parsed records                     │
│           (xml-feed-summarize-queue)                       │
│    asyncio.gather(                                         │
│      execute_activity("summarize_records", t1, j1),       │
│      execute_activity("summarize_records", t2, j2),       │
│      ...                                                   │
│    )                                                       │
└──────────────────────────────────────────────────────────┘
```

Each stage uses a separate task queue with 5 dedicated workers. Failed tasks at any stage are filtered out of subsequent stages.

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

### Activity-Level: Per-Activity DB Sessions (FastAPI-style)

```
FetchActivity (fetch-queue)
  └── async with session_factory() as session:
        fetcher.fetch(url) → raw_xml
        (no DB needed unless error — then task marked failed)

ParseActivity (parse-queue)
  └── async with session_factory() as session:
        parser.parse(raw_xml) → records
        fetch_full_contents(records)  ← concurrent per-domain
        record_repo.create_many(records)
        session.commit()

SummarizeActivity (summarize-queue)
  └── async with session_factory() as session:
        record_repo.list_by_task(task_id) → records
        for each record: generate_summary → save_summary
        task.mark_completed() → task_repo.update()
        job.update_progress() → job_repo.update()
        session.commit()

Each activity gets a fresh AsyncSession from the pool, managed via
`async with session_factory() as session:` — auto-closes on exit,
auto-rollbacks on exception (FastAPI-style lifecycle).
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
Stage 1 — Fetch Queue (5 workers)
  │  asyncio.gather(all tasks)
  │  ├──► fetch_url(task_id, url, job_id)
  │  │      └── aiohttp.get(url) → raw_xml
  │  └──► ... (repeat for each URL)
  │
  ▼
Stage 2 — Parse Queue (5 workers)
  │  asyncio.gather(successful fetches)
  │  ├──► parse_records(task_id, raw_xml, job_id)
  │  │      ├── feedparser.parse(raw_xml) → records
  │  │      ├── Truncate to max_articles=10
  │  │      ├── Fetch full article content ← CONCURRENT per-domain
  │  │      │      (trafilatura, gzip-compress full_content)
  │  │      └── Store records in DB → session.commit()
  │  └──► ... (repeat for each successful fetch)
  │
  ▼
Stage 3 — Summarize Queue (5 workers)
  │  asyncio.gather(successful parses)
  │  ├──► summarize_records(task_id, job_id)
  │  │      ├── list records by task_id from DB
  │  │      ├── generate summaries (TextRank + TF-IDF + LSA)
  │  │      ├── mark task completed
  │  │      └── update job progress → session.commit()
  │  └──► ... (repeat for each successful parse)
```

## Key Decisions

### Why 3 separate queues?
- **Independent scaling**: fetch (I/O-bound), parse (I/O + CPU), summarize (CPU-bound) each have 5 dedicated workers
- **Isolation**: a fetch backlog doesn't starve summarize workers; each stage can be tuned independently
- **Pipeline efficiency**: all fetches complete before parsing starts, preventing partial work

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

### Why async context manager for sessions?
- **FastAPI-style lifecycle**: `async with session_factory() as session:` — auto-closes on exit, auto-rollbacks on error
- **No manual cleanup**: eliminates `try/finally { close() }` boilerplate
- **Consistent across API + workers**: same pattern in controllers and Temporal activities

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

**Temporal activity retry policy** (in `workflows.py:25-30`):
- `maximum_attempts=3` — only triggers if the activity raises an exception
- Permanent failures are persisted as task failures and returned (no exception) — Temporal does not retry
- Only transient errors (network timeouts, rate limits, DB errors) propagate and trigger Temporal retries

**Special status codes:**
- **403/404/410 (Permanent)**: Raise `RuntimeError` immediately — no retry within activity. Task is persisted as failed and temporal does not retry the activity
- **429 (Rate limited)**: Respect `Retry-After` header if present, else exponential backoff 5s/10s/20s. After exhaustion, raise `ClientResponseError`
- **5xx (Server error)**: Log warning, return body anyway — YouTube often returns 500 with valid XML body

## Performance

### Feed-Level Timings (same domain)

| Mode | Per-Feed Time | Total (101 feeds) |
|-------|--------------|-------------------|
| Sequential article fetch (before, 20 articles) | ~40-60s | ~25-40 min |
| Concurrent article fetch (after, 20 articles) | ~12-20s | ~8-14 min |
| Concurrent article fetch (10 articles) | ~6-10s | ~4-7 min |
| YouTube (0 articles, fail fast) | ~1-2s | ~0.5-1 min |

### Overall Job (101 URLs with all fixes)

| Metric | Value |
|--------|-------|
| Successful feeds | 67 |
| Permanently failed (in this run) | 34 (32 YouTube 404 + 2 Cloudflare 403) |
| Additional URLs fixable by patches | 12 (8 YouTube 500 body + 4 YouTube headers) |
| Expected with all fixes | 79/101 |
| Total time (1 worker, max_articles=20) | ~10-15 min |
| Total time (1 worker, max_articles=10) | ~6-8 min |
| Total time (5 workers, max_articles=10) | ~2-3 min |
| Bottleneck | Article-level HTTP fetching + summarization |

## Failure Analysis

### Latest Run: 67/101 succeed, 34 fail. All 34 are HTTP 404/403.

| Category | Count | HTTP | Root Cause |
|----------|-------|------|------------|
| Invalid/deleted YouTube channels | 32 | 404 | Channel never existed or was deleted |
| Cloudflare WAF (tripwire.com, sony.com) | 2 | 403 | TLS fingerprint + JS challenge |
| **Total** | **34** | | **All permanent** |

### Additional Fixes (applied separately, not reflected in this run)

| Fix | Count | HTTP | Behavior |
|-----|-------|------|----------|
| Return 5xx body regardless of status | 8 | 500 | YouTube 500 with valid XML — now accepted |
| Added Sec-Fetch-* headers | 4 | 500 | YouTube requires modern browser security headers |

These 12 channels would have succeeded with fixes applied, giving **79/101** expected.

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
