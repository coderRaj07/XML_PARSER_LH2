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

## Performance

Feed-level timings (20 articles, same domain):
- Sequential (before): ~40-60s per feed
- Concurrent (after): ~12-20s per feed
- YouTube channels (0 articles): ~3-5s total

## Failure Modes

| Scenario | Behavior |
|----------|----------|
| RSS feed 404 | Task failed permanently, no retry |
| RSS feed 500 with body | Body returned anyway, parser validates |
| Article fetch timeout | Skipped (warning logged), other articles continue |
| Article garbage content | Skipped (garbage detected), not stored |
| DB connection failure | Activity rollback, retry by Temporal |
| Temporal cancellation | Partial results saved, activity marked cancelled |
