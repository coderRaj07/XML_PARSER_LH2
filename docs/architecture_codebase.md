# XML Feed Summarizer -- Architecture

## System Overview

```
                          ┌─────────────┐
                          │   FastAPI   │  POST /jobs, GET /jobs/:id
                          │   (app)     │  GET /jobs/:id/records
                          └──────┬──────┘
                                 │ starts workflow
                          ┌──────▼───────┐
                          │  Temporal    │
                          │  Server      │  port 7233
                          └──┬───┬───┬───┘
                             │   │   │
            ┌────────────────┘   │   └────────────────┐
            ▼                    ▼                     ▼
   ┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
   │  workflow   │     │  fetch       │     │  parse       │     │ summarize    │
   │  worker     │     │  worker(s)   │     │  worker(s)   │     │ worker(s)    │
   │  1 instance │     │  up to 50    │     │  up to 50    │     │ up to 4      │
   │             │     │  concurrent  │     │  concurrent  │     │ concurrent   │
   └─────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
        │                    │                      │                     │
        │  4 separate Temporal task queues          │                     │
        │  xml-feed-workflow-queue                  │                     │
        │  xml-feed-fetch-queue                     │                     │
        │  xml-feed-parse-queue                     │                     │
        │  xml-feed-summarize-queue                 │                     │
        │                                           │                     │
        └──────────────────┬────────────────────────┘─────────────────────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │PostgreSQL│ │  MinIO   │ │ aiohttp  │
        │  16      │ │  S3      │ │ (extern) │
        │ port 5432│ │port 9000 │ │          │
        └──────────┘ └──────────┘ └──────────┘
```

---

## Job Lifecycle (End to End)

```
POST /jobs {"urls": [...101 URLs...]}
        │
        ▼
  JobService.create_job()
        │
        ├── 1. Job(total_tasks=101) saved to DB as PENDING
        ├── 2. Scheduler creates 101 Task objects, each saved to DB as PENDING
        ├── 3. Job status → RUNNING, saved to DB
        └── 4. TemporalEngine.start_job() starts JobWorkflow on Temporal
                │
                ▼
         ┌─── JobWorkflow.run() ───────────────────────────┐
         │                                                 │
         │  batch = tasks[:50]    (first 50)               │
         │  remaining = tasks[50:]  (remaining 51)         │
         │                                                 │
         │  FOR EACH (task_id, url) in batch:              │
         │    start_child_workflow("url-workflow")         │
         │      ↳ non-blocking, returns handle instantly   │
         │      ↳ id = "{job_id}/url/{task_id}"            │
         │      ↳ REJECT_DUPLICATE policy                  │
         │                                                 │
         │  IF remaining:                                  │
         │    workflow.continue_as_new(args=[job_id,       │
         │                                  remaining])    │
         │    ↳ TERMINAL - current execution ends          │
         │    ↳ new execution starts with remaining tasks  │
         │                                                 │
         │  asyncio.wait(futures, timeout=BATCH_GATHER)    │
         │    ↳ waits for THIS batch's children only       │
         │    ↳ done tasks: collect results                │
         │    ↳ pending tasks: cancel, count as failed     │
         └─────────────────────────────────────────────────┘
                        │
                        │  (per child workflow)
                        ▼
         ┌─── UrlWorkflow.run() ─────────────────────────────┐
         │                                                   │
         │  Activity 1: fetch_url (FETCH_QUEUE)              │
         │    ↳ aiohttp fetch URL                            │
         │    ↳ store raw XML in MinIO                       │
         │    ↳ timeout: 5 min, retry: 3                     │
         │                                                   │
         │  Activity 2: parse_records (PARSE_QUEUE)          │
         │    ↳ retrieve XML from MinIO                      │
         │    ↳ feedparser.parse() → Records                 │
         │    ↳ fetch full article content (concurrent)      │
         │    ↳ gzip + store in MinIO                        │
         │    ↳ save Records to DB                           │
         │    ↳ timeout: 5 min, retry: 3                     │
         │                                                   │
         │  Activity 3: summarize_records (SUMMARIZE_QUEUE)  │
         │    ↳ read Records from DB                         │
         │    ↳ TextRank → TF-IDF → LSA → fallback           │ 
         │    ↳ save Summaries to DB                         │
         │    ↳ mark task COMPLETED in DB                    │
         │    ↳ update job progress in DB                    │
         │    ↳ timeout: 5 min, retry: 3                     │
         └───────────────────────────────────────────────────┘
                        │
                        ▼
              Job.update_progress(completed, failed)
                ↳ when completed + failed == total_tasks
                ↳ job status → COMPLETED
```

---

## `continue_as_new` Explained

**Purpose:** Prevents unbounded Temporal workflow history growth. Each child workflow start/complete adds events. With many children, history would grow large.

**How it works:**

1. `JobWorkflow` takes first 50 tasks, starts them as child workflows via `workflow.start_child_workflow()` (non-blocking)
2. Calls `workflow.continue_as_new(args=[job_id, remaining_51])` -- this **terminates** the current workflow execution immediately
3. Temporal creates a fresh workflow execution with the 51 remaining tasks
4. The 50 already-started children run independently on Temporal -- they are NOT killed by `continue_as_new`
5. Next execution: batch 51, start 50 more children, `continue_as_new` with 1 remaining
6. Last execution: batch 1, start 1 child, wait for it, return results

**Why non-blocking `start_child_workflow`:** The old code used `execute_child_workflow` (blocking) + `asyncio.gather` before `continue_as_new`. If any child hung, `continue_as_new` never fired and the remaining batch was stranded. Now children are started without waiting.

**`REJECT_DUPLICATE` policy:** If the workflow worker restarts mid-batch, `start_child_workflow` with the same ID won't create duplicates.

---

## All Timeouts and Their Purposes

| Timeout | Value | Where Set | Purpose |
|---------|-------|-----------|---------|
| `ACTIVITY_TIMEOUT` | **5 min** | `config.py:19` env `ACTIVITY_TIMEOUT_MINUTES` | `start_to_close_timeout` on each Temporal activity (fetch, parse, summarize). If an activity runs longer, Temporal kills it and retries. |
| `CHILD_WORKFLOW_TIMEOUT` | **3 min** | `config.py:20` env `CHILD_WORKFLOW_TIMEOUT_MINUTES` | `execution_timeout` on each UrlWorkflow child. If a child's 3-activity pipeline runs longer, Temporal kills it. |
| `BATCH_GATHER_TIMEOUT` | **3 min** | `config.py:21` env `BATCH_GATHER_TIMEOUT_MINUTES` | `asyncio.wait(timeout=...)` on the last batch's gather. If children don't finish in time, pending tasks are cancelled and counted as failed. |
| aiohttp default | **30s** | `aiohttp_fetcher.py:36` | Total timeout per HTTP fetch request for RSS feeds and article content. |
| aiohttp YouTube | **10s** | `aiohttp_fetcher.py:46` | Reduced timeout specifically for YouTube RSS feeds (small, fast endpoints). |
| aiohttp 429 backoff | **5s, 10s, 20s** | `aiohttp_fetcher.py:67` | `5 * (2**attempt)` sleep between rate-limit retries. |
| aiohttp transient backoff | **1s, 2s** | `aiohttp_fetcher.py:104` | `backoff_base * (2**attempt)` sleep between general retries. |
| Temporal retry interval | **1s -> 30s** | `config.py:24-25` | `initial_interval=1s`, `maximum_interval=30s`, `backoff_coefficient=2.0` between Temporal activity retries. |
| DB pool_size | **30** | `session.py:17` | SQLAlchemy persistent connections per engine. |
| DB max_overflow | **60** | `session.py:17` | Extra connections beyond pool_size when all persistent ones are busy. Total max = 90 per worker. |

---

## All Asyncio Patterns

### `workflow.start_child_workflow()` (non-blocking)
**File:** `workflows.py:37-44`
Fires off a child workflow and returns a handle immediately without waiting for completion. This is the key fix -- the old `execute_child_workflow` blocked until the child finished.

### `workflow.continue_as_new()`
**File:** `workflows.py:50`
Terminates the current workflow execution and starts a new one with the given args. The new execution gets a clean history. The old execution's children continue running independently.

### `asyncio.Semaphore(10)`
**File:** `workflows.py:52`
Limits how many child workflow handles we await concurrently in the last batch. Prevents overwhelming the workflow worker.

### `asyncio.create_task()`
**File:** `workflows.py:59`
Wraps coroutines into futures for use with `asyncio.wait`.

### `asyncio.wait(futures, timeout=180)`
**File:** `workflows.py:60`
Waits for batch completion with a hard timeout. Returns `(done, pending)` sets. Pending tasks are cancelled.

### `asyncio.gather(*coros, return_exceptions=True)`
**File:** `activities.py:173` (full-content fetch), `activities.py:223` (summarize)
Runs multiple coroutines concurrently. `return_exceptions=True` means exceptions become results instead of cancelling everything.

### `loop.run_in_executor(None, func, arg)`
**File:** `activities.py:106,147`
Offloads CPU-bound synchronous work (feedparser.parse, trafilatura.extract) to the default thread pool. Used because these libraries are synchronous.

### `loop.run_in_executor(self._executor, func, arg)`
**File:** `activities.py:213-214`
Offloads CPU-bound summarization (TextRank/TF-IDF/LSA) to a **dedicated** `ThreadPoolExecutor(max_workers=4)`. Separate from default pool because summarization is expensive.

### `asyncio.to_thread(boto3_func, ...)`
**File:** `s3_storage.py:37,51,54,62,73,84,96`
Wraps synchronous boto3 S3 calls in threads. boto3 has no async SDK so every S3 operation runs in a thread.

### `asyncio.Lock()` (double-checked locking)
**File:** `s3_storage.py:27,32`
Ensures only one coroutine initializes the boto3 S3 client. First check without lock (fast path), second check with lock (safe path).

### `asyncio.sleep(delay)`
**File:** `aiohttp_fetcher.py:75,105`
Exponential backoff between HTTP fetch retries.

### `asyncio.create_task(w.run())`
**File:** `worker.py:49,61,73,85`
Starts the Temporal worker as a background task within the asyncio event loop.

---

## Threading Patterns

| Pattern | Location | Details |
|---------|----------|---------|
| `ThreadPoolExecutor(max_workers=4)` | `activities.py:189` | Dedicated executor for CPU-bound summarization (TextRank + TF-IDF + LSA). Shared across all concurrent summarize activities in a worker process. |
| `loop.run_in_executor(None, ...)` | `activities.py:106,147` | Default executor (unlimited threads) for feedparser.parse and trafilatura.extract. These are fast I/O-bound-to-CPU-bound operations. |
| `loop.run_in_executor(self._executor, ...)` | `activities.py:213-214` | Dedicated 4-thread pool for summarization. Prevents summarization from starving other work. |
| `asyncio.to_thread(...)` | `s3_storage.py` (all S3 ops) | Every boto3 call (put_object, get_object, list_buckets, create_bucket) runs in a thread. boto3 is synchronous. |

---

## S3 Storage (MinIO)

### Configuration
- **Endpoint:** `http://minio:9000` (env: `S3_ENDPOINT_URL`)
- **Bucket:** `xml-feeds` (env: `S3_BUCKET`)
- **Access Key:** `minioadmin` (env: `S3_ACCESS_KEY_ID`)
- **Secret Key:** `minioadmin` (env: `S3_SECRET_ACCESS_KEY`)
- **Region:** `us-east-1`

### What's Stored

| Data | S3 Key Pattern | Content-Type | When Written |
|------|---------------|-------------|--------------|
| Raw RSS/XML | `feeds/{job_id}/{task_id}.xml` | `application/xml` | `FetchActivity.fetch_url` (`activities.py:63-64`) |
| Full article content | `content/{job_id}/{task_id}/{record_id}.gz` | `application/gzip` | `ParseActivity._fetch_full_contents` (`activities.py:149-152`) |

### Streaming Types

**`store(key, data: str)`** (`s3_storage.py:59-69`): Encodes string to UTF-8 bytes, passes as `Body` to `put_object`. Used for raw XML.

**`store_stream(key, data: io.IOBase, content_type)`** (`s3_storage.py:71-80`): Takes a stream object (e.g. `io.BytesIO(compressed)`), passes directly to `put_object`. Used for gzip-compressed article content. Avoids loading full article text into memory twice.

**`retrieve(key) -> str`** (`s3_storage.py:82-91`): Calls `get_object`, reads full body, decodes UTF-8. Used by ParseActivity to get raw XML back.

**`get_bytes(key) -> bytes`** (`s3_storage.py:93-103`): Same as retrieve but returns raw bytes. Used by record controller to decompress gzip content for the API.

### Lazy Initialization

S3 client is created on first use via double-checked locking (`asyncio.Lock()`). Bucket is auto-created if missing.

---

## Database Schema and Connection Pooling

### Connection Pool

- **Engine:** `create_async_engine(database_url, pool_size=30, max_overflow=60, echo=False)`
- **Max connections per worker:** 30 persistent + 60 overflow = **90 total**
- **Session factory:** `async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)`
- **PostgreSQL max_connections:** default ~100 (no override in docker-compose)
- **Warning:** With 5 worker containers each creating 90 potential connections, total could reach 450. PostgreSQL default is 100. In practice, not all connections are used simultaneously.

### Tables

**`jobs`**
| Column | Type | Default |
|--------|------|---------|
| id | String(36) PK | uuid4 |
| status | String(20) | "pending" |
| total_tasks | Integer | 0 |
| completed_tasks | Integer | 0 |
| failed_tasks | Integer | 0 |
| created_at | DateTime | func.now() |
| completed_at | DateTime | nullable |

**`tasks`**
| Column | Type | Default |
|--------|------|---------|
| id | String(36) PK | uuid4 |
| job_id | String(36) FK -> jobs.id | not null |
| url | Text | not null |
| status | String(20) | "pending" |
| attempts | Integer | 0 |
| error | Text | nullable |
| created_at | DateTime | func.now() |

**`records`**
| Column | Type | Default |
|--------|------|---------|
| id | String(36) PK | uuid4 |
| task_id | String(36) FK -> tasks.id | not null |
| title | Text | "" |
| author | Text | "" |
| published_date | DateTime | nullable |
| source_link | Text | "" |
| description | Text | "" |
| content | Text | "" |
| full_content_s3_key | String(255) | nullable |

**`summaries`**
| Column | Type | Default |
|--------|------|---------|
| id | String(36) PK | uuid4 |
| record_id | String(36) FK -> records.id | not null |
| summary_text | Text | "" |
| summary_type | String(50) | "" |
| model_used | String(100) | "" |

### Key Query

`PostgresTaskRepository.count_by_status(job_id)` (`postgres_task_repository.py:56-66`): Returns `(pending, completed, failed)` counts using `func.count().filter()`. This is how the API and job progress tracking get live status.

---

## HTTP Fetcher (aiohttp)

**File:** `aiohttp_fetcher.py`

### Configuration
- Default timeout: 30s per request
- YouTube timeout: 10s (hardcoded for `youtube.com/feeds/videos.xml`)
- Max retries: 3
- Backoff base: 1.0s
- Connector limit: 100 total connections
- Connector limit per host: 10 connections
- Browser-mimicking headers (Chrome 134 User-Agent)

### Retry Behavior

| HTTP Status | Action |
|-------------|--------|
| 200-299 | Return body |
| 403, 404, 410 | **Immediate failure** -- no retry, raise `RuntimeError("Permanent failure")` |
| 429 | Read `Retry-After` header, else `5 * (2^attempt)` seconds backoff. Retry up to 3 times. |
| 500+ | Log warning, **return body anyway** (partial data is better than none) |
| Connection error | Sleep `backoff_base * (2^attempt)` seconds, retry |

### Retry Chain

Combined fetcher-level + Temporal-level retries: up to **3 x 3 = 9** total attempts per URL. However, 403/404/410 failures are permanent and fail immediately at the fetcher level (1 attempt only).

---

## Summarization Strategy

**File:** `extractive_summary_strategy.py`

### 3-Strategy Fallback Chain

1. **TextRank** (`sumy.summarizers.text_rank.TextRankSummarizer`)
   - Graph-based ranking of sentences
   - Uses stemmer + stop words
   - Most accurate, but can fail on very short content

2. **TF-IDF** (`sklearn.feature_extraction.text.TfidfVectorizer`)
   - Vectorizes sentences, sums TF-IDF scores per sentence
   - Picks top N sentences by score
   - Falls back here if TextRank fails

3. **LSA** (`sumy.summarizers.lsa.LsaSummarizer`)
   - Latent Semantic Analysis
   - Uses stemmer + stop words
   - Falls back here if TF-IDF fails

4. **Raw fallback**: First N sentences joined together

All strategies target `sentences_count=5` (configurable). Source text for summarization uses fallback: `record.content` -> `record.description` -> `record.title` (each checked for garbage content).

---

## API Endpoints

| Method | Path | Request | Response | Status |
|--------|------|---------|----------|--------|
| GET | `/health` | -- | `{"status": "ok"}` | 200 |
| POST | `/jobs` | `{"urls": ["..."]}` | `{"job_id": "..."}` | 201 |
| GET | `/jobs/{job_id}` | -- | `{status, total, completed, failed}` | 200/404 |
| GET | `/jobs/{job_id}/tasks` | -- | `[{id, url, status, attempts, error}]` | 200 |
| GET | `/jobs/{job_id}/records?limit=50&offset=0` | -- | `[{id, title, author, content (max 500 chars), summary}]` | 200/404 |
| GET | `/records/{record_id}` | -- | Full record with `full_content` decompressed from S3 gzip | 200/404 |

---

## Docker Compose Services

| Service | Image | Port(s) | Concurrency |
|---------|-------|---------|-------------|
| `minio` | `minio/minio:latest` | 9000, 9001 | -- |
| `postgres` | `postgres:16-alpine` | 5432 | -- |
| `temporal` | `temporalio/auto-setup:1.23` | 7233 | -- |
| `temporal-ui` | `temporalio/ui:latest` | 8233 -> 8080 | -- |
| `app` | build . | 8000 | -- |
| `workflow-worker` | build . | -- | `JobWorkflow` + `UrlWorkflow` |
| `fetch-worker` | build . | -- | `max_concurrent_activities`: 50 (docker) / 5 (default) |
| `parse-worker` | build . | -- | `max_concurrent_activities`: 50 (docker) / 5 (default) |
| `summarize-worker` | build . | -- | `max_concurrent_activities`: 4 (docker) / cpu_count (default) |

---

## All Configurable Constants

### `src/infrastructure/temporal/config.py`

| Constant | Default | Env Var |
|----------|---------|---------|
| `WORKFLOW_NAME` | `"job-workflow"` | -- |
| `WORKFLOW_QUEUE` | `"xml-feed-workflow-queue"` | `TEMPORAL_WORKFLOW_QUEUE` |
| `FETCH_QUEUE` | `"xml-feed-fetch-queue"` | `TEMPORAL_FETCH_QUEUE` |
| `PARSE_QUEUE` | `"xml-feed-parse-queue"` | `TEMPORAL_PARSE_QUEUE` |
| `SUMMARIZE_QUEUE` | `"xml-feed-summarize-queue"` | `TEMPORAL_SUMMARIZE_QUEUE` |
| `FETCH_WORKER_COUNT` | `5` | `FETCH_WORKERS` |
| `PARSE_WORKER_COUNT` | `5` | `PARSE_WORKERS` |
| `SUMMARIZE_WORKER_COUNT` | `os.cpu_count() or 4` | `SUMMARIZE_WORKERS` |
| `BATCH_SIZE` | `50` | `WORKFLOW_BATCH_SIZE` |
| `MAX_CONCURRENT_URLS` | `10` | `MAX_CONCURRENT_URLS` |
| `ACTIVITY_TIMEOUT` | `5 min` | `ACTIVITY_TIMEOUT_MINUTES` |
| `CHILD_WORKFLOW_TIMEOUT` | `3 min` | `CHILD_WORKFLOW_TIMEOUT_MINUTES` |
| `BATCH_GATHER_TIMEOUT` | `3 min` | `BATCH_GATHER_TIMEOUT_MINUTES` |
| `RETRY_POLICY.maximum_attempts` | `3` | `ACTIVITY_MAX_RETRIES` |
| `RETRY_POLICY.initial_interval` | `1s` | -- |
| `RETRY_POLICY.maximum_interval` | `30s` | -- |
| `RETRY_POLICY.backoff_coefficient` | `2.0` | -- |

### `src/main.py` (Settings)

| Setting | Default | Env Var |
|---------|---------|---------|
| `database_url` | `postgresql+asyncpg://postgres:postgres@localhost:5432/xml_feeds` | `DATABASE_URL` |
| `temporal_host` | `localhost:7233` | `TEMPORAL_HOST` |
| `log_level` | `INFO` | `LOG_LEVEL` |

### `src/infrastructure/fetchers/aiohttp_fetcher.py`

| Constant | Value |
|----------|-------|
| `timeout_seconds` | 30 |
| `max_retries` | 3 |
| `backoff_base` | 1.0 |
| `connector_limit` | 100 |
| `connector_limit_per_host` | 10 |
| YouTube timeout | 10 seconds (hardcoded) |
| `_PERMANENT_STATUSES` | `{403, 404, 410}` |

### `src/infrastructure/storage/s3_storage.py`

| Constant | Value |
|----------|-------|
| `_endpoint_url` | `http://minio:9000` |
| `_bucket` | `xml-feeds` |
| `_access_key_id` | `minioadmin` |
| `_secret_access_key` | `minioadmin` |
| `_region` | `us-east-1` |

### `src/infrastructure/db/session.py`

| Constant | Value |
|----------|-------|
| `pool_size` | 30 |
| `max_overflow` | 60 |
| `echo` | False |

### `src/application/services/task_processor.py` and `activities.py`

| Constant | Value |
|----------|-------|
| `max_articles` | 10 (constructor default) |
| `ThreadPoolExecutor(max_workers)` | 4 |
| `CONCURRENT_PER_DOMAIN` | 10 |
| Garbage detection min length | 50 chars |

---

## File Inventory

### Source Files (`src/`)

| File | Lines | Purpose |
|------|-------|---------|
| `src/__init__.py` | 0 | Package marker |
| `src/main.py` | 129 | FastAPI app, DI container, settings, lifespan |
| `src/domain/enums/job_status.py` | 8 | `JobStatus` enum: PENDING, RUNNING, COMPLETED, FAILED |
| `src/domain/enums/task_status.py` | 8 | `TaskStatus` enum: PENDING, RUNNING, COMPLETED, FAILED |
| `src/domain/entities/job.py` | 36 | `Job` dataclass, `update_progress()` auto-completes job |
| `src/domain/entities/task.py` | 31 | `Task` dataclass, `increment_attempts()`, `mark_failed()` |
| `src/domain/entities/record.py` | 20 | `Record` dataclass, holds article metadata + S3 key |
| `src/domain/entities/summary.py` | 11 | `Summary` dataclass |
| `src/application/interfaces/repositories.py` | 129 | ABCs: JobRepository, TaskRepository, RecordRepository |
| `src/application/interfaces/fetcher.py` | 7 | Fetcher ABC |
| `src/application/interfaces/execution_engine.py` | 12 | ExecutionEngine ABC |
| `src/application/strategies/parser/base_parser_strategy.py` | 9 | ParserStrategy ABC |
| `src/application/strategies/parser/rss_parser.py` | 52 | RSSParser using feedparser |
| `src/application/strategies/summary/base_summary_strategy.py` | 7 | SummaryStrategy ABC |
| `src/application/strategies/summary/template_summary_strategy.py` | 24 | Template-based summary |
| `src/application/strategies/summary/extractive_summary_strategy.py` | 93 | TextRank + TF-IDF + LSA fallback chain |
| `src/application/services/scheduler.py` | 13 | Creates Task objects from URLs |
| `src/application/services/summary_service.py` | 9 | Delegates to SummaryStrategy |
| `src/application/services/job_service.py` | 45 | Creates jobs, persists, starts workflow |
| `src/application/services/task_processor.py` | 205 | Non-Temporal task processor (used in tests) |
| `src/infrastructure/db/session.py` | 59 | DatabaseSessionManager, connection pooling |
| `src/infrastructure/db/models.py` | 67 | SQLAlchemy ORM: jobs, tasks, records, summaries |
| `src/infrastructure/repositories/postgres_job_repository.py` | 71 | Job CRUD |
| `src/infrastructure/repositories/postgres_task_repository.py` | 87 | Task CRUD + `count_by_status()` |
| `src/infrastructure/repositories/postgres_record_repository.py` | 145 | Record CRUD + summaries |
| `src/infrastructure/fetchers/aiohttp_fetcher.py` | 109 | Async HTTP with retry, backoff, permanent failure detection |
| `src/infrastructure/storage/s3_storage.py` | 109 | MinIO S3 storage, lazy client init |
| `src/infrastructure/temporal/config.py` | 27 | All Temporal constants with env var overrides |
| `src/infrastructure/temporal/workflows.py` | 129 | `JobWorkflow` (batched fire-and-forget), `UrlWorkflow` (3-activity pipeline) |
| `src/infrastructure/temporal/activities.py` | 253 | `FetchActivity`, `ParseActivity`, `SummarizeActivity` |
| `src/infrastructure/temporal/worker.py` | 90 | Worker process launcher per queue |
| `src/infrastructure/temporal/temporal_engine.py` | 43 | Starts workflows on Temporal |
| `src/infrastructure/temporal/run_worker.py` | 53 | CLI entry point for worker processes |

### Test Files (`tests/`)

| File | Lines | Purpose |
|------|-------|---------|
| `tests/conftest.py` | 40 | Shared fixtures |
| `tests/test_task_processor.py` | 71 | TaskProcessor unit tests |
| `tests/test_summary_strategies.py` | 37 | Summary strategy tests |
| `tests/test_summary_service.py` | 17 | SummaryService tests |
| `tests/test_parser_strategies.py` | 17 | RSSParser tests |
| `tests/test_end_to_end.py` | 90 | E2E integration test script |
| `tests/test_urls.txt` | 101 | Test URL list |

---

## Garbage Detection

**Files:** `activities.py:27-44`, `task_processor.py:27-44`

Identical pattern in both files. Detects useless content before storing/summarizing:

- Strip HTML tags, check if clean text < 50 chars
- Check for patterns: "please enable javascript", "enable javascript to continue", "enable javascript to view", "javascript is required", "your browser does not support javascript", "click here if you are not redirected"

Used in:
- `_fetch_full_contents`: Skip storing garbage article content in S3
- `summarize_records`: Fallback source text selection (content -> description -> title, each checked for garbage)
