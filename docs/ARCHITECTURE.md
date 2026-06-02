# Architecture

## Layered Design

```
┌──────────────────────────────────────────────────────────┐
│                        API Layer                         │
│               FastAPI / job_controller.py                │
└──────────────────────┬───────────────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────────────┐
│                   Application Services                   │
│  ┌──────────────┐  ┌───────────┐  ┌──────────────────┐   │
│  │  JobService  │  │ Scheduler │  │  SummaryService  │   │
│  └──────┬───────┘  └─────┬─────┘  └────────┬─────────┘   │
└─────────┼─────────────────┼──────────────────┼───────────┘
          │                 │                  │
┌─────────▼─────────────────▼──────────────────▼───────────┐
│                   Strategy Layer                         │
│                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────┐  │
│  │     Queue    │  │    Parser    │  │    Summary     │  │
│  │   Strategy   │  │   Strategy   │  │    Strategy    │  │
│  │              │  │              │  │                │  │
│  │• FIFO        │  │• RSS         │  │• Template      │  │
│  │              │  │              │  │• Extractive    │  │
│  │              │  │              │  │                │  │
│  └──────────────┘  └──────────────┘  └────────────────┘  │
└─────────┬─────────────────┬──────────────────┬───────────┘
          │                 │                  │
┌─────────▼─────────────────▼──────────────────▼───────────┐
│               Infrastructure Layer                       │
│                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────┐  │
│  │   Temporal   │  │ PostgreSQL   │  │   Fetcher      │  │
│  │   Engine     │  │ + SQLAlchemy │  │  (aiohttp)     │  │
│  └──────────────┘  └──────────────┘  └────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

### Pipeline Flow

```
POST /jobs (100 URLs)
    │
    ▼
JobService.create_job()
    │
    ├──► Scheduler.schedule_job()  → creates Task entities
    │
    └──► TemporalEngine.start_job() → launches JobWorkflow
                  │
                  ▼
          JobWorkflow (fan-out via asyncio.gather)
                  │
        ┌─────────┼─────────┐
        ▼         ▼         ▼
   URLProcessingActivity (×N, parallel)
        │
        ├──► Fetcher.fetch(url)
        ├──► ParserStrategy.parse(xml) → list[Record]
        ├──► RecordRepository.create_many()
        ├──► SummaryService.generate_summary()
        │       └──► SummaryStrategy.summarize()
        └──► TaskRepository.update()
```

## Project Structure

```
src/
├── api/
│   └── job_controller.py          # FastAPI endpoints
├── application/
│   ├── interfaces/                # Abstract base classes
│   │   ├── execution_engine.py    # Temporal abstraction
│   │   ├── fetcher.py             # HTTP fetch contract
│   │   └── repositories.py        # Data access contracts
│   ├── services/
│   │   ├── job_service.py         # Job orchestration
│   │   ├── scheduler.py           # Task scheduling + queue
│   │   ├── summary_service.py     # Summary delegation
│   │   └── task_processor.py      # Core processing pipeline
│   └── strategies/
│       ├── queue/                 # Queue selection strategies
│       ├── parser/                # Feed format strategies
│       └── summary/               # Summarization strategies
├── domain/
│   ├── entities/                  # Job, Task, Record, Summary
│   └── enums/                     # JobStatus, TaskStatus
├── infrastructure/
│   ├── db/                        # SQLAlchemy models + session
│   ├── fetchers/                  # aiohttp fetcher
│   ├── repositories/              # PostgreSQL implementations
│   └── temporal/                  # Workflows + activities + engine + worker
└── main.py                        # FastAPI app + DI container
```

## Strategy Usage

### Parser Strategy (used by `TaskProcessor` & Temporal `URLProcessingActivity`)

| Strategy      | Status      | Description                            |
|---------------|-------------|----------------------------------------|
| `RSSParser`   | ✅ **Active** | RSS 2.0 feed parsing via `feedparser`  |

### Summary Strategy (used by `SummaryService` & Temporal `URLProcessingActivity`)

| Strategy                          | Status            | Description                                        |
|-----------------------------------|-------------------|----------------------------------------------------|
| `TemplateSummaryStrategy`         | 📝 Available      | Structured template (word/char count, first sentence) |
| `ExtractiveSummaryStrategy`       | ✅ **Active**     | TextRank + TF-IDF + LSA via `sumy`/`scikit-learn`  |

---

# Design Decisions

## Why This Architecture?

The system is built around **Temporal + asyncio** for concurrency rather than Celery, task queues (Redis/RabbitMQ/SQS), or raw asyncio. Here's why:

| Approach                               | Pros                                                                     | Cons                                                                           | Chosen?   |
|----------------------------------------|--------------------------------------------------------------------------|--------------------------------------------------------------------------------|-----------|
| **Temporal + asyncio**                 | Durable execution, built-in retry+backoff, fan-out via `asyncio.gather`  | Heavy dependency (server+DB), learning curve, deterministic workflow code     | **✅ Yes** |
| Raw asyncio + `asyncio.gather`         | Simple, zero deps                                                        | No durability, no retry, no state visibility                                  | ❌ No     |
| Celery + Redis/RabbitMQ                | Mature, well-known                                                       | Manual retry, no workflow state mgmt, no built-in fan-out                     | ❌ No     |
| Thread pool + asyncio                  | Simple for CPU-bound work                                                | Higher overhead for I/O-bound work, GIL contention                            | ❌ No     |

**`asyncio.gather`** is used inside the workflow to fan out activities. This was chosen over sequential dispatch or batching because:
- All 101 URLs are independent (no ordering constraints), so parallel dispatch is optimal.
- Temporal handles retries per-activity — a single failure doesn't block others.
- `asyncio.gather(return_exceptions=True)` lets the workflow collect all results without failing fast on the first error.

---

## Concurrency Model

```
Workflow (deterministic)
    │
    ├──► asyncio.gather(
    │       activity("process_url", task_1, url_1),
    │       activity("process_url", task_2, url_2),
    │       ...                              ← all dispatched concurrently
    │   )
    │
    ▼
Worker (non-deterministic)
    ├──► Activity 1: session_factory() → session₁ → fetch → parse → store → summarize → commit → close
    ├──► Activity 2: session_factory() → session₂ → fetch → parse → store → summarize → commit → close
    └──► Activity N: session_factory() → sessionₙ → ...
         Each activity gets its own DB connection from the pool (pool_size=10, max_overflow=20)
```

Key properties:
- **Per-activity DB sessions** — each activity creates a fresh `AsyncSession` from the factory, so N concurrent activities use N independent connections from the pool.
- **Shared aiohttp session** — the `AioHttpFetcher`'s `ClientSession` is long-lived and shared across all activities (safe — aiohttp supports concurrent requests on one session via its connection pool).
- **No thread pool** — all I/O is asyncio-based. CPU-bound summarization (TextRank, TF-IDF) runs inline in the async task, which blocks the event loop.
- **Temporal controls dispatch rate** — the worker has a max concurrent activity limit (configurable, default ~200). Activities beyond this are queued by Temporal until capacity opens.

---

## Why Temporal for Queue/Worker Instead of a Message Queue?

The app does **not** use Redis, RabbitMQ, SQS, or any traditional message queue. Temporal's task queue serves as both the queue and the worker orchestrator:

1. **Queue** — the workflow enqueues activities by calling `workflow.execute_activity()`. Temporal stores these tasks durably in its own database (PostgreSQL). No separate queue infrastructure needed.

2. **Worker** — the Python worker polls Temporal's task queue, receives activity tasks, executes them, and reports results back. Temporal tracks completion, retries failures, and advances the workflow.

3. **Why not a traditional queue?** With SQS/RabbitMQ, you'd need to build your own state machine (track which URLs are done, which failed, retry logic). With Temporal, the workflow code _is_ the state machine — `asyncio.gather` naturally waits for all activities to complete, and Temporal persists the workflow state between events.

4. **Downside** — Temporal server is itself a stateful service requiring PostgreSQL. This is a meaningful operational dependency. For a simpler setup, SQS + Lambda or Redis + Celery would have fewer moving parts.

---

## Logging Strategy

```
Format: JSON (one line per event)
{"time": "...", "name": "...", "level": "INFO", "message": "task_completed"}

Context via extra dict (not in output format but available to log aggregators):
    job_id, task_id, url, attempt, error, count, status
```

Design choices:
- **JSON format** — each log line is a single JSON object, parseable by log aggregators (ELK, Datadog, Loki, CloudWatch). No multi-line stack traces breaking parsers.
- **Structured fields** — `name` = logger name (identifies source component), `level` = severity. The `extra` dict carries domain context.
- **Event-based messages** — messages are event names (`task_started`, `fetch_started`, `fetch_attempt_failed`, `task_completed`, `task_failed`) rather than prose. This enables exact aggregation ("how many fetch failures per URL?") without grep on free text.
- **No PII in logs** — URLs and errors are logged, but not full feed content or auth tokens.
- **Current limitation** — the log format renders `extra` context not visible in console output. Fix: add `extra` fields to the format string or use a log aggregator that parses JSON-encoded messages from `message`.

---

## Scaling Analysis: Where This Breaks

Tested at **101 URLs**: 97 completed, 4 failed, ~2 min total.

### 10× Scale — 1,000 URLs

| Bottleneck                           | Limit                                            | Impact                                                                   |
|--------------------------------------|--------------------------------------------------|--------------------------------------------------------------------------|
| 🔌 **DB connection pool**            | `pool_size=10, max_overflow=20` → max 30 conns  | 970 activities block waiting for a connection; throughput drops to ~30/s |
| 🌐 **aiohttp connector limit**       | `TCPConnector(limit=100)`                       | HTTP requests queue up — adds latency, no failures                       |
| ⚙️ **Temporal worker concurrency**   | Default ~200 concurrent activities               | Beyond 200, Temporal queues server-side — added latency                  |
| 🗄️ **PostgreSQL write throughput**   | ~5K-20K writes/sec per connection                | 50K records → ~1s writes. Fine at this scale.                            |
| 💻 **Single-worker CPU (summarize)** | TextRank/TF-IDF on all records (event loop)     | Summaries processed one-at-a-time per activity; CPU bottleneck at scale  |

**Verdict: 1,000 URLs would work but take ~10-15 minutes instead of 2.**

### 100× Scale — 10,000 URLs

| Bottleneck                           | Limit                                                              | Failure Mode                                                                                     |
|--------------------------------------|--------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| 🔌 **DB connection pool**            | 30 connections max                                                 | 9,970 activities waiting; queue backlog grows unbounded; workflow timeout may fire               |
| 🗄️ **PostgreSQL storage**           | 10K feeds × ~50 records = 500K rows + 500K summaries ≈ 2GB        | Storage fine, but **INSERT throughput** bottleneck — 500K single-row round-trips                 |
| 💻 **Worker memory**                 | ~500KB/activity + numpy arrays ≈ 500MB-1GB at 200 concurrent      | Manageable, but single worker can't use >1 CPU for numpy ops                                     |
| ⏱️ **Temporal server**              | Single-node with embedded Postgres default config                  | 10K pending activities + 10K state transitions hit gRPC/DB limits                                |
| 🌐 **HTTP connection churn**         | 10K sockets to external hosts                                      | Docker connection tracking table fills; source port exhaustion → `EADDRNOTAVAIL`                 |

**Verdict: 10,000 URLs would likely fail or timeout on this architecture as-is.**

### How to Fix at Scale

| Bottleneck                     | Fix                                                                                                          |
|--------------------------------|--------------------------------------------------------------------------------------------------------------|
| 🔌 DB pool                     | Increase `pool_size=100`, use PgBouncer, or serverless DB (RDS Proxy)                                        |
| ⚙️ Worker concurrency          | Run multiple replicas (`docker compose scale worker=10`); each polls same task queue                         |
| 💻 Summarization CPU           | Offload to process pool or async LLM API (OpenAI, Claude, Ollama)                                            |
| 🗄️ DB writes                   | Batch INSERTs into larger chunks; use COPY for bulk loads                                                    |
| 🌐 HTTP connections             | Increase connector limit; enable keep-alive; add DNS caching; use HTTP/2 multiplexing                        |
| ⏱️ Temporal capacity           | Run Temporal in production mode (multi-node), not auto-setup single-node                                     |
| ⏳ Workflow timeout             | Use `asyncio.wait` with semaphore (max 500 concurrent) instead of `asyncio.gather` on all 10K at once        |
| 🔄 Event loop blocking          | Move CPU-bound summarization to `loop.run_in_executor(None, ...)` with a `ThreadPoolExecutor`                |

---

## Tradeoffs: What Would Change With More Time

| Current Choice                              | Why                                  | What I'd Change                                                                               |
|---------------------------------------------|--------------------------------------|-----------------------------------------------------------------------------------------------|
| `asyncio.gather` on all URLs at once        | Simplest fan-out for 100 URLs        | **Semaphore-bounded gather** — dispatch N at a time to avoid overwhelming worker/DB/Temporal  |
| Single Temporal worker process              | Simple — one container, one process  | **Multiple worker replicas** + process pool for CPU-bound summarization                       |
| Inline CPU-bound summarization in activity  | Avoids extra infra                   | **`run_in_executor` with ThreadPoolExecutor** or push to separate background service          |
| `postgresql+asyncpg` with per-activity session | Fixes concurrency contention      | **SQLAlchemy 2.0 async + async_sessionmaker** + write-through Redis cache                     |
| Docker Compose single-host                  | Easy local dev                       | **Kubernetes** for multi-worker, auto-scaling, resource limits                                |
| No progress reporting                       | Temporal `describe_workflow` only    | **WebSocket/SSE endpoint** + Redis pub/sub for real-time progress                             |
| All-or-nothing job model                    | Workflow waits for all               | **Incremental job completion** — return partial results as they arrive                        |
| feedparser + sumy for extraction            | Works for RSS, zero cost             | **LLM-based extraction+summarization** (OpenAI, Claude, Ollama)                               |
| No task deduplication                       | Same URL = duplicate work            | **URL content-addressed dedup** (hash of URL → check TTL) for recurring crawl jobs            |
| Per-request API sessions                    | `get_db_repos()` in each endpoint    | **FastAPI `Depends()`** for automatic session lifecycle management                            |

