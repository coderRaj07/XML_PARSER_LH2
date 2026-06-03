# XML Feed Processing & Summarization System

Production-grade backend for ingesting RSS/XML feeds, extracting records, and generating summaries. Built with **FastAPI + Temporal + PostgreSQL** following Clean Architecture, SOLID, and Domain-Driven Design.

## Quick Start

### Prerequisites

- Docker & Docker Compose (recommended)
- Python 3.12+ (for local development)

### Run with Docker (recommended)

```bash
# Build and start all services in detached mode
docker compose up --build -d

# Follow worker logs to monitor processing
docker compose logs -f worker

# API available at 
http://localhost:8000

# Swagger UI at 
http://localhost:8000/docs

# Temporal Dev UI at 
http://localhost:8233

# Submit a test job — open Swagger UI at http://localhost:8000/docs,
# paste the payload below into the POST /jobs endpoint, and hit Execute

# Check job status:
curl http://localhost:8000/jobs/{job_id}

# View processed records with summaries:
curl "http://localhost:8000/jobs/{job_id}/records?limit=5"
```

### Run locally

```bash
# 1. Create virtual environment
python3 -m venv venv
source venv/bin/activate

# 2. Install dependencies
pip install -e ".[dev]"

# 3. Start PostgreSQL and Temporal
docker compose up postgres temporal -d

# 4. Start the application
uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload

# 5. Start the Temporal Worker (separate terminal)
python -m src.infrastructure.temporal.run_worker
```

### Run tests

```bash
pytest tests/ -v
```

## API

### Create a Job

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"urls": ["https://example.com/feed.xml", "https://other.com/rss"]}'
```

Response: `{"job_id": "..."}`

### Get Job Status

```bash
curl http://localhost:8000/jobs/{job_id}
```

Response: `{"status": "running", "total": 100, "completed": 50, "failed": 3}`

### List Job Tasks

```bash
curl http://localhost:8000/jobs/{job_id}/tasks
```

### Health Check

```bash
curl http://localhost:8000/health
```



---

## Design Decisions

### Why This Architecture?

The system is built around **Temporal + asyncio** for concurrency rather than Celery, task queues (Redis/RabbitMQ/SQS), or raw asyncio:

| Approach                               | Pros                                                                     | Cons                                                                           | Chosen?   |
|----------------------------------------|--------------------------------------------------------------------------|--------------------------------------------------------------------------------|-----------|
| **Temporal + asyncio**                 | Durable execution, built-in retry+backoff, fan-out via `asyncio.gather`  | Heavy dependency (server+DB), learning curve, deterministic workflow code     | **✅ Yes** |
| Raw asyncio + `asyncio.gather`         | Simple, zero deps                                                        | No durability, no retry, no state visibility                                  | ❌ No     |
| Celery + Redis/RabbitMQ                | Mature, well-known                                                       | Manual retry, no workflow state mgmt, no built-in fan-out                     | ❌ No     |
| Thread pool + asyncio                  | Simple for CPU-bound work                                                | Higher overhead for I/O-bound work, GIL contention                            | ❌ No     |

### Concurrency Model

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
    ├──► Activity 1: fetch RSS → parse → fetch_full_contents* → store → summarize → commit
    ├──► Activity 2: fetch RSS → parse → fetch_full_contents* → store → summarize → commit
    └──► Activity N: ...
         * Article content fetching is concurrent within each activity:
           asyncio.gather with per-domain Semaphore(2) + 1s throttle
```

- **Feed-level**: all URLs dispatched concurrently via Temporal `asyncio.gather`
- **Article-level**: within a feed, article content fetching runs concurrently via `asyncio.gather` with per-domain `Semaphore(2)` — different domains fully parallel, same domain up to 2 at a time with 1s throttle
- **Per-activity DB sessions** — each activity creates a fresh `AsyncSession` from the factory
- **Shared aiohttp session** — long-lived `ClientSession` reused across all activities
- **No thread pool** — all I/O is asyncio-based; CPU-bound summarization runs inline (blocks event loop)
- **Worker dispatch** — max ~200 concurrent activities; beyond that Temporal queues server-side

### Failure Analysis

**Final result: 61 permanently fail.**

| Reason | Count | HTTP |
|--------|-------|------|
| Invalid/deleted YouTube channels | 59 | 404 |
| Cloudflare WAF (tripwire.com, sony.com) | 2 | 403 |
| **Total** | **61** | |

YouTube feeds tested: **62 total** — **3 passed**, **59 failed** (all HTTP 404 — invalid/deleted channel IDs).

The 61 failures are all HTTP 404 (invalid/deleted YouTube channel IDs) or HTTP 403 (Cloudflare WAF blocking) — no amount of header tweaking or retries can fix these.

#### 10× Scale — 1,000 URLs

| Bottleneck                         | Limit                                          | Impact                                                                 |
|------------------------------------|------------------------------------------------|------------------------------------------------------------------------|
| 🔌 DB connection pool              | `pool_size=10, max_overflow=20` → 30 conns    | 970 activities block; throughput drops to ~30/s                        |
| 🌐 aiohttp connector limit         | `TCPConnector(limit=100)`                     | HTTP requests queue up — latency, no failures                          |
| ⚙️ Temporal worker concurrency     | ~200 concurrent activities                     | Beyond 200, Temporal queues server-side                                |
| 🗄️ PostgreSQL write throughput     | ~5K-20K writes/sec                            | 50K records → ~1s. Fine.                                               |
| 💻 Single-worker CPU (summarize)   | TextRank/TF-IDF on event loop                 | Summaries one-at-a-time; CPU bottleneck at scale                       |

**Verdict: 1,000 URLs would work but take ~10-15 min instead of 2.**

#### 100× Scale — 10,000 URLs

| Bottleneck                         | Limit                                            | Failure Mode                                                           |
|------------------------------------|--------------------------------------------------|------------------------------------------------------------------------|
| 🔌 DB connection pool              | 30 connections max                               | 9,970 waiting; backlog unbounded; workflow may timeout                |
| 🗄️ PostgreSQL storage              | 500K rows + 500K summaries ≈ 2GB                | Storage fine, but INSERT throughput bottleneck                         |
| 💻 Worker memory                   | ~500KB/activity + numpy ≈ 500MB-1GB             | Manageable, but single worker can't use >1 CPU for numpy              |
| ⏱️ Temporal server                 | Single-node embedded Postgres                   | 10K pending activities hit gRPC/DB limits                              |
| 🌐 HTTP connection churn           | 10K sockets to external hosts                   | Docker connection tracking fills; source port exhaustion              |

**Verdict: 10,000 URLs would likely fail or timeout as-is.**

### How to Fix at Scale

| Bottleneck                   | Fix                                                                                                    |
|------------------------------|--------------------------------------------------------------------------------------------------------|
| 🔌 DB pool                   | Increase `pool_size=100`, PgBouncer, or RDS Proxy                                                      |
| ⚙️ Worker concurrency        | `docker compose scale worker=10` — each polls same task queue                                          |
| 💻 Summarization CPU         | Offload to process pool or async LLM API (OpenAI, Claude, Ollama)                                      |
| 🗄️ DB writes                | Batch INSERTs; use COPY for bulk loads                                                                 |
| 🌐 HTTP connections          | Increase connector limit; keep-alive; DNS cache; HTTP/2 multiplexing                                   |
| ⏱️ Temporal capacity         | Multi-node production mode, not auto-setup single-node                                                 |
| ⏳ Workflow timeout          | Semaphore-bounded `asyncio.wait` instead of `gather` on all 10K at once                                |
| 🔄 Event loop blocking       | `run_in_executor` with ThreadPoolExecutor for CPU-bound work                                           |

### Tradeoffs

| Current Choice                            | Why                                | What I'd Change                                                                             |
|-------------------------------------------|------------------------------------|---------------------------------------------------------------------------------------------|
| `asyncio.gather` on all URLs at once      | Simplest for 100 URLs              | **Semaphore-bounded gather** — dispatch N at a time                                         |
| Single Temporal worker                    | Simple container                   | **Multiple replicas** + process pool for CPU-bound work                                     |
| Inline CPU-bound summarization in activity| Avoids extra infra                 | **`run_in_executor`** or push to separate background service                                |
| `postgresql+asyncpg` per-activity session | Fixes concurrency                  | **SQLAlchemy 2.0 async** + write-through Redis cache                                        |
| Docker Compose single-host                | Easy local dev                     | **Kubernetes** for multi-worker, auto-scaling                                               |
| No progress reporting                     | `describe_workflow` only           | **WebSocket/SSE** + Redis pub/sub for real-time progress                                    |
| All-or-nothing job model                  | Workflow waits for all             | **Incremental completion** — return partial results as they arrive                          |
| feedparser + sumy                         | Works, zero cost                   | **LLM-based extraction+summarization** (OpenAI, Claude, Ollama)                             |
| No task deduplication                     | Same URL = duplicate work          | **URL content-addressed dedup** (hash → TTL check)                                          |
| Per-request API sessions                  | `get_db_repos()` per endpoint      | **FastAPI `Depends()`** for automatic session lifecycle                                     |

## Future Extensions

The codebase is designed for easy extension. Here's how to add new implementations:

### Adding a new Parser Strategy

1. Create a new file in `src/application/strategies/parser/` that implements `ParserStrategy`
2. Add the class to `src/application/strategies/parser/__init__.py`
3. Swap the implementation in `src/main.py` `build_container()` function

```python
from src.application.strategies.parser import AtomParser

parser: ParserStrategy = AtomParser()  # instead of RSSParser
```

### Adding a new Summary Strategy

1. Create a new file in `src/application/strategies/summary/` that implements `SummaryStrategy`
2. Add the class to `src/application/strategies/summary/__init__.py`
3. Swap the implementation in `src/main.py` `build_container()` function

```python
from src.application.strategies.summary import LLMSummaryStrategy

summary_strategy: SummaryStrategy = LLMSummaryStrategy()  # instead of ExtractiveSummaryStrategy
```

### Adding a new Execution Engine

1. Create a new file in `src/infrastructure/` that implements `ExecutionEngine`
2. Swap the implementation in `src/main.py` `build_container()` function



---

## Test Payload (101 URLs)

Copy this into Swagger UI at `http://localhost:8000/docs`:

```json
{
  "urls": [
    "https://1password.com/blog/index.xml",
    "https://huggingface.co/blog/feed.xml",
    "https://in.ign.com/feed.xml",
    "https://mediagazer.com/feed.xml",
    "https://techmeme.com/feed.xml",
    "https://thehockeynews.com/feed/THNHOME/full.xml",
    "https://www.3djuegos.com/feedburner.xml",
    "https://www.androidcentral.com/feeds.xml",
    "https://www.anduril.com/feed.xml",
    "https://www.arkansasairandmilitary.com/blog-feed.xml",
    "https://www.babycenter.com/bc-latest-content.xml?discover",
    "https://www.buzzfeed.com/in/index.xml",
    "https://www.cinemablend.com/feeds.xml",
    "https://www.complex.com/index.xml",
    "https://www.digitalcameraworld.com/feeds.xml",
    "https://www.gamesradar.com/feeds.xml",
    "https://www.gfinityesports.com/feed.xml",
    "https://www.globalsecurity.org/globalsecurity-org.xml",
    "https://www.homesandgardens.com/feeds.xml",
    "https://www.hospitalitynet.org/news/global.xml",
    "https://www.laptopmag.com/feeds.xml",
    "https://www.livingetc.com/feeds.xml",
    "https://www.malwarebytes.com/blog/feed/index.xml",
    "https://www.netlify.com/changelog/feed.xml",
    "https://www.nitinguptadfw.com/blog-feed.xml",
    "https://www.pcgamer.com/feeds.xml",
    "https://www.sony.com/en/SonyInfo/News/Press/data/pressrelease_for_top.xml",
    "https://www.spacewar.com/missiledefense.xml",
    "https://www.t3.com/feeds.xml",
    "https://www.techradar.com/feeds.xml",
    "https://www.thebroadwaybeat.com/blog-feed.xml",
    "https://www.tomsguide.com/feeds.xml",
    "https://www.tomshardware.com/feeds.xml",
    "https://www.tripwire.com/blogs.xml",
    "https://www.wallpaper.com/feeds.xml",
    "https://www.whathifi.com/feeds.xml",
    "https://www.whowhatwear.com/feeds.xml",
    "https://www.windowscentral.com/feeds.xml",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC026Q-5nFJh-sK6Pmyy4dCw",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC0a_pO439rhcyHBZq3AKdrw",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC0BUoEkrQKuifZdb-1lub8A",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC0jNg4hZbUqMr7sD0mece9g",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC0LrZO9wORIqn_aRJtKdgfA",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC0MKBRS7teISJ5iGufoc_Iw",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC0mpXp7Gcy2GmX6exBva70g",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC0noL-MX_kMr81iHXU2LSxw",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC0nyU2GAhdM1dbgbWwBerBw",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC0QOM5-oQ2wCqPstBdCAyrw",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC0wVQ6StHvTc9sTY-Z44LkQ",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC0Wwu7r1ybaaR09ANhudTzA",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC0yD_dcTxITdZXyw1vW7pIg",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC11MjaCTJzTdTYhePzxNdRQ",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC11Ven0Ko54NQbqMeirUpfg",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC15bpV1OhNXggDe9A5nQLTA",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC15ThpbeQv6NkgYrup61SfA",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC160Ln3rSb3Ym2iUxJasZVw",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC16-FGJ4ceYvt7W4kRfTFyA",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC1BdpFCsvKmqzetThZO9M6A",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC1CEo3UAzB39fj1F2jf6tAQ",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC1DGpYiEiqBrQtYXFbLhMVQ",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC1es5fp8FEK1L0EgHjCvmtQ",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC-1K8JAyndlyWACgndosXCw",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC1lgJkpCx_0SMzsvrTCdxPw",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC1MIypF1CqevUorqhILzB6g",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC1Mx92GFTNBMn-rs7VmDCgg",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC1oigGPZ4A6atKCnj17ow8Q",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC1s9576cQFdQq3QTtTxocmA",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC1s9bDwUFnpLsNy4o1QX0iw",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC_1TBgZ5FuGSdRlrrnyJU7w",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC1-ufz1Py29sLaTvXN4lGYg",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC1w6pDb9Bgu5WeFI3pJ5iAg",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC27JCqpAz5DZHS5uYiCMfOw",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC2a0ENbCZqIO5C1fWXGXZXA",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC2DWnHy2ne106gSUElEFPzQ",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC2eEGT06FrWFU6VBnPOR9lg",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC2kIvLgdf-s1uEwzq6bNdzw",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC2PcJcBiF6U7k6PmvGYushQ",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC2pmfLm7iq6Ov1UwYrWYkZA",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC2PryLg7hc4AAKSdbNnHxLw",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC2Qw1dzXDBAZPwS7zm37g8g",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC2Stn8atEra7SMdPWyQoSLA",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC2UxiAccH3jI5ZTPQW0tnFw",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC2wKfjlioOCLP4xQMOWNcgg",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC2WTyXjB87E6Xk98NQA6Fow",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC2YKXCQ8wt1RFpQEELwILCQ",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC38BDK3SVRHB6hmBY-7fU6Q",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC3dW2nkuO82xKUjpCG3hoBQ",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC3eolSinYbBNtDGuHWanlHw",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC3FeUnlHLdpKG7OBV1z1piQ",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC3HPbvB6f58X_7SMIp6OPYw",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC3I2GFN_F8WudD_2jUZbojA",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC3ifTl5zKiCAhHIBQYcaTeg",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC3L9XPe0_FGfRG-CMGtBvFg",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC3mWejHzgH1cq_Lwcnp-0Og",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC3nPaf5MeeDTHA2JN7clidg",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC3osNjJeuDdvyALIEP-nh0g",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC3qbvcgOHXRIFIofXyd1vBw",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC3R-xanNgtoa8b7gpVexVlA",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC3SUr9duoxSOxou6VlLcf7Q",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC3vvdOioadhn-ZNjOa-tqg",
    "https://www.navair.navy.mil/navair-news.xml"
  ]
}
```

## Learn More

| Document | Description |
|----------|-------------|
| [`ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Architecture diagrams, concurrency model, design decisions, scaling analysis, tradeoffs |
| [`FIXES.md`](docs/FIXES.md) | Complete log of all 11 issues faced during development and how they were resolved |