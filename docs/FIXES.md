# Fixes & Issues Log

## 1. YouTube RSS Feeds Returning 500 with Valid XML

**Issue:** YouTube returns HTTP 500 for some RSS feed URLs, but the response body contains valid XML feed content. The fetcher was calling `response.raise_for_status()` which raised on 5xx, discarding the body.

**Fix:** Removed `raise_for_status()` for 5xx responses. Now logs a warning and returns the body regardless. The parser validates content downstream.

**Files:** `src/infrastructure/fetchers/aiohttp_fetcher.py:81-88`

---

## 2. YouTube Requires Sec-Fetch-* Headers

**Issue:** Some YouTube RSS feeds refused to serve content (500 error) without modern browser security headers.

**Fix:** Added `Sec-Fetch-Dest`, `Sec-Fetch-Mode`, `Sec-Fetch-Site`, `Sec-Fetch-User`, and `Upgrade-Insecure-Requests` headers to the default request headers. Also updated `Accept` and `Accept-Language` to match Chrome 134.

**Files:** `src/infrastructure/fetchers/aiohttp_fetcher.py:10-19`

---

## 3. Permanent HTTP Errors (403/404/410) Waste Retries

**Issue:** Permanent errors (404 invalid YouTube channel, 403 Cloudflare block) were handled by `raise_for_status()` which raised a `ClientResponseError` caught by `except Exception`, causing 3 wasteful retry attempts before failing.

**Fix:** For `_PERMANENT_STATUSES`, raise `RuntimeError` immediately with a clear message. Re-raise this error directly from the exception handler without retrying.

**Files:** `src/infrastructure/fetchers/aiohttp_fetcher.py:51-62, 92-94`

---

## 4. Temporal CancelledError During Full-Content Throttle Sleep

**Issue:** The 2.5s per-domain throttle in `_fetch_full_contents` caused 50-90s total sleep time for 20-article feeds. `asyncio.CancelledError` (a `BaseException`, not `Exception`) raised during sleep was not caught by `except Exception`, causing the entire task to fail even though the RSS feed was already fetched and parsed successfully. This created ~11 false failures after trafilatura was introduced.

**Fix:** 
- Reduced throttle from 2.5s to 1.0s
- Moved sleep inside the try/except block so `CancelledError` is caught and partial results are saved
- Added explicit `CancelledError` handler in `process_task` that marks task as failed and re-raises
- In `_fetch_full_contents`, `CancelledError` during sleep returns early (saving records processed so far) instead of letting it cascade

**Files:** `src/application/services/task_processor.py:80-84, 127-149`

---

## 5. Sequential Article Fetching Too Slow (20 Articles = ~60s)

**Issue:** `_fetch_full_contents` used a plain `for` loop — one article at a time, sequential. 20 articles × 1s throttle + ~1-2s fetch = ~40-60s per feed, regardless of domain diversity.

**Fix:** Rewrote to use `asyncio.gather` with per-domain `asyncio.Semaphore(2)` (matching `limit_per_host=2`). Articles from different domains run fully concurrently. Articles from the same domain run up to 2 at a time with the 1s throttle between start times.

**Before:** 20 articles on same domain → ~40-60s  
**After:** 20 articles on same domain → ~12-20s (2× faster); articles on different domains → ~1-3s total

**Files:** `src/application/services/task_processor.py:105-149`

---

## 6. Complex.com / Tripwire.com / Sony.com Bot Blocking

**Issue:** These sites returned 403/403/403 — blocked by Cloudflare WAF or bot detection.

**Fix:** 
- Added Chrome 134 browser User-Agent instead of aiohttp/bot-identifying UA
- Added `Accept` and `Accept-Language` headers
- **Result:** complex.com fixed. Tripwire.com and sony.com still blocked (Cloudflare-level, beyond UA spoofing).

**Files:** `src/infrastructure/fetchers/aiohttp_fetcher.py:10-19`

---

## 7. HuggingFace 429 Rate Limiting

**Issue:** HuggingFace blog feed returned 429 (rate limited) on repeated requests. The fetcher had no backoff for 429 responses.

**Fix:** Added 429 handler with `Retry-After` header support and exponential backoff (5s / 10s / 20s). After exhausting retries, raises a specific `ClientResponseError`.

**Files:** `src/infrastructure/fetchers/aiohttp_fetcher.py:63-80`

---

## 8. Anduril Feed with 473+ Articles

**Issue:** A single feed (anduril.com) contained 473 articles. Processing all of them took excessive time and resources.

**Fix:** Added `max_articles=20` cap. Only the first 20 articles from any feed are processed for full content and summaries.

**Files:** `src/application/services/task_processor.py:67-73`

---

## 9. "Please Enable JavaScript" Trafilatura Output

**Issue:** Trafilatura sometimes extracted "Please enable javascript to continue" as article content for sites that require JS to render.

**Fix:** Added `_is_garbage()` function that strips HTML tags and checks for known garbage patterns (enable javascript, click here if not redirected, etc.). Content detected as garbage is not stored; summary falls back to description or title.

**Files:** `src/application/services/task_processor.py:31-37`

---

## 10. HTML-Only Descriptions as Summary Source

**Issue:** Some feeds have HTML-only descriptions (e.g., `<img src="...">`) with no readable text. These passed length checks but produced empty summaries.

**Fix:** `_is_garbage()` now strips HTML tags before checking length, so `<img>`-only descriptions (0 readable chars) are detected as garbage and skipped in the summary fallback chain.

**Files:** `src/application/services/task_processor.py:31-37`

---

## 12. Large Payload Exceeds Temporal gRPC Message Limit

**Issue:** FetchActivity returns raw XML as part of its result (`{"task_id": task_id, "raw_xml": raw_xml}`), which is then passed as an argument to ParseActivity via `fetch_result["raw_xml"]`. For feeds with large RSS/XML payloads, this can exceed Temporal's default 4 MB gRPC message size limit, causing:

```text
PayloadSizeWarning
Size: 9496702 bytes
Limit: 524288 bytes

grpc: received message larger than max (9496960 vs. 4194304)
```

The activity completion itself fails — this happens before history is written, so Continue-As-New/child workflows cannot fix it.

**Root cause:** Architecture uses Temporal as a data pipe by passing raw content between activities.

**Fix:** Store `raw_xml` in PostgreSQL (e.g., a new `raw_xml` column on the tasks table) inside FetchActivity, and return only small metadata. ParseActivity reads the raw XML from the database instead of receiving it as a workflow argument.

**Before (anti-pattern):**
```
FetchActivity → returns raw_xml → workflow → passes raw_xml as arg → ParseActivity
```

**After (correct):**
```
FetchActivity → stores raw_xml in DB → returns task_id → workflow → passes task_id → ParseActivity reads raw_xml from DB
```

**Files:** `src/infrastructure/temporal/activities.py:57-71` (FetchActivity returns raw_xml), `src/infrastructure/temporal/workflows.py:87-94` (raw_xml passed as arg to ParseActivity)

---

## 11. Temporal Activity Never Dispatched

**Issue:** After worker restart, some activities in the `asyncio.gather` were never dispatched. The task stayed "pending" with 0 attempts indefinitely.

**Fix:** Manual intervention — marked the stuck task as failed in the database. Root cause is a Temporal race condition during worker restart.

**Files:** N/A (operational workaround)

---

## 12. Race Condition: Multiple Containers Calling `Base.metadata.create_all()` on Startup

**Issue:** On startup, all 5 containers (`app`, `workflow-worker`, `fetch-worker`, `parse-worker`, `summarize-worker`) simultaneously called `await Base.metadata.create_all()` inside `DatabaseSessionManager.create_tables()`. This is a classic TOCTOU (Time-of-Check to Time-of-Use) race condition: each container's connection checked "do the tables exist?", saw "no", then all tried to create them simultaneously. PostgreSQL's `pg_type_typname_nsp_index` unique constraint caught the duplicate `CREATE TYPE` and raised:
```
sqlalchemy.exc.IntegrityError: (psycopg2.errors.UniqueViolation) duplicate key value violates unique constraint "pg_type_typname_nsp_index"
DETAIL: Key (typname, typnamespace)=(..., ...) already exists.
```
This was **not** a PostgreSQL bug — it was an application-level race from multiple processes trying to create the same schema concurrently.

**Fix:**
- Deleted all old migrations (0001, 0002, 0003) and created a single baseline migration (`0000_initial_schema.py`) that creates all 4 tables in their final state
- Added an `init-db` service to `docker-compose.yml` that runs `alembic upgrade head` exactly once, then exits with status 0
- All worker services depend on `init-db` via `condition: service_completed_successfully`
- Removed `create_tables()` calls from `main.py` lifespan, `run_worker.py`, and `session.py`
- Updated `.dockerignore` to include `alembic/versions/` in Docker builds
- Fixed Dockerfile `COPY` lines so `alembic.ini` lands at `/app/alembic.ini` (not `/app/alembic/alembic.ini`)

**Before:** 5 containers race to `CREATE TABLE IF NOT EXISTS` simultaneously
**After:** 1 container runs `alembic upgrade head`, all others wait for it to finish

**Files:** `docker-compose.yml` (init-db service), `alembic/versions/0000_initial_schema.py`, `.dockerignore`, `Dockerfile`, `src/main.py`, `src/infrastructure/temporal/run_worker.py`, `src/infrastructure/db/session.py`

---

## 13. Parse Activity Doing Too Much (XML Parsing + Enrichment + S3 Upload in One Activity)

**Issue:** The `parse_records` activity was responsible for: (1) reading XML from S3, (2) parsing with feedparser, (3) fetching full article content for every record via trafilatura, (4) uploading to S3, (5) saving records to DB. For feeds with 10-20 articles, this single activity ran for 30-60+ seconds. Combined with the `CHILD_WORKFLOW_TIMEOUT` of 45 seconds (derived from `ACTIVITY_TIMEOUT=15s × 3 retries × 3 activities = 135s`), activities would be killed before completing, leaving records in an inconsistent state. Additionally, a failure in any one article's enrichment would fail the entire batch.

**Fix:**
- **Separated parsing from enrichment**: `parse_records` now only parses XML and saves record metadata (title, author, source_link, description). It returns `record_infos` — a list of `{id, source_link}` dicts.
- **New `EnrichmentActivity.fetch_article`**: Per-record activity that fetches the article URL via trafilatura, uploads content to S3, and updates the record in the DB. Each article is independent with its own error handling.
- **New `EnrichmentWorkflow`**: Child workflow spawned by `UrlWorkflow` that runs parallel `fetch_article` activities (one per record) via `asyncio.gather`.
- **Updated `UrlWorkflow`**: Chains: fetch → parse → EnrichmentWorkflow (child) → summarize.
- **New `enrichment-worker` service**: Polls `xml-feed-enrichment-queue`, handles both `EnrichmentWorkflow` and `fetch_article` activity.
- **Updated timeouts**: `ACTIVITY_TIMEOUT_SECONDS` default increased from 15 to 60. `CHILD_WORKFLOW_TIMEOUT` derived as `60 × 1 × 3 = 180s`.

**Before:**
```
UrlWorkflow:
  1. fetch_url → fetch-queue
  2. parse_records → parse-queue  (30-60s: XML + enrichment + S3 + DB)
  3. summarize_records → summarize-queue
```

**After:**
```
UrlWorkflow:
  1. fetch_url → fetch-queue          (~2-5s)
  2. parse_records → parse-queue      (~2-3s: XML parsing + DB metadata only)
  3. EnrichmentWorkflow (child) → enrichment-queue
       ├── fetch_article(record_1)    (~2-5s, parallel)
       ├── fetch_article(record_2)    (~2-5s, parallel)
       └── ... (one per record)
  4. summarize_records → summarize-queue  (~2-5s)
```

**Files:** `src/infrastructure/temporal/activities.py`, `src/infrastructure/temporal/workflows.py`, `src/infrastructure/temporal/worker.py`, `src/infrastructure/temporal/config.py`, `src/infrastructure/repositories/postgres_record_repository.py`, `src/application/interfaces/repositories.py`, `docker-compose.yml`
