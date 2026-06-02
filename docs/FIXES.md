# Issues Faced & How They Were Fixed

## 1. Task ID Mismatch Between API and Worker

**Symptom:** `ValueError: Task {id} not found` — worker tried to `UPDATE tasks WHERE id = '{job_id}-{idx}'` but tasks were stored with UUID primary keys.

**Root Cause:** `JobService.create_job()` created tasks in the DB with auto-generated UUID IDs via `Task(id=field(default_factory=lambda: str(uuid4())))`. Meanwhile, the Temporal workflow generated synthetic IDs as `f"{job_id}-{idx}"`. The activity then created an in-memory `Task(id=synthetic_id)` and called `task_repository.update()`, which did a `SELECT` by that ID — which never matched.

**Fix:** Changed `ExecutionEngine.start_job()` to accept `list[tuple[str, str]]` of `(task_id, url)` pairs instead of just `list[str]` of URLs. `JobService.create_job()` now passes `[(task.id, task.url) for task in tasks]` to the engine. The workflow iterates over real `(task_id, url)` pairs instead of generating synthetic IDs.

**Files changed:**
- `src/application/interfaces/execution_engine.py` — signature change
- `src/infrastructure/temporal/temporal_engine.py` — pass task data
- `src/infrastructure/temporal/workflows.py` — use `tasks: list[tuple[str, str]]`
- `src/application/services/job_service.py` — pass real task IDs

---

## 2. Concurrent DB Session Contention

**Symptom:** `InterfaceError: cannot perform operation: another operation is in progress` — multiple Temporal activities running concurrently on the same SQLAlchemy session/asyncpg connection.

**Root Cause:** The worker created a single session and shared it across all `URLProcessingActivity` invocations. When `asyncio.gather` fanned out 101 concurrent activities, they all called `session.execute()` on the same asyncpg connection simultaneously.

**Fix:** Moved session creation inside each activity invocation. `URLProcessingActivity` now accepts a `session_factory` (an `async_sessionmaker`) and creates a fresh `AsyncSession` (and fresh repo instances) inside each `process_url()` call. Each activity gets its own connection from the pool.

**Files changed:**
- `src/infrastructure/temporal/activities.py` — per-call session/repo creation
- `src/infrastructure/temporal/worker.py` — accept `session_factory` instead of pre-built repos
- `src/infrastructure/temporal/run_worker.py` — pass `session_factory`

---

## 3. AioHttp ClientSession Closed Early

**Symptom:** `Fetch attempt failed: Session is closed` — concurrent HTTP fetches failed after the first request succeeded.

**Root Cause:** `AioHttpFetcher.fetch()` created a new `aiohttp.ClientSession` inside each call using `async with` (context manager) but shared a single `TCPConnector` across all calls. When the first `ClientSession` exited its `async with` block, it closed itself **and** the shared connector, breaking all subsequent sessions.

**Fix:** Moved `ClientSession` creation to `__init__` as a long-lived instance attribute instead of creating a new one per `fetch()` call. The session persists for the lifetime of the fetcher and is properly closed via an explicit `close()` method.

**Files changed:**
- `src/infrastructure/fetchers/aiohttp_fetcher.py` — long-lived `ClientSession`

---

## 4. Timezone-Aware Datetime in TIMESTAMP WITHOUT TIME ZONE Column

**Symptom:** `DataError: invalid input for query argument $5 ... can't subtract offset-naive and offset-aware datetimes` — inserting records into PostgreSQL failed.

**Root Cause:** The RSS parser used `dateutil.parser.parse()` on feed dates, which returns timezone-aware `datetime` objects (e.g., with `tzutc()`). The database column was `TIMESTAMP WITHOUT TIME ZONE`, and SQLAlchemy/asyncpg raised an error when trying to bind a tz-aware value to a tz-naive column.

**Fix:** Added UTC normalization in `RSSParser._parse_date()` — after parsing, if the datetime has a `tzinfo`, it is converted to UTC via `.astimezone(timezone.utc)` and then stripped of tzinfo via `.replace(tzinfo=None)`.

**Files changed:**
- `src/application/strategies/parser/rss_parser.py` — naive UTC conversion

---

## 5. PendingRollbackError After DB Failure

**Symptom:** After a DB error (e.g., the timezone issue above), all subsequent operations on the same session failed with `PendingRollbackError: This Session's transaction has been rolled back due to a previous exception during flush.`

**Root Cause:** When `_store_records()` or `_summarize()` raised a DB exception, `TaskProcessor.process_task()` caught it, marked the task as failed, and called `task_repository.update()`. But the session was in a broken state — SQLAlchemy requires an explicit `rollback()` before the session can be reused.

**Fix:**
1. Added `rollback()` method to all repository interfaces and PostgreSQL implementations.
2. In `process_task()`, call `await self._task_repository.rollback()` in the `except` block before attempting the failed-status update.

**Files changed:**
- `src/application/interfaces/repositories.py` — added `rollback()` to all three interfaces
- `src/infrastructure/repositories/*.py` — implemented `rollback()` in all three repos
- `src/application/services/task_processor.py` — call rollback on failure

---

## 6. HTML Content in Summaries

**Symptom:** Summaries contained raw HTML (`<img>`, `<a>`, `<p>` tags) instead of clean text.

**Root Cause:** RSS feed content is typically HTML. The `ExtractiveSummaryStrategy` fed raw HTML directly to `sumy`'s `PlaintextParser` and `TfidfVectorizer`, which couldn't parse it into meaningful sentences. All three algorithms (TextRank, TF-IDF, LSA) silently failed, falling back to `" ".join(sentences[:5])` — which returned raw HTML because the sentence splitter couldn't split HTML on `[.!?]`.

**Fix:** Added `_strip_html()` method that:
1. Removes HTML tags with `re.sub(r"<[^>]+>", " ", text)`
2. Unescapes HTML entities via `html.unescape()`
3. Strips URLs with `re.sub(r"https?://\S+", "", text)`
4. Normalizes whitespace

This runs before any summarization algorithm, so TextRank/TF-IDF/LSA receive clean plain text.

**Files changed:**
- `src/application/strategies/summary/extractive_summary_strategy.py` — added `_strip_html()`

---

## 7. HTML in API Response Fields

**Symptom:** API endpoints returned raw HTML in `description`, `content`, and `summary` fields.

**Fix:** Added `_clean_html()` helper in `record_controller.py` that strips HTML tags, unescapes entities, removes URLs, and normalizes whitespace from all text fields before returning them in API responses.

**Files changed:**
- `src/api/record_controller.py` — added `_clean_html()` and applied it to all text fields

---

## 8. NameError After Parameter Rename

**Symptom:** `NameError: name 'urls' is not defined` in workflow execution.

**Root Cause:** When renaming the workflow parameter from `urls` to `tasks`, the `return` statement at the bottom still referenced `len(urls)` instead of `len(tasks)`.

**Fix:** Updated the return dict to use `len(tasks)`.

**Files changed:**
- `src/infrastructure/temporal/workflows.py` — fixed variable name in return

---

## 9. DI Container: JobService Not Registered

**Symptom:** `KeyError` when `job_controller.py` tried to look up `JobService` from the DI container.

**Root Cause:** The original code registered the `JobService` under the concrete class but the controller looked it up by the abstract type `JobService` (or vice versa). The container used a simple dict keyed by type, and the registration/lookup type mismatch caused a miss.

**Fix:** Changed `job_controller.py` to call `main.get_job_service()` directly instead of using the DI container lookup, since `JobService` requires per-request session wiring.

**Files changed:**
- `src/api/job_controller.py` — direct factory call instead of container lookup

---

## 10. Session Closed Before Use

**Symptom:** Queries failed because the database session was closed before the service could use it.

**Root Cause:** `get_job_service()` in `main.py` used `async with session_factory() as session`, which closed the session when the `async with` block exited — before the caller could execute any queries.

**Fix:** Replaced `async with` with a direct `session = session_factory()` call, returning the session explicitly for the caller to manage (commit/close in the endpoint's try/finally).

**Files changed:**
- `src/main.py` — removed `async with`, return session directly
