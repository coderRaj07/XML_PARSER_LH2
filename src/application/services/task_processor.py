import asyncio
import gzip
import logging
import time
from asyncio import CancelledError
from urllib.parse import urlparse

import trafilatura

from src.application.interfaces.fetcher import Fetcher
from src.application.interfaces.repositories import JobRepository, RecordRepository, TaskRepository
from src.application.services.summary_service import SummaryService
from src.application.strategies.parser.base_parser_strategy import ParserStrategy
from src.domain.entities.record import Record
from src.domain.entities.summary import Summary
from src.domain.entities.task import Task
from src.domain.enums.task_status import TaskStatus

logger = logging.getLogger(__name__)

_GARBAGE_PATTERNS = [
    "please enable javascript",
    "enable javascript to continue",
    "enable javascript to view",
    "javascript is required",
    "your browser does not support javascript",
    "click here if you are not redirected",
]


def _is_garbage(text: str) -> bool:
    import re as _re
    clean = _re.sub(r"<[^>]+>", "", text).strip()
    if len(clean) < 50:
        return True
    lowered = clean.lower()
    return any(p in lowered for p in _GARBAGE_PATTERNS)


class TaskProcessor:
    def __init__(
        self,
        fetcher: Fetcher,
        parser: ParserStrategy,
        summary_service: SummaryService,
        task_repository: TaskRepository,
        record_repository: RecordRepository,
        job_repository: JobRepository,
        max_articles: int = 10,
    ) -> None:
        self._fetcher = fetcher
        self._parser = parser
        self._summary_service = summary_service
        self._task_repository = task_repository
        self._record_repository = record_repository
        self._job_repository = job_repository
        self._max_articles = max_articles

    async def process_task(self, task: Task) -> Task:
        task.increment_attempts()
        logger.info("task_started", extra={"task_id": task.id, "url": task.url})

        try:
            raw_xml = await self._fetch(task)
            records = await self._parse(task, raw_xml)

            original_count = len(records)
            if len(records) > self._max_articles:
                records = records[: self._max_articles]
                logger.info(
                    "articles_truncated",
                    extra={"task_id": task.id, "original": original_count, "max": self._max_articles},
                )

            await self._fetch_full_contents(records)
            await self._store_records(task, records)
            await self._summarize(records)
            task.mark_completed()
            logger.info("task_completed", extra={"task_id": task.id, "url": task.url})
            await self._task_repository.update(task)
            await self._update_job_progress(task)
            return task
        except CancelledError:
            task.mark_failed("Activity cancelled")
            logger.info("task_cancelled", extra={"task_id": task.id, "url": task.url})
            raise
        except RuntimeError as e:
            if "Permanent failure" in str(e):
                task.mark_failed(str(e))
                logger.error("task_failed", extra={"task_id": task.id, "url": task.url, "error": str(e)})
                await self._task_repository.rollback()
                await self._task_repository.update(task)
                await self._update_job_progress(task)
                return task
            raise
        except Exception as e:
            task.mark_failed(str(e))
            logger.error("task_failed", extra={"task_id": task.id, "url": task.url, "error": str(e)})
            raise

    async def _fetch(self, task: Task) -> str:
        logger.info("fetch_started", extra={"task_id": task.id, "url": task.url})
        return await self._fetcher.fetch(task.url)

    async def _parse(self, task: Task, raw_xml: str) -> list[Record]:
        logger.info("parse_started", extra={"task_id": task.id, "url": task.url})
        records = self._parser.parse(raw_xml)
        for r in records:
            r.task_id = task.id
        return records

    async def _fetch_full_contents(self, records: list[Record]) -> None:
        semaphores: dict[str, asyncio.Semaphore] = {}
        last_request: dict[str, float] = {}
        CONCURRENT_PER_DOMAIN = 2

        async def _fetch_one(record: Record) -> None:
            if not record.source_link:
                return
            domain = urlparse(record.source_link).netloc
            if domain not in semaphores:
                semaphores[domain] = asyncio.Semaphore(CONCURRENT_PER_DOMAIN)
            async with semaphores[domain]:
                now = time.monotonic()
                since_last = now - last_request.get(domain, 0.0)
                if since_last < 1.0:
                    await asyncio.sleep(1.0 - since_last)
                try:
                    html = await self._fetcher.fetch(record.source_link)
                    extracted = trafilatura.extract(html)
                    if extracted and not _is_garbage(extracted):
                        record.full_content = gzip.compress(extracted.encode("utf-8"))
                        record.content = extracted
                    elif extracted:
                        logger.info(
                            "extracted_content_garbage_skipped",
                            extra={"record_id": record.id, "url": record.source_link, "text": extracted[:80]},
                        )
                except Exception:
                    logger.warning(
                        "full_content_fetch_failed",
                        extra={"record_id": record.id, "url": record.source_link},
                    )
                finally:
                    last_request[domain] = time.monotonic()

        coros = [_fetch_one(r) for r in records if r.source_link]
        if not coros:
            return
        results = await asyncio.gather(*coros, return_exceptions=True)
        for r in results:
            if isinstance(r, CancelledError):
                logger.info("full_content_cancelled")
                raise r

    async def _store_records(self, task: Task, records: list[Record]) -> None:
        if records:
            await self._record_repository.create_many(records)
        logger.info("records_stored", extra={"task_id": task.id, "count": len(records)})

    async def _summarize(self, records: list[Record]) -> None:
        for record in records:
            logger.info("summary_started", extra={"record_id": record.id})
            source_text = record.content or ""
            if _is_garbage(source_text):
                source_text = record.description or ""
            if _is_garbage(source_text):
                source_text = record.title or ""
            summary_text = await asyncio.to_thread(self._summary_service.generate_summary, source_text)
            summary = Summary(
                record_id=record.id,
                summary_text=summary_text,
                summary_type="extractive",
                model_used="textrank+tfidf+lsa",
            )
            await self._record_repository.save_summary(summary)
            logger.info("summary_completed", extra={"record_id": record.id})

    async def _update_job_progress(self, task: Task) -> None:
        job = await self._job_repository.get(task.job_id)
        if job is None:
            return
        pending, completed, failed = await self._task_repository.count_by_status(task.job_id)
        job.update_progress(completed, failed)
        await self._job_repository.update(job)