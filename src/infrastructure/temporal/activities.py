import asyncio
import gzip
import logging
from asyncio import CancelledError
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

import trafilatura
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from temporalio import activity

from src.application.interfaces.fetcher import Fetcher
from src.application.services.summary_service import SummaryService
from src.application.strategies.parser.base_parser_strategy import ParserStrategy
from src.domain.entities.record import Record
from src.domain.entities.summary import Summary
from src.infrastructure.repositories import (
    PostgresJobRepository,
    PostgresRecordRepository,
    PostgresTaskRepository,
)
from src.infrastructure.storage import S3Storage

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


class FetchActivity:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        fetcher: Fetcher,
        storage: S3Storage,
    ) -> None:
        self._session_factory = session_factory
        self._fetcher = fetcher
        self._storage = storage

    @activity.defn
    async def fetch_url(self, task_id: str, url: str, job_id: str) -> dict:
        logger.info("fetch_started", extra={"task_id": task_id, "url": url})
        try:
            raw_xml = await self._fetcher.fetch(url)
            storage_key = self._storage.build_key(job_id, task_id)
            await self._storage.store(storage_key, raw_xml)
            return {"task_id": task_id, "storage_key": storage_key}
        except Exception as e:
            logger.error("fetch_failed", extra={"task_id": task_id, "url": url, "error": str(e)})
            async with self._session_factory() as session:
                task_repo = PostgresTaskRepository(session)
                task = await task_repo.get(task_id)
                if task:
                    task.mark_failed(str(e))
                    await task_repo.update(task)
                    await session.commit()
            return {"task_id": task_id, "status": "failed", "error": str(e)}


class ParseActivity:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        parser: ParserStrategy,
        fetcher: Fetcher,
        storage: S3Storage,
        max_articles: int = 10,
    ) -> None:
        self._session_factory = session_factory
        self._parser = parser
        self._fetcher = fetcher
        self._storage = storage
        self._max_articles = max_articles

    @activity.defn
    async def parse_records(self, task_id: str, storage_key: str, job_id: str) -> dict:
        async with self._session_factory() as session:
            task_repo = PostgresTaskRepository(session)
            record_repo = PostgresRecordRepository(session)

            task = await task_repo.get(task_id)
            if task is None:
                return {"task_id": task_id, "status": "failed", "error": "Task not found"}

            try:
                raw_xml = await self._storage.retrieve(storage_key)
                records = self._parser.parse(raw_xml)
                for r in records:
                    r.task_id = task_id

                original_count = len(records)
                if len(records) > self._max_articles:
                    records = records[: self._max_articles]

                await self._fetch_full_contents(records)
                if records:
                    await record_repo.create_many(records)
                await session.commit()

                logger.info(
                    "parse_completed",
                    extra={"task_id": task_id, "original_count": original_count, "saved": len(records)},
                )
                return {"task_id": task_id, "record_count": len(records)}
            except Exception as e:
                task.mark_failed(str(e))
                await task_repo.update(task)
                await session.commit()
                logger.error("parse_failed", extra={"task_id": task_id, "error": str(e)})
                return {"task_id": task_id, "status": "failed", "error": str(e)}

    async def _fetch_full_contents(self, records: list[Record]) -> None:
        semaphores: dict[str, asyncio.Semaphore] = {}
        CONCURRENT_PER_DOMAIN = 10

        async def _fetch_one(record: Record) -> None:
            if not record.source_link:
                return
            domain = urlparse(record.source_link).netloc
            if domain not in semaphores:
                semaphores[domain] = asyncio.Semaphore(CONCURRENT_PER_DOMAIN)
            async with semaphores[domain]:
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

        coros = [_fetch_one(r) for r in records if r.source_link]
        if not coros:
            return
        results = await asyncio.gather(*coros, return_exceptions=True)
        for r in results:
            if isinstance(r, CancelledError):
                raise r


class SummarizeActivity:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        summary_service: SummaryService,
        max_summary_workers: int = 4,
    ) -> None:
        self._session_factory = session_factory
        self._summary_service = summary_service
        self._max_summary_workers = max_summary_workers
        self._executor = ThreadPoolExecutor(max_workers=max_summary_workers)

    @activity.defn
    async def summarize_records(self, task_id: str, job_id: str) -> dict:
        async with self._session_factory() as session:
            task_repo = PostgresTaskRepository(session)
            record_repo = PostgresRecordRepository(session)
            job_repo = PostgresJobRepository(session)

            task = await task_repo.get(task_id)
            if task is None:
                return {"task_id": task_id, "status": "failed", "error": "Task not found"}

            try:
                records = await record_repo.list_by_task(task_id)
                loop = asyncio.get_running_loop()

                async def _summarize_one(record: Record) -> Summary:
                    logger.info("summary_started", extra={"record_id": record.id})
                    source_text = record.content or ""
                    if _is_garbage(source_text):
                        source_text = record.description or ""
                    if _is_garbage(source_text):
                        source_text = record.title or ""
                    summary_text = await loop.run_in_executor(
                        self._executor, self._summary_service.generate_summary, source_text
                    )
                    return Summary(
                        record_id=record.id,
                        summary_text=summary_text,
                        summary_type="extractive",
                        model_used="textrank+tfidf+lsa",
                    )

                summaries = await asyncio.gather(*[_summarize_one(r) for r in records])
                await record_repo.save_summaries_many(summaries)
                for summary in summaries:
                    logger.info("summary_completed", extra={"record_id": summary.record_id})

                task.mark_completed()
                await task_repo.update(task)

                job = await job_repo.get(job_id)
                if job:
                    pending, completed, failed = await task_repo.count_by_status(job_id)
                    job.update_progress(completed, failed)
                    await job_repo.update(job)

                await session.commit()
                logger.info("summarize_completed", extra={"task_id": task_id})
                return {
                    "task_id": task.id,
                    "status": task.status.value,
                    "error": task.error,
                }
            except Exception as e:
                logger.error("summarize_failed", extra={"task_id": task_id, "error": str(e)})
                task.mark_failed(str(e))
                await task_repo.update(task)
                await session.commit()
                return {
                    "task_id": task.id,
                    "status": "failed",
                    "error": str(e),
                }
