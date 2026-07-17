import asyncio
import gzip
import io
import logging
from concurrent.futures import ThreadPoolExecutor

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
        storage: S3Storage,
        max_articles: int = 10,
    ) -> None:
        self._session_factory = session_factory
        self._parser = parser
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
                loop = asyncio.get_running_loop()
                records = await loop.run_in_executor(None, self._parser.parse, raw_xml)
                del raw_xml
                for r in records:
                    r.task_id = task_id

                original_count = len(records)
                if len(records) > self._max_articles:
                    records = records[: self._max_articles]

                if records:
                    await record_repo.create_many(records)
                    await session.commit()

                record_infos = [
                    {"id": r.id, "source_link": r.source_link}
                    for r in records
                    if r.source_link
                ]

                logger.info(
                    "parse_completed",
                    extra={"task_id": task_id, "original_count": original_count, "saved": len(records)},
                )
                return {
                    "task_id": task_id,
                    "record_count": len(records),
                    "record_infos": record_infos,
                }
            except BaseException as e:
                if isinstance(e, (SystemExit, KeyboardInterrupt)):
                    raise
                task.mark_failed(str(e))
                await task_repo.update(task)
                await session.commit()
                logger.error("parse_failed", extra={"task_id": task_id, "error": str(e)})
                return {"task_id": task_id, "status": "failed", "error": str(e)}


class EnrichmentActivity:
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
    async def fetch_article(self, record_id: str, source_link: str, job_id: str, task_id: str) -> dict:
        logger.info("enrichment_started", extra={"record_id": record_id, "url": source_link})
        try:
            html: str | None = None
            try:
                html = await self._fetcher.fetch(source_link)
                loop = asyncio.get_running_loop()
                extracted = await loop.run_in_executor(None, trafilatura.extract, html)
                if extracted and not _is_garbage(extracted):
                    compressed = gzip.compress(extracted.encode("utf-8"))
                    content_key = self._storage._make_content_key(job_id, task_id, record_id)
                    await self._storage.store_stream(
                        content_key, io.BytesIO(compressed), "application/gzip"
                    )
                    async with self._session_factory() as session:
                        record_repo = PostgresRecordRepository(session)
                        await record_repo.update_content(record_id, extracted, content_key)
                        await session.commit()
                    logger.info("enrichment_completed", extra={"record_id": record_id})
                    return {"record_id": record_id, "status": "completed"}
                else:
                    logger.info(
                        "enrichment_skipped_garbage",
                        extra={"record_id": record_id, "url": source_link},
                    )
                    return {"record_id": record_id, "status": "skipped"}
            except Exception as e:
                logger.warning(
                    "enrichment_failed",
                    extra={"record_id": record_id, "url": source_link, "error": str(e)},
                )
                return {"record_id": record_id, "status": "failed", "error": str(e)}
            finally:
                del html
        except Exception as e:
            logger.error("enrichment_unexpected_error", extra={"record_id": record_id, "error": str(e)})
            return {"record_id": record_id, "status": "failed", "error": str(e)}


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

                sem = asyncio.Semaphore(self._max_summary_workers)

                async def _throttled_summarize(record: Record) -> Summary:
                    async with sem:
                        return await _summarize_one(record)

                results = await asyncio.gather(*[_throttled_summarize(r) for r in records], return_exceptions=True)
                summaries = [r for r in results if isinstance(r, Summary)]
                errors = [r for r in results if isinstance(r, Exception)]
                if errors:
                    logger.warning("summarize_partial_failures", extra={"task_id": task_id, "total": len(records), "failed": len(errors), "succeeded": len(summaries)})
                if not summaries:
                    raise RuntimeError(f"All {len(records)} summaries failed. First error: {errors[0] if errors else 'unknown'}")
                await record_repo.save_summaries_many(summaries)
                for summary in summaries:
                    logger.info("summary_completed", extra={"record_id": summary.record_id})

                task.mark_completed()
                await task_repo.update(task)
                await session.commit()

                async with self._session_factory() as job_session:
                    job_repo = PostgresJobRepository(job_session)
                    task_repo2 = PostgresTaskRepository(job_session)
                    job = await job_repo.get(job_id)
                    if job:
                        pending, completed, failed = await task_repo2.count_by_status(job_id)
                        job.update_progress(completed, failed)
                        await job_repo.update(job)
                        await job_session.commit()

                logger.info("summarize_completed", extra={"task_id": task_id})
                return {
                    "task_id": task.id,
                    "status": task.status.value,
                    "error": task.error,
                }
            except BaseException as e:
                if isinstance(e, (SystemExit, KeyboardInterrupt)):
                    raise
                logger.error("summarize_failed", extra={"task_id": task_id, "error": str(e)})
                task.mark_failed(str(e))
                await task_repo.update(task)
                await session.commit()

                try:
                    async with self._session_factory() as job_session:
                        job_repo = PostgresJobRepository(job_session)
                        task_repo2 = PostgresTaskRepository(job_session)
                        job = await job_repo.get(job_id)
                        if job:
                            pending, completed, failed = await task_repo2.count_by_status(job_id)
                            job.update_progress(completed, failed)
                            await job_repo.update(job)
                            await job_session.commit()
                except Exception:
                    logger.exception("summarize_job_update_after_failure", extra={"task_id": task_id})

                return {
                    "task_id": task.id,
                    "status": "failed",
                    "error": str(e),
                }


class FinalizeTaskActivity:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    @activity.defn
    async def finalize_task(self, task_id: str, job_id: str) -> dict:
        async with self._session_factory() as session:
            task_repo = PostgresTaskRepository(session)
            record_repo = PostgresRecordRepository(session)
            job_repo = PostgresJobRepository(session)

            task = await task_repo.get(task_id)
            if task is None:
                return {"task_id": task_id, "status": "not_found"}

            if task.status.value != "pending":
                return {"task_id": task_id, "status": task.status.value, "skipped": True}

            records = await record_repo.list_by_task(task_id)
            has_records = len(records) > 0
            has_summaries = any(r.summary_text for r in records)

            if has_records and has_summaries:
                task.mark_completed()
                logger.info("finalize_completed_via_summaries", extra={"task_id": task_id})
            else:
                task.mark_failed("Workflow terminated before task was finalized")
                logger.warning("finalize_failed_no_summaries", extra={"task_id": task_id, "has_records": has_records})

            await task_repo.update(task)
            await session.commit()

            try:
                async with self._session_factory() as job_session:
                    job_repo2 = PostgresJobRepository(job_session)
                    task_repo2 = PostgresTaskRepository(job_session)
                    job = await job_repo2.get(job_id)
                    if job:
                        pending, completed, failed = await task_repo2.count_by_status(job_id)
                        job.update_progress(completed, failed)
                        await job_repo2.update(job)
                        await job_session.commit()
            except Exception:
                logger.exception("finalize_job_update_failed", extra={"task_id": task_id})

            return {"task_id": task_id, "status": task.status.value}
