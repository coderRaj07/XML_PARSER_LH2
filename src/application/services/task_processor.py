import gzip
import logging

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


class TaskProcessor:
    def __init__(
        self,
        fetcher: Fetcher,
        parser: ParserStrategy,
        summary_service: SummaryService,
        task_repository: TaskRepository,
        record_repository: RecordRepository,
        job_repository: JobRepository,
    ) -> None:
        self._fetcher = fetcher
        self._parser = parser
        self._summary_service = summary_service
        self._task_repository = task_repository
        self._record_repository = record_repository
        self._job_repository = job_repository

    async def process_task(self, task: Task) -> Task:
        task.increment_attempts()
        logger.info("task_started", extra={"task_id": task.id, "url": task.url})

        try:
            raw_xml = await self._fetch(task)
            records = await self._parse(task, raw_xml)
            await self._fetch_full_contents(records)
            await self._store_records(task, records)
            await self._summarize(records)
            task.mark_completed()
            logger.info("task_completed", extra={"task_id": task.id, "url": task.url})
        except Exception as e:
            task.mark_failed(str(e))
            logger.error("task_failed", extra={"task_id": task.id, "url": task.url, "error": str(e)})
            await self._task_repository.rollback()

        await self._task_repository.update(task)
        await self._update_job_progress(task)
        return task

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
        for record in records:
            if not record.source_link:
                continue
            try:
                html = await self._fetcher.fetch(record.source_link)
                extracted = trafilatura.extract(html)
                if extracted:
                    record.full_content = gzip.compress(extracted.encode("utf-8"))
                    record.content = extracted
            except Exception:
                logger.warning("full_content_fetch_failed", extra={"record_id": record.id, "url": record.source_link})

    async def _store_records(self, task: Task, records: list[Record]) -> None:
        if records:
            await self._record_repository.create_many(records)
        logger.info("records_stored", extra={"task_id": task.id, "count": len(records)})

    async def _summarize(self, records: list[Record]) -> None:
        for record in records:
            logger.info("summary_started", extra={"record_id": record.id})
            source_text = record.content or record.description or record.title
            summary_text = self._summary_service.generate_summary(source_text)
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