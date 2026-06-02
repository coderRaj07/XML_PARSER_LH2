import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from temporalio import activity

from src.application.interfaces.fetcher import Fetcher
from src.application.services.summary_service import SummaryService
from src.application.services.task_processor import TaskProcessor
from src.application.strategies.parser.base_parser_strategy import ParserStrategy
from src.domain.entities.task import Task
from src.domain.enums.task_status import TaskStatus
from src.infrastructure.repositories import (
    PostgresJobRepository,
    PostgresRecordRepository,
    PostgresTaskRepository,
)

logger = logging.getLogger(__name__)


class URLProcessingActivity:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        fetcher: Fetcher,
        parser: ParserStrategy,
        summary_service: SummaryService,
    ) -> None:
        self._session_factory = session_factory
        self._fetcher = fetcher
        self._parser = parser
        self._summary_service = summary_service

    @activity.defn
    async def process_url(self, task_id: str, url: str, job_id: str) -> dict:
        session = self._session_factory()
        task_repo = PostgresTaskRepository(session)
        record_repo = PostgresRecordRepository(session)
        job_repo = PostgresJobRepository(session)

        task_processor = TaskProcessor(
            fetcher=self._fetcher,
            parser=self._parser,
            summary_service=self._summary_service,
            task_repository=task_repo,
            record_repository=record_repo,
            job_repository=job_repo,
        )

        task = Task(id=task_id, job_id=job_id, url=url, status=TaskStatus.PENDING)
        try:
            updated_task = await task_processor.process_task(task)
            await job_repo.commit()
        except Exception:
            await job_repo.close()
            raise

        return {
            "task_id": updated_task.id,
            "status": updated_task.status.value,
            "error": updated_task.error,
        }