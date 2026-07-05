import logging
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from pydantic_settings import BaseSettings

from src.api.job_controller import router as job_router
from src.api.record_controller import router as record_router
from src.application.interfaces.fetcher import Fetcher
from src.application.interfaces.repositories import JobRepository, RecordRepository, TaskRepository
from src.application.services.job_service import JobService
from src.application.services.scheduler import Scheduler
from src.application.services.summary_service import SummaryService
from src.application.strategies.parser import RSSParser
from src.application.strategies.parser.base_parser_strategy import ParserStrategy
from src.application.strategies.summary import ExtractiveSummaryStrategy
from src.application.strategies.summary.base_summary_strategy import SummaryStrategy
from src.infrastructure.db import DatabaseSessionManager
from src.infrastructure.fetchers import AioHttpFetcher
from src.infrastructure.repositories import (
    PostgresJobRepository,
    PostgresRecordRepository,
    PostgresTaskRepository,
)
from src.infrastructure.temporal import TemporalEngine

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/xml_feeds"
    temporal_host: str = "localhost:7233"
    log_level: str = "INFO"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format='{"time": "%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "message": "%(message)s"}',
    stream=sys.stdout,
)


_container: dict[type, Any] = {}


def register_service(interface: type, implementation: Any) -> None:
    _container[interface] = implementation


def get_service(interface: type) -> Any:
    return _container[interface]


def build_container() -> None:
    DatabaseSessionManager.initialize(settings.database_url)

    fetcher: Fetcher = AioHttpFetcher()
    parser: ParserStrategy = RSSParser()
    summary_strategy: SummaryStrategy = ExtractiveSummaryStrategy()
    execution_engine = TemporalEngine(temporal_host=settings.temporal_host)

    register_service(Fetcher, fetcher)
    register_service(ParserStrategy, parser)
    register_service(SummaryStrategy, summary_strategy)
    register_service(TemporalEngine, execution_engine)

    # Repositories need sessions; they are created per-request, not in container.
    # The container stores factories or the services that use them.

    scheduler = Scheduler()
    summary_service = SummaryService(strategy=summary_strategy)

    register_service(Scheduler, scheduler)
    register_service(SummaryService, summary_service)


@asynccontextmanager
async def get_db_repos() -> AsyncGenerator[tuple[RecordRepository, JobRepository], None]:
    factory = DatabaseSessionManager.get_session_factory()
    async with factory() as session:
        record_repo: RecordRepository = PostgresRecordRepository(session)
        job_repo: JobRepository = PostgresJobRepository(session)
        yield record_repo, job_repo


@asynccontextmanager
async def get_job_service() -> AsyncGenerator[tuple[JobService, JobRepository, TaskRepository], None]:
    factory = DatabaseSessionManager.get_session_factory()
    async with factory() as session:
        job_repo: JobRepository = PostgresJobRepository(session)
        task_repo: TaskRepository = PostgresTaskRepository(session)
        record_repo: RecordRepository = PostgresRecordRepository(session)
        scheduler = get_service(Scheduler)
        execution_engine = get_service(TemporalEngine)
        service = JobService(
            job_repository=job_repo,
            task_repository=task_repo,
            scheduler=scheduler,
            execution_engine=execution_engine,
        )
        yield service, job_repo, task_repo


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    build_container()
    await DatabaseSessionManager.create_tables()
    yield
    await DatabaseSessionManager.dispose()


app = FastAPI(title="XML Feed Summarizer", version="0.1.0", lifespan=lifespan)
app.include_router(job_router)
app.include_router(record_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}