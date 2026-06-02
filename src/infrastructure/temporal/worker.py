import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from temporalio.client import Client
from temporalio.worker import Worker

from src.application.interfaces.fetcher import Fetcher
from src.application.services.summary_service import SummaryService
from src.application.strategies.parser.base_parser_strategy import ParserStrategy
from src.infrastructure.temporal.activities import URLProcessingActivity
from src.infrastructure.temporal.workflows import JobWorkflow

logger = logging.getLogger(__name__)


async def run_worker(
    temporal_host: str,
    task_queue: str,
    session_factory: async_sessionmaker[AsyncSession],
    fetcher: Fetcher,
    parser: ParserStrategy,
    summary_service: SummaryService,
) -> None:
    client = await Client.connect(temporal_host)

    activity = URLProcessingActivity(
        session_factory=session_factory,
        fetcher=fetcher,
        parser=parser,
        summary_service=summary_service,
    )

    worker = Worker(
        client=client,
        task_queue=task_queue,
        workflows=[JobWorkflow],
        activities=[activity.process_url],
    )

    logger.info("Temporal worker started", extra={"task_queue": task_queue})
    await worker.run()