import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from temporalio.client import Client
from temporalio.worker import Worker

from src.application.interfaces.fetcher import Fetcher
from src.application.services.summary_service import SummaryService
from src.application.strategies.parser.base_parser_strategy import ParserStrategy
from src.infrastructure.temporal.activities import FetchActivity, ParseActivity, SummarizeActivity
from src.infrastructure.temporal.config import (
    FETCH_WORKER_COUNT,
    PARSE_WORKER_COUNT,
    SUMMARIZE_WORKER_COUNT,
)
from src.infrastructure.temporal.workflows import (
    FETCH_QUEUE,
    PARSE_QUEUE,
    SUMMARIZE_QUEUE,
    WORKFLOW_QUEUE,
    JobWorkflow,
)

logger = logging.getLogger(__name__)


async def run_workers(
    temporal_host: str,
    session_factory: async_sessionmaker[AsyncSession],
    fetcher: Fetcher,
    parser: ParserStrategy,
    summary_service: SummaryService,
) -> None:
    client = await Client.connect(temporal_host)

    fetch_activity = FetchActivity(
        session_factory=session_factory,
        fetcher=fetcher,
    )
    parse_activity = ParseActivity(
        session_factory=session_factory,
        parser=parser,
        fetcher=fetcher,
    )
    summarize_activity = SummarizeActivity(
        session_factory=session_factory,
        summary_service=summary_service,
    )

    worker_tasks: list[asyncio.Task[None]] = []

    workflow_worker = Worker(
        client=client,
        task_queue=WORKFLOW_QUEUE,
        workflows=[JobWorkflow],
    )
    worker_tasks.append(asyncio.create_task(workflow_worker.run()))
    logger.info("Workflow worker created", extra={"task_queue": WORKFLOW_QUEUE})

    w = Worker(
        client=client,
        task_queue=FETCH_QUEUE,
        activities=[fetch_activity.fetch_url],
        max_concurrent_activities=FETCH_WORKER_COUNT,
    )
    worker_tasks.append(asyncio.create_task(w.run()))
    logger.info("Fetch worker created", extra={"max_concurrent": FETCH_WORKER_COUNT, "task_queue": FETCH_QUEUE})

    w = Worker(
        client=client,
        task_queue=PARSE_QUEUE,
        activities=[parse_activity.parse_records],
        max_concurrent_activities=PARSE_WORKER_COUNT,
    )
    worker_tasks.append(asyncio.create_task(w.run()))
    logger.info("Parse worker created", extra={"max_concurrent": PARSE_WORKER_COUNT, "task_queue": PARSE_QUEUE})

    w = Worker(
        client=client,
        task_queue=SUMMARIZE_QUEUE,
        activities=[summarize_activity.summarize_records],
        max_concurrent_activities=SUMMARIZE_WORKER_COUNT,
    )
    worker_tasks.append(asyncio.create_task(w.run()))
    logger.info("Summarize worker created", extra={"max_concurrent": SUMMARIZE_WORKER_COUNT, "task_queue": SUMMARIZE_QUEUE})

    logger.info(
        "All workers started",
        extra={"total_workers": 1 + FETCH_WORKER_COUNT + PARSE_WORKER_COUNT + SUMMARIZE_WORKER_COUNT},
    )
    await asyncio.gather(*worker_tasks)
