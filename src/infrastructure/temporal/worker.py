import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from temporalio.client import Client
from temporalio.worker import Worker

from src.application.interfaces.fetcher import Fetcher
from src.application.services.summary_service import SummaryService
from src.application.strategies.parser.base_parser_strategy import ParserStrategy
from src.infrastructure.storage import S3Storage
from src.infrastructure.temporal.activities import FetchActivity, ParseActivity, SummarizeActivity
from src.infrastructure.temporal.config import (
    FETCH_WORKER_COUNT,
    PARSE_WORKER_COUNT,
    SUMMARIZE_WORKER_COUNT,
)
from src.infrastructure.temporal.config import (
    FETCH_QUEUE,
    PARSE_QUEUE,
    SUMMARIZE_QUEUE,
    WORKFLOW_QUEUE,
)
from src.infrastructure.temporal.workflows import (
    JobWorkflow,
    UrlWorkflow,
)

logger = logging.getLogger(__name__)


async def run_queue_worker(
    temporal_host: str,
    session_factory: async_sessionmaker[AsyncSession],
    fetcher: Fetcher,
    parser: ParserStrategy,
    summary_service: SummaryService,
    storage: S3Storage,
    queue: str,
) -> None:
    client = await Client.connect(temporal_host)

    if queue == "workflow":
        w = Worker(
            client=client,
            task_queue=WORKFLOW_QUEUE,
            workflows=[JobWorkflow, UrlWorkflow],
        )
        worker_task = asyncio.create_task(w.run())
        logger.info("Workflow worker started", extra={"task_queue": WORKFLOW_QUEUE})
        await worker_task

    elif queue == "fetch":
        activity = FetchActivity(session_factory=session_factory, fetcher=fetcher, storage=storage)
        w = Worker(
            client=client,
            task_queue=FETCH_QUEUE,
            activities=[activity.fetch_url],
            max_concurrent_activities=FETCH_WORKER_COUNT,
        )
        worker_task = asyncio.create_task(w.run())
        logger.info("Fetch worker started", extra={"task_queue": FETCH_QUEUE, "max_concurrent": FETCH_WORKER_COUNT})
        await worker_task

    elif queue == "parse":
        activity = ParseActivity(session_factory=session_factory, parser=parser, fetcher=fetcher, storage=storage)
        w = Worker(
            client=client,
            task_queue=PARSE_QUEUE,
            activities=[activity.parse_records],
            max_concurrent_activities=PARSE_WORKER_COUNT,
        )
        worker_task = asyncio.create_task(w.run())
        logger.info("Parse worker started", extra={"task_queue": PARSE_QUEUE, "max_concurrent": PARSE_WORKER_COUNT})
        await worker_task

    elif queue == "summarize":
        activity = SummarizeActivity(session_factory=session_factory, summary_service=summary_service)
        w = Worker(
            client=client,
            task_queue=SUMMARIZE_QUEUE,
            activities=[activity.summarize_records],
            max_concurrent_activities=SUMMARIZE_WORKER_COUNT,
        )
        worker_task = asyncio.create_task(w.run())
        logger.info("Summarize worker started", extra={"task_queue": SUMMARIZE_QUEUE, "max_concurrent": SUMMARIZE_WORKER_COUNT})
        await worker_task

    else:
        raise ValueError(f"Unknown queue: {queue}. Must be one of: workflow, fetch, parse, summarize")
