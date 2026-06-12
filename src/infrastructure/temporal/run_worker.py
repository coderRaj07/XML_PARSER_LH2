import asyncio
import logging
import os
import sys

from src.application.interfaces.fetcher import Fetcher
from src.application.strategies.parser.base_parser_strategy import ParserStrategy
from src.application.services.summary_service import SummaryService
from src.infrastructure.db import DatabaseSessionManager
from src.infrastructure.fetchers import AioHttpFetcher
from src.infrastructure.temporal.worker import run_queue_worker
from src.application.strategies.parser import RSSParser
from src.application.strategies.summary import ExtractiveSummaryStrategy

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format='{"time": "%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "message": "%(message)s"}',
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/xml_feeds")
TEMPORAL_HOST = os.getenv("TEMPORAL_HOST", "localhost:7233")
QUEUE = os.getenv("QUEUE", "workflow")


async def main() -> None:
    DatabaseSessionManager.initialize(DATABASE_URL)
    await DatabaseSessionManager.create_tables()
    session_factory = DatabaseSessionManager.get_session_factory()

    fetcher: Fetcher = AioHttpFetcher()
    parser: ParserStrategy = RSSParser()
    summary_strategy = ExtractiveSummaryStrategy()
    summary_service = SummaryService(strategy=summary_strategy)

    logger.info(f"Starting {QUEUE} worker")
    await run_queue_worker(
        temporal_host=TEMPORAL_HOST,
        session_factory=session_factory,
        fetcher=fetcher,
        parser=parser,
        summary_service=summary_service,
        queue=QUEUE,
    )


if __name__ == "__main__":
    asyncio.run(main())
