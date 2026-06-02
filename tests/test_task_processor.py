from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.application.services.summary_service import SummaryService
from src.application.services.task_processor import TaskProcessor
from src.application.strategies.parser import RSSParser
from src.application.strategies.summary import TemplateSummaryStrategy
from src.domain.entities.task import Task
from src.domain.enums.task_status import TaskStatus


@pytest.fixture
def mock_fetcher() -> AsyncMock:
    fetcher = AsyncMock()
    fetcher.fetch.return_value = """<?xml version="1.0"?>
<rss><channel><item><title>Test</title></item></channel></rss>"""
    return fetcher


@pytest.fixture
def mock_repos() -> dict:
    task_repo = AsyncMock()
    task_repo.count_by_status.return_value = (0, 1, 0)
    return {
        "task_repo": task_repo,
        "record_repo": AsyncMock(),
        "job_repo": AsyncMock(),
    }


@pytest.fixture
def processor(mock_fetcher: AsyncMock, mock_repos: dict) -> TaskProcessor:
    parser = RSSParser()
    summary_svc = SummaryService(strategy=TemplateSummaryStrategy())
    return TaskProcessor(
        fetcher=mock_fetcher,
        parser=parser,
        summary_service=summary_svc,
        task_repository=mock_repos["task_repo"],
        record_repository=mock_repos["record_repo"],
        job_repository=mock_repos["job_repo"],
    )


class TestTaskProcessor:
    @pytest.mark.asyncio
    async def test_process_task_success(self, processor: TaskProcessor, mock_repos: dict) -> None:
        task = Task(id="task-1", job_id="job-1", url="http://example.com/feed", status=TaskStatus.PENDING)
        result = await processor.process_task(task)
        assert result.status == TaskStatus.COMPLETED
        mock_repos["task_repo"].update.assert_awaited_once()
        mock_repos["record_repo"].create_many.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_process_task_fetch_failure(self, mock_fetcher: AsyncMock, mock_repos: dict) -> None:
        mock_fetcher.fetch.side_effect = RuntimeError("Connection error")
        parser = RSSParser()
        summary_svc = SummaryService(strategy=TemplateSummaryStrategy())
        processor = TaskProcessor(
            fetcher=mock_fetcher,
            parser=parser,
            summary_service=summary_svc,
            task_repository=mock_repos["task_repo"],
            record_repository=mock_repos["record_repo"],
            job_repository=mock_repos["job_repo"],
        )
        task = Task(id="task-2", job_id="job-1", url="http://example.com/feed", status=TaskStatus.PENDING)
        result = await processor.process_task(task)
        assert result.status == TaskStatus.FAILED
        assert result.error is not None
