import asyncio
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from src.infrastructure.temporal.activities import FetchActivity, ParseActivity, SummarizeActivity

WORKFLOW_NAME = "job-workflow"
FETCH_QUEUE = "xml-feed-fetch-queue"
PARSE_QUEUE = "xml-feed-parse-queue"
SUMMARIZE_QUEUE = "xml-feed-summarize-queue"

ACTIVITY_TIMEOUT = timedelta(minutes=5)
RETRY_POLICY = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=30),
    backoff_coefficient=2.0,
)


@workflow.defn(name=WORKFLOW_NAME)
class JobWorkflow:
    @workflow.run
    async def run(self, job_id: str, tasks: list[tuple[str, str]]) -> dict[str, Any]:
        task_results: dict[str, dict[str, Any]] = {}

        # Stage 1: Fetch all URLs
        fetch_futures = [
            workflow.execute_activity(
                "fetch_url",
                args=[task_id, url, job_id],
                task_queue=FETCH_QUEUE,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY_POLICY,
            )
            for task_id, url in tasks
        ]
        fetch_results = await asyncio.gather(*fetch_futures, return_exceptions=True)

        successful_fetches: list[tuple[str, dict]] = []
        for (task_id, url), result in zip(tasks, fetch_results):
            if isinstance(result, Exception):
                task_results[task_id] = {"status": "failed", "error": str(result)}
            elif result.get("status") == "failed":
                task_results[task_id] = result
            else:
                successful_fetches.append((task_id, result))

        # Stage 2: Parse all successfully fetched XMLs
        parse_futures = [
            workflow.execute_activity(
                "parse_records",
                args=[task_id, result["raw_xml"], job_id],
                task_queue=PARSE_QUEUE,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY_POLICY,
            )
            for task_id, result in successful_fetches
        ]
        parse_results = await asyncio.gather(*parse_futures, return_exceptions=True)

        successful_parses: list[str] = []
        for (task_id, _), result in zip(successful_fetches, parse_results):
            if isinstance(result, Exception):
                task_results[task_id] = {"status": "failed", "error": str(result)}
            elif result.get("status") == "failed":
                task_results[task_id] = result
            else:
                successful_parses.append(task_id)

        # Stage 3: Summarize all successfully parsed records
        summarize_futures = [
            workflow.execute_activity(
                "summarize_records",
                args=[task_id, job_id],
                task_queue=SUMMARIZE_QUEUE,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY_POLICY,
            )
            for task_id in successful_parses
        ]
        summarize_results = await asyncio.gather(*summarize_futures, return_exceptions=True)

        for task_id, result in zip(successful_parses, summarize_results):
            if isinstance(result, Exception):
                task_results[task_id] = {"status": "failed", "error": str(result)}
            else:
                task_results[task_id] = result

        completed = sum(1 for r in task_results.values() if r.get("status") == "completed")
        failed = sum(1 for r in task_results.values() if r.get("status") == "failed")

        return {
            "job_id": job_id,
            "total": len(tasks),
            "completed": completed,
            "failed": failed,
            "results": [task_results[tid] for tid, _ in tasks],
        }
