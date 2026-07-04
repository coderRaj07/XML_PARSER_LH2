import asyncio
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    pass

WORKFLOW_NAME = "job-workflow"
CHILD_WORKFLOW_NAME = "url-workflow"
WORKFLOW_QUEUE = "xml-feed-workflow-queue"
FETCH_QUEUE = "xml-feed-fetch-queue"
PARSE_QUEUE = "xml-feed-parse-queue"
SUMMARIZE_QUEUE = "xml-feed-summarize-queue"
BATCH_SIZE = 10  # keep per-batch history small; continue_as_new resets between batches
ACTIVITY_TIMEOUT = timedelta(minutes=5)
RETRY_POLICY = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=30),
    backoff_coefficient=2.0,
)
CHILD_WORKFLOW_TIMEOUT = timedelta(minutes=30)


@workflow.defn(name=WORKFLOW_NAME)
class JobWorkflow:
    @workflow.run
    async def run(self, job_id: str, tasks: list[tuple[str, str]]) -> dict[str, Any]:
        batch = tasks[:BATCH_SIZE]
        remaining = tasks[BATCH_SIZE:]

        results = await asyncio.gather(
            *[
                self._process_url_via_child(task_id, url, job_id)
                for task_id, url in batch
            ],
            return_exceptions=True,
        )

        if remaining:
            workflow.continue_as_new(args=[job_id, remaining])

        completed = sum(1 for r in results if not isinstance(r, Exception) and r.get("status") == "completed")
        failed = sum(1 for r in results if isinstance(r, Exception) or r.get("status") == "failed")

        return {
            "job_id": job_id,
            "total": len(batch),
            "completed": completed,
            "failed": failed,
        }

    async def _process_url_via_child(self, task_id: str, url: str, job_id: str) -> dict[str, Any]:
        try:
            result = await workflow.execute_child_workflow(
                CHILD_WORKFLOW_NAME,
                args=[task_id, url, job_id],
                id=f"{job_id}/url/{task_id}",
                task_queue=WORKFLOW_QUEUE,
                execution_timeout=CHILD_WORKFLOW_TIMEOUT,
            )
            return result
        except Exception as e:
            return {"task_id": task_id, "status": "failed", "error": str(e)}


@workflow.defn(name=CHILD_WORKFLOW_NAME)
class UrlWorkflow:
    @workflow.run
    async def run(self, task_id: str, url: str, job_id: str) -> dict[str, Any]:
        try:
            fetch_result = await workflow.execute_activity(
                "fetch_url",
                args=[task_id, url, job_id],
                task_queue=FETCH_QUEUE,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY_POLICY,
            )
        except Exception as e:
            return {"task_id": task_id, "status": "failed", "error": str(e)}
        if fetch_result.get("status") == "failed":
            return fetch_result

        try:
            parse_result = await workflow.execute_activity(
                "parse_records",
                args=[task_id, fetch_result["storage_key"], job_id],
                task_queue=PARSE_QUEUE,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY_POLICY,
            )
        except Exception as e:
            return {"task_id": task_id, "status": "failed", "error": str(e)}
        if parse_result.get("status") == "failed":
            return parse_result

        try:
            summarize_result = await workflow.execute_activity(
                "summarize_records",
                args=[task_id, job_id],
                task_queue=SUMMARIZE_QUEUE,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY_POLICY,
            )
        except Exception as e:
            return {"task_id": task_id, "status": "failed", "error": str(e)}
        return summarize_result
