import asyncio
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from src.infrastructure.temporal.activities import URLProcessingActivity

WORKFLOW_NAME = "job-workflow"


@workflow.defn(name=WORKFLOW_NAME)
class JobWorkflow:
    @workflow.run
    async def run(self, job_id: str, tasks: list[tuple[str, str]]) -> dict[str, Any]:
        results = await asyncio.gather(
            *[
                workflow.execute_activity(
                    "process_url",
                    args=[task_id, url, job_id],
                    task_queue="xml-feed-queue",
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=RetryPolicy(
                        maximum_attempts=3,
                        initial_interval=timedelta(seconds=1),
                        maximum_interval=timedelta(seconds=30),
                        backoff_coefficient=2.0,
                    ),
                )
                for task_id, url in tasks
            ],
            return_exceptions=True,
        )

        processed_results: list[dict[str, Any]] = []
        for result in results:
            if isinstance(result, Exception):
                processed_results.append({
                    "status": "failed",
                    "error": str(result),
                })
            else:
                processed_results.append(result)

        completed = sum(1 for r in processed_results if r["status"] == "completed")
        failed = sum(1 for r in processed_results if r["status"] == "failed")

        return {
            "job_id": job_id,
            "total": len(tasks),
            "completed": completed,
            "failed": failed,
            "results": processed_results,
        }