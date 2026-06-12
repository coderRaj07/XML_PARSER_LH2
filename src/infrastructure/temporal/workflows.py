import asyncio
from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from src.infrastructure.temporal.config import (
        ACTIVITY_TIMEOUT,
        FETCH_QUEUE,
        PARSE_QUEUE,
        RETRY_POLICY,
        SUMMARIZE_QUEUE,
        WORKFLOW_NAME,
    )


@workflow.defn(name=WORKFLOW_NAME)
class JobWorkflow:
    @workflow.run
    async def run(self, job_id: str, tasks: list[tuple[str, str]]) -> dict[str, Any]:
        results = await asyncio.gather(
            *[self._process_url(task_id, url, job_id) for task_id, url in tasks],
            return_exceptions=True,
        )

        task_results: list[dict[str, Any]] = []
        for result in results:
            if isinstance(result, Exception):
                task_results.append({"status": "failed", "error": str(result)})
            else:
                task_results.append(result)

        completed = sum(1 for r in task_results if r.get("status") == "completed")
        failed = sum(1 for r in task_results if r.get("status") == "failed")

        return {
            "job_id": job_id,
            "total": len(tasks),
            "completed": completed,
            "failed": failed,
            "results": task_results,
        }

    async def _process_url(self, task_id: str, url: str, job_id: str) -> dict[str, Any]:
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
                args=[task_id, fetch_result["raw_xml"], job_id],
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
