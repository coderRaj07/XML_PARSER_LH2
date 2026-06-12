import asyncio
from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from src.infrastructure.temporal.config import (
        ACTIVITY_TIMEOUT,
        BATCH_SIZE,
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
        all_task_ids = [tid for tid, _ in tasks]
        task_results: dict[str, dict[str, Any]] = {}

        for batch_start in range(0, len(tasks), BATCH_SIZE):
            batch = tasks[batch_start : batch_start + BATCH_SIZE]
            results = await asyncio.gather(
                *[self._process_url(task_id, url, job_id) for task_id, url in batch],
                return_exceptions=True,
            )
            for (task_id, _), result in zip(batch, results):
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
            "results": [task_results[tid] for tid in all_task_ids],
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
