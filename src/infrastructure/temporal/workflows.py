import asyncio
from typing import Any

from temporalio import workflow
from temporalio.common import WorkflowIDReusePolicy

from src.infrastructure.temporal.config import (
    ACTIVITY_TIMEOUT,
    BATCH_GATHER_TIMEOUT,
    BATCH_SIZE,
    CHILD_WORKFLOW_TIMEOUT,
    FETCH_QUEUE,
    MAX_CONCURRENT_URLS,
    PARSE_QUEUE,
    SUMMARIZE_QUEUE,
    WORKFLOW_QUEUE,
    WORKFLOW_NAME,
    RETRY_POLICY,
)

CHILD_WORKFLOW_NAME = "url-workflow"


@workflow.defn(name=WORKFLOW_NAME)
class JobWorkflow:
    @workflow.run
    async def run(self, job_id: str, tasks: list[tuple[str, str]]) -> dict[str, Any]:
        if not tasks:
            return {"job_id": job_id, "total": 0, "completed": 0, "failed": 0}

        batch = tasks[:BATCH_SIZE]
        remaining = tasks[BATCH_SIZE:]

        handles = []
        for task_id, url in batch:
            try:
                handle = await workflow.start_child_workflow(
                    CHILD_WORKFLOW_NAME,
                    args=[task_id, url, job_id],
                    id=f"{job_id}/url/{task_id}",
                    task_queue=WORKFLOW_QUEUE,
                    execution_timeout=CHILD_WORKFLOW_TIMEOUT,
                    id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                )
                handles.append(handle)
            except Exception:
                pass

        if remaining:
            workflow.continue_as_new(args=[job_id, remaining])

        sem = asyncio.Semaphore(MAX_CONCURRENT_URLS)

        async def _await_handle(h: Any) -> dict[str, Any]:
            async with sem:
                return await h

        coros = [_await_handle(h) for h in handles]
        futures = [asyncio.create_task(c) for c in coros]
        done, pending = await asyncio.wait(futures, timeout=BATCH_GATHER_TIMEOUT.total_seconds())

        results: list[Any] = []
        for task in done:
            try:
                results.append(task.result())
            except Exception as e:
                results.append(e)
        for task in pending:
            task.cancel()
            results.append({"status": "failed", "error": "batch_gather_timeout"})

        completed = sum(
            1 for r in results
            if isinstance(r, dict) and r.get("status") == "completed"
        )
        failed = sum(
            1 for r in results
            if isinstance(r, Exception) or (isinstance(r, dict) and r.get("status") == "failed")
        )

        return {
            "job_id": job_id,
            "total": len(batch),
            "completed": completed,
            "failed": failed,
        }


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
