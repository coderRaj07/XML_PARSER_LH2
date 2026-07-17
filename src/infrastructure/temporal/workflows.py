import asyncio
import logging
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import WorkflowIDReusePolicy
from temporalio.workflow import ParentClosePolicy

from src.infrastructure.temporal.config import (
    ACTIVITY_TIMEOUT,
    BATCH_SIZE,
    CHILD_WORKFLOW_TIMEOUT,
    ENRICHMENT_QUEUE,
    ENRICHMENT_WORKFLOW_NAME,
    FETCH_QUEUE,
    MAX_CONCURRENT_URLS,
    PARSE_QUEUE,
    SUMMARIZE_QUEUE,
    URL_WORKFLOW_QUEUE,
    WORKFLOW_NAME,
    RETRY_POLICY,
)

logger = logging.getLogger(__name__)

CHILD_WORKFLOW_NAME = "url-workflow"


@workflow.defn(name=WORKFLOW_NAME)
class JobWorkflow:
    @workflow.run
    async def run(self, job_id: str, tasks: list[tuple[str, str]], previous_completed: int = 0, previous_failed: int = 0) -> dict[str, Any]:
        if not tasks:
            return {"job_id": job_id, "total": 0, "completed": previous_completed, "failed": previous_failed}

        batch = tasks[:BATCH_SIZE]
        remaining = tasks[BATCH_SIZE:]

        handles = []
        for task_id, url in batch:
            try:
                handle = await workflow.start_child_workflow(
                    CHILD_WORKFLOW_NAME,
                    args=[task_id, url, job_id],
                    id=f"{job_id}/url/{task_id}",
                    task_queue=URL_WORKFLOW_QUEUE,
                    execution_timeout=CHILD_WORKFLOW_TIMEOUT,
                    id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
                    parent_close_policy=ParentClosePolicy.ABANDON,
                )
                handles.append(handle)
            except Exception:
                pass

        sem = asyncio.Semaphore(MAX_CONCURRENT_URLS)

        async def _throttled(h: Any) -> Any:
            async with sem:
                return await h.result()

        results = await asyncio.gather(
            *[_throttled(h) for h in handles],
            return_exceptions=True,
        )

        completed = previous_completed + sum(
            1 for r in results
            if isinstance(r, dict) and r.get("status") == "completed"
        )
        failed = previous_failed + sum(
            1 for r in results
            if isinstance(r, Exception) or (isinstance(r, dict) and r.get("status") == "failed")
        )

        if remaining:
            workflow.continue_as_new(args=[job_id, remaining, completed, failed])

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

        record_infos = parse_result.get("record_infos", [])
        if record_infos:
            try:
                enrichment_handle = await workflow.start_child_workflow(
                    ENRICHMENT_WORKFLOW_NAME,
                    args=[record_infos, job_id, task_id],
                    id=f"{job_id}/enrichment/{task_id}",
                    task_queue=ENRICHMENT_QUEUE,
                    execution_timeout=timedelta(minutes=5),
                    id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
                    parent_close_policy=ParentClosePolicy.ABANDON,
                )
                await enrichment_handle.result()
            except Exception as e:
                logger.warning("enrichment_child_failed", extra={"task_id": task_id, "error": str(e)})

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


@workflow.defn(name=ENRICHMENT_WORKFLOW_NAME)
class EnrichmentWorkflow:
    @workflow.run
    async def run(self, record_infos: list[dict], job_id: str, task_id: str) -> dict[str, Any]:
        handles = []
        for info in record_infos:
            try:
                handle = workflow.execute_activity(
                    "fetch_article",
                    args=[info["id"], info["source_link"], job_id, task_id],
                    task_queue=ENRICHMENT_QUEUE,
                    start_to_close_timeout=timedelta(minutes=2),
                    retry_policy=RETRY_POLICY,
                )
                handles.append(handle)
            except Exception:
                pass

        results = await asyncio.gather(*handles, return_exceptions=True)

        enriched = sum(
            1 for r in results
            if isinstance(r, dict) and r.get("status") == "completed"
        )
        failed = sum(
            1 for r in results
            if isinstance(r, Exception) or (isinstance(r, dict) and r.get("status") == "failed")
        )

        return {"task_id": task_id, "enriched": enriched, "failed": failed}
