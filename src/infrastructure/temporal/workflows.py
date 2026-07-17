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
ENRICHMENT_BATCH_SIZE = 5


@workflow.defn(name=WORKFLOW_NAME)
class JobWorkflow:
    @workflow.run
    async def run(self, job_id: str, tasks: list[tuple[str, str]], previous_completed: int = 0, previous_failed: int = 0) -> dict[str, Any]:
        if not tasks:
            return {"job_id": job_id, "total": 0, "completed": previous_completed, "failed": previous_failed}

        logger.info("job_workflow_batch_start", extra={"job_id": job_id, "batch_size": len(tasks)})

        batch = tasks[:BATCH_SIZE]
        remaining = tasks[BATCH_SIZE:]

        handles = []
        workflow_ids = []
        for task_id, url in batch:
            wf_id = f"{job_id}/url/{task_id}"
            try:
                handle = await workflow.start_child_workflow(
                    CHILD_WORKFLOW_NAME,
                    args=[task_id, url, job_id],
                    id=wf_id,
                    task_queue=URL_WORKFLOW_QUEUE,
                    execution_timeout=CHILD_WORKFLOW_TIMEOUT,
                    id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
                    parent_close_policy=ParentClosePolicy.ABANDON,
                )
                handles.append(handle)
                workflow_ids.append(wf_id)
            except Exception:
                pass

        logger.info("job_workflow_children_started", extra={"job_id": job_id, "count": len(handles)})

        sem = asyncio.Semaphore(MAX_CONCURRENT_URLS)

        async def _throttled(h: Any, wf_id: str) -> Any:
            async with sem:
                result = await h.result()
                logger.info("job_workflow_child_completed", extra={"job_id": job_id, "workflow_id": wf_id, "status": result.get("status") if isinstance(result, dict) else "exception"})
                return result

        results = await asyncio.gather(
            *[_throttled(h, wid) for h, wid in zip(handles, workflow_ids)],
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

        logger.info("job_workflow_batch_done", extra={"job_id": job_id, "batch_completed": completed - previous_completed, "batch_failed": failed - previous_failed, "remaining": len(remaining)})

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
        logger.info("url_workflow_started", extra={"task_id": task_id, "url": url})

        try:
            logger.info("url_workflow_fetch_start", extra={"task_id": task_id})
            fetch_result = await workflow.execute_activity(
                "fetch_url",
                args=[task_id, url, job_id],
                task_queue=FETCH_QUEUE,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY_POLICY,
            )
            logger.info("url_workflow_fetch_done", extra={"task_id": task_id, "status": fetch_result.get("status")})
        except Exception as e:
            logger.exception("url_workflow_fetch_error", extra={"task_id": task_id, "error": str(e)})
            return {"task_id": task_id, "status": "failed", "error": str(e)}
        if fetch_result.get("status") == "failed":
            return fetch_result

        try:
            logger.info("url_workflow_parse_start", extra={"task_id": task_id})
            parse_result = await workflow.execute_activity(
                "parse_records",
                args=[task_id, fetch_result["storage_key"], job_id],
                task_queue=PARSE_QUEUE,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY_POLICY,
            )
            logger.info("url_workflow_parse_done", extra={"task_id": task_id, "status": parse_result.get("status"), "record_count": parse_result.get("record_count")})
        except Exception as e:
            logger.exception("url_workflow_parse_error", extra={"task_id": task_id, "error": str(e)})
            return {"task_id": task_id, "status": "failed", "error": str(e)}
        if parse_result.get("status") == "failed":
            return parse_result

        record_infos = parse_result.get("record_infos", [])
        if record_infos:
            enrichment_timeout = timedelta(seconds=min(len(record_infos) * 30, 300))
            try:
                logger.info("url_workflow_enrichment_start", extra={"task_id": task_id, "records": len(record_infos), "timeout_s": enrichment_timeout.total_seconds()})
                enrichment_handle = await workflow.start_child_workflow(
                    ENRICHMENT_WORKFLOW_NAME,
                    args=[record_infos, job_id, task_id],
                    id=f"{job_id}/enrichment/{task_id}",
                    task_queue=ENRICHMENT_QUEUE,
                    execution_timeout=enrichment_timeout,
                    id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
                    parent_close_policy=ParentClosePolicy.ABANDON,
                )
                await enrichment_handle.result()
                logger.info("url_workflow_enrichment_done", extra={"task_id": task_id})
            except Exception as e:
                logger.exception("url_workflow_enrichment_failed", extra={"task_id": task_id, "error": str(e)})

        try:
            logger.info("url_workflow_summarize_start", extra={"task_id": task_id})
            summarize_result = await workflow.execute_activity(
                "summarize_records",
                args=[task_id, job_id],
                task_queue=SUMMARIZE_QUEUE,
                start_to_close_timeout=ACTIVITY_TIMEOUT,
                retry_policy=RETRY_POLICY,
            )
            logger.info("url_workflow_summarize_done", extra={"task_id": task_id, "status": summarize_result.get("status")})
        except Exception as e:
            logger.exception("url_workflow_summarize_error", extra={"task_id": task_id, "error": str(e)})
            return {"task_id": task_id, "status": "failed", "error": str(e)}

        logger.info("url_workflow_completed", extra={"task_id": task_id})
        return summarize_result



@workflow.defn(name=ENRICHMENT_WORKFLOW_NAME)
class EnrichmentWorkflow:
    @workflow.run
    async def run(self, record_infos: list[dict], job_id: str, task_id: str) -> dict[str, Any]:
        logger.info("enrichment_workflow_started", extra={"task_id": task_id, "total_records": len(record_infos)})
        total_enriched = 0
        total_failed = 0

        for batch_start in range(0, len(record_infos), ENRICHMENT_BATCH_SIZE):
            batch = record_infos[batch_start : batch_start + ENRICHMENT_BATCH_SIZE]
            batch_num = batch_start // ENRICHMENT_BATCH_SIZE + 1
            handles = []
            for info in batch:
                try:
                    handle = workflow.execute_activity(
                        "fetch_article",
                        args=[info["id"], info["source_link"], job_id, task_id],
                        task_queue=ENRICHMENT_QUEUE,
                        start_to_close_timeout=timedelta(seconds=90),
                        retry_policy=RETRY_POLICY,
                    )
                    handles.append(handle)
                except Exception:
                    pass

            if not handles:
                continue

            logger.info("enrichment_batch_start", extra={"task_id": task_id, "batch": batch_num, "activities": len(handles)})
            results = await asyncio.gather(*handles, return_exceptions=True)

            batch_enriched = sum(
                1 for r in results
                if isinstance(r, dict) and r.get("status") == "completed"
            )
            batch_failed = sum(
                1 for r in results
                if isinstance(r, Exception) or (isinstance(r, dict) and r.get("status") == "failed")
            )
            total_enriched += batch_enriched
            total_failed += batch_failed
            logger.info("enrichment_batch_done", extra={"task_id": task_id, "batch": batch_num, "enriched": batch_enriched, "failed": batch_failed})

        logger.info("enrichment_workflow_completed", extra={"task_id": task_id, "total_enriched": total_enriched, "total_failed": total_failed})
        return {"task_id": task_id, "enriched": total_enriched, "failed": total_failed}
