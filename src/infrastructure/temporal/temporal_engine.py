import logging
from typing import Any

from temporalio.client import Client

from src.application.interfaces.execution_engine import ExecutionEngine
from src.infrastructure.temporal.workflows import WORKFLOW_NAME, WORKFLOW_QUEUE

logger = logging.getLogger(__name__)


class TemporalEngine(ExecutionEngine):
    def __init__(self, temporal_host: str = "localhost:7233", task_queue: str = WORKFLOW_QUEUE) -> None:
        self._temporal_host = temporal_host
        self._task_queue = task_queue
        self._client: Client | None = None

    async def _ensure_client(self) -> Client:
        if self._client is None:
            self._client = await Client.connect(self._temporal_host)
        return self._client

    async def start_job(self, job_id: str, tasks: list[tuple[str, str]]) -> str:
        client = await self._ensure_client()
        handle = await client.start_workflow(
            WORKFLOW_NAME,
            args=[job_id, tasks],
            id=job_id,
            task_queue=self._task_queue,
        )
        logger.info("Workflow started", extra={"job_id": job_id, "workflow_id": handle.id})
        return handle.id

    async def monitor_workflow(self, job_id: str) -> dict[str, Any]:
        client = await self._ensure_client()
        handle = client.get_workflow_handle(job_id)
        result = await handle.result()
        details = await handle.describe()
        return {
            "job_id": job_id,
            "status": details.status.name,
            "result": result,
        }