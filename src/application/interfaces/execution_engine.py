from abc import ABC, abstractmethod
from typing import Any


class ExecutionEngine(ABC):
    @abstractmethod
    async def start_job(self, job_id: str, tasks: list[tuple[str, str]]) -> str:
        ...

    @abstractmethod
    async def monitor_workflow(self, job_id: str) -> dict[str, Any]:
        ...