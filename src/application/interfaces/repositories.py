from abc import ABC, abstractmethod
from typing import Optional

from src.domain.entities import Job, Task, Record, Summary


class JobRepository(ABC):
    @abstractmethod
    async def rollback(self) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...


    @abstractmethod
    async def get(self, job_id: str) -> Optional[Job]:
        ...

    @abstractmethod
    async def update(self, job: Job) -> Job:
        ...

    @abstractmethod
    async def commit(self) -> None:
        ...

    @abstractmethod
    async def rollback(self) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...


class TaskRepository(ABC):
    @abstractmethod
    async def create(self, task: Task) -> Task:
        ...

    @abstractmethod
    async def get(self, task_id: str) -> Optional[Task]:
        ...

    @abstractmethod
    async def update(self, task: Task) -> Task:
        ...

    @abstractmethod
    async def list_by_job(self, job_id: str) -> list[Task]:
        ...

    @abstractmethod
    async def count_by_status(self, job_id: str) -> tuple[int, int, int]:
        ...

    @abstractmethod
    async def commit(self) -> None:
        ...

    @abstractmethod
    async def rollback(self) -> None:
        ...

    @abstractmethod
    async def rollback(self) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...


class RecordRepository(ABC):
    @abstractmethod
    async def create(self, record: Record) -> Record:
        ...

    @abstractmethod
    async def create_many(self, records: list[Record]) -> list[Record]:
        ...

    @abstractmethod
    async def update_content(self, record_id: str, content: str, s3_key: str) -> None:
        ...

    @abstractmethod
    async def list_by_task(self, task_id: str) -> list[Record]:
        ...

    @abstractmethod
    async def get(self, record_id: str) -> Optional[Record]:
        ...

    @abstractmethod
    async def save_summary(self, summary: Summary) -> Summary:
        ...

    @abstractmethod
    async def save_summaries_many(self, summaries: list[Summary]) -> list[Summary]:
        ...

    @abstractmethod
    async def get_summary(self, record_id: str) -> Optional[Summary]:
        ...

    @abstractmethod
    async def list_by_job(
        self, job_id: str, offset: int = 0, limit: int = 100
    ) -> list[Record]:
        ...

    @abstractmethod
    async def commit(self) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...

    @abstractmethod
    async def rollback(self) -> None:
        ...

    @abstractmethod
    async def commit(self) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...