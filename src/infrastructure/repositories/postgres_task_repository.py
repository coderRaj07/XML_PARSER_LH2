from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.interfaces.repositories import TaskRepository
from src.domain.entities import Task
from src.domain.enums.task_status import TaskStatus
from src.infrastructure.db.models import TaskModel


class PostgresTaskRepository(TaskRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, task: Task) -> Task:
        model = TaskModel(
            id=task.id,
            job_id=task.job_id,
            url=task.url,
            status=task.status.value,
            attempts=task.attempts,
            error=task.error,
            created_at=task.created_at,
        )
        self._session.add(model)
        await self._session.flush()
        return task

    async def get(self, task_id: str) -> Optional[Task]:
        stmt = select(TaskModel).where(TaskModel.id == task_id)
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is None:
            return None
        return self._to_domain(model)

    async def update(self, task: Task) -> Task:
        stmt = select(TaskModel).where(TaskModel.id == task.id)
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is None:
            raise ValueError(f"Task {task.id} not found")
        model.status = task.status.value
        model.attempts = task.attempts
        model.error = task.error
        model.url = task.url
        await self._session.flush()
        return task

    async def list_by_job(self, job_id: str) -> list[Task]:
        stmt = select(TaskModel).where(TaskModel.job_id == job_id)
        result = await self._session.execute(stmt)
        return [self._to_domain(m) for m in result.scalars().all()]

    async def count_by_status(self, job_id: str) -> tuple[int, int, int]:
        stmt = (
            select(
                func.count().filter(TaskModel.status == "pending"),
                func.count().filter(TaskModel.status == "completed"),
                func.count().filter(TaskModel.status == "failed"),
            ).where(TaskModel.job_id == job_id)
        )
        result = await self._session.execute(stmt)
        row = result.one()
        return row.tuple()[0], row.tuple()[1], row.tuple()[2]

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()

    async def close(self) -> None:
        await self._session.close()

    @staticmethod
    def _to_domain(model: TaskModel) -> Task:
        return Task(
            id=model.id,
            job_id=model.job_id,
            url=model.url,
            status=TaskStatus(model.status),
            attempts=model.attempts,
            error=model.error,
            created_at=model.created_at,
        )
