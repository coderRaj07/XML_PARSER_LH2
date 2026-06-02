from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.application.interfaces.repositories import JobRepository
from src.domain.entities import Job
from src.domain.enums.job_status import JobStatus
from src.infrastructure.db.models import JobModel


class PostgresJobRepository(JobRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, job: Job) -> Job:
        model = JobModel(
            id=job.id,
            status=job.status.value,
            total_tasks=job.total_tasks,
            completed_tasks=job.completed_tasks,
            failed_tasks=job.failed_tasks,
            created_at=job.created_at,
            completed_at=job.completed_at,
        )
        self._session.add(model)
        await self._session.flush()
        return job

    async def get(self, job_id: str) -> Optional[Job]:
        stmt = select(JobModel).where(JobModel.id == job_id)
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is None:
            return None
        return self._to_domain(model)

    async def update(self, job: Job) -> Job:
        stmt = select(JobModel).where(JobModel.id == job.id)
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is None:
            raise ValueError(f"Job {job.id} not found")
        model.status = job.status.value
        model.total_tasks = job.total_tasks
        model.completed_tasks = job.completed_tasks
        model.failed_tasks = job.failed_tasks
        model.completed_at = job.completed_at
        await self._session.flush()
        return job

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()

    async def close(self) -> None:
        await self._session.close()

    @staticmethod
    def _to_domain(model: JobModel) -> Job:
        return Job(
            id=model.id,
            status=JobStatus(model.status),
            total_tasks=model.total_tasks,
            completed_tasks=model.completed_tasks,
            failed_tasks=model.failed_tasks,
            created_at=model.created_at,
            completed_at=model.completed_at,
        )