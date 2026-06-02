import logging

from src.application.interfaces.execution_engine import ExecutionEngine
from src.application.interfaces.repositories import JobRepository, TaskRepository
from src.application.services.scheduler import Scheduler
from src.domain.entities.job import Job
from src.domain.entities.task import Task
from src.domain.enums.job_status import JobStatus

logger = logging.getLogger(__name__)


class JobService:
    def __init__(
        self,
        job_repository: JobRepository,
        task_repository: TaskRepository,
        scheduler: Scheduler,
        execution_engine: ExecutionEngine,
    ) -> None:
        self._job_repository = job_repository
        self._task_repository = task_repository
        self._scheduler = scheduler
        self._execution_engine = execution_engine

    async def create_job(self, urls: list[str]) -> Job:
        job = Job(total_tasks=len(urls))
        job = await self._job_repository.create(job)
        tasks = await self._scheduler.schedule_job(job, urls)

        for task in tasks:
            await self._task_repository.create(task)

        job.status = JobStatus.RUNNING
        await self._job_repository.update(job)
        await self._execution_engine.start_job(job.id, [(task.id, task.url) for task in tasks])

        logger.info("job_started", extra={"job_id": job.id, "url_count": len(urls)})
        return job

    async def get_job(self, job_id: str) -> Job | None:
        return await self._job_repository.get(job_id)

    async def list_tasks(self, job_id: str) -> list[Task]:
        return await self._task_repository.list_by_job(job_id)