import logging

from src.domain.entities.job import Job
from src.domain.entities.task import Task

logger = logging.getLogger(__name__)


class Scheduler:
    async def schedule_job(self, job: Job, urls: list[str]) -> list[Task]:
        tasks = [Task(job_id=job.id, url=url) for url in urls]
        logger.info("Scheduled tasks", extra={"job_id": job.id, "task_count": len(tasks)})
        return tasks