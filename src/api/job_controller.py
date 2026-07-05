from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/jobs", tags=["jobs"])


class CreateJobRequest(BaseModel):
    urls: list[str]


class CreateJobResponse(BaseModel):
    job_id: str


class JobStatusResponse(BaseModel):
    status: str
    total: int
    completed: int
    failed: int


@router.post("", response_model=CreateJobResponse, status_code=201)
async def create_job(request: CreateJobRequest) -> CreateJobResponse:
    from src.main import get_job_service as factory
    async with factory() as (service, repo, task_repo):
        job = await service.create_job(request.urls)
        await repo.commit()
        return CreateJobResponse(job_id=job.id)


@router.get("/{job_id}", response_model=JobStatusResponse)
async def get_job(job_id: str) -> JobStatusResponse:
    from src.main import get_job_service as factory
    async with factory() as (service, repo, task_repo):
        job = await service.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        pending, completed, failed = await task_repo.count_by_status(job_id)
        return JobStatusResponse(
            status=job.status.value,
            total=job.total_tasks,
            completed=completed,
            failed=failed,
        )


@router.get("/{job_id}/tasks")
async def list_tasks(job_id: str) -> list[dict]:
    from src.main import get_job_service as factory
    async with factory() as (service, repo, task_repo):
        tasks = await service.list_tasks(job_id)
        return [
            {
                "id": t.id,
                "url": t.url,
                "status": t.status,
                "attempts": t.attempts,
                "error": t.error,
            }
            for t in tasks
        ]
