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
        if pending == 0 and completed + failed >= job.total_tasks:
            actual_status = "completed"
        else:
            actual_status = job.status.value
        return JobStatusResponse(
            status=actual_status,
            total=job.total_tasks,
            completed=completed,
            failed=failed,
        )


@router.get("/{job_id}/tasks")
async def list_tasks(job_id: str) -> list[dict]:
    from src.main import get_job_service as factory
    async with factory() as (service, repo, task_repo):
        tasks = await service.list_tasks(job_id)
        result = []
        for t in tasks:
            status = t.status.value if hasattr(t.status, "value") else t.status
            if status == "pending":
                from src.infrastructure.db import DatabaseSessionManager
                from src.infrastructure.repositories import PostgresRecordRepository
                async with DatabaseSessionManager.get_session_factory()() as check_session:
                    rec_repo = PostgresRecordRepository(check_session)
                    records = await rec_repo.list_by_task(t.id)
                    if records and any(r.summary_text for r in records):
                        status = "completed"
                    elif records:
                        status = "failed"
            result.append({
                "id": t.id,
                "url": t.url,
                "status": status,
                "attempts": t.attempts,
                "error": t.error,
            })
        return result
