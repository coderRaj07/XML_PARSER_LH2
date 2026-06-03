import gzip
import html
import re

from fastapi import APIRouter, HTTPException

from src.application.interfaces.repositories import JobRepository, RecordRepository

router = APIRouter(tags=["records"])


async def get_repos() -> tuple[RecordRepository, JobRepository]:
    from src.main import get_db_repos
    return await get_db_repos()


def _clean_html(text: str | None, max_len: int | None = None) -> str | None:
    if not text:
        return text
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    if max_len and len(text) > max_len:
        text = text[:max_len] + "..."
    return text if text else None


@router.get("/jobs/{job_id}/records")
async def list_records(job_id: str, limit: int = 50, offset: int = 0) -> list[dict]:
    record_repo, job_repo = await get_repos()
    try:
        job = await job_repo.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        records = await record_repo.list_by_job(job_id)
        result = []
        for record in records[offset:offset + limit]:
            summary = await record_repo.get_summary(record.id)
            result.append({
                "id": record.id,
                "task_id": record.task_id,
                "title": record.title,
                "author": record.author,
                "published_date": str(record.published_date) if record.published_date else None,
                "source_link": record.source_link,
                "description": _clean_html(record.description),
                "content": _clean_html(record.content, max_len=500),
                "summary": _clean_html(summary.summary_text) if summary else None,
            })
        return result
    finally:
        await record_repo.close()
        await job_repo.close()


@router.get("/records/{record_id}")
async def get_record(record_id: str) -> dict:
    record_repo, job_repo = await get_repos()
    try:
        record = await record_repo.get(record_id)
        if record is None:
            raise HTTPException(status_code=404, detail="Record not found")
        summary = await record_repo.get_summary(record.id)
        full_content = None
        if record.full_content:
            try:
                full_content = gzip.decompress(record.full_content).decode("utf-8")
            except Exception:
                pass
        return {
            "id": record.id,
            "task_id": record.task_id,
            "title": record.title,
            "author": record.author,
            "published_date": str(record.published_date) if record.published_date else None,
            "source_link": record.source_link,
            "description": _clean_html(record.description),
            "content": _clean_html(record.content),
            "full_content": full_content,
            "summary": _clean_html(summary.summary_text) if summary else None,
        }
    finally:
        await record_repo.close()
        await job_repo.close()
