import gzip
import html
import re

from fastapi import APIRouter, HTTPException

router = APIRouter(tags=["records"])


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
    from src.main import get_db_repos
    async with get_db_repos() as (record_repo, job_repo):
        job = await job_repo.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        records = await record_repo.list_by_job(job_id, offset=offset, limit=limit)
        return [
            {
                "id": record.id,
                "task_id": record.task_id,
                "title": record.title,
                "author": record.author,
                "published_date": str(record.published_date) if record.published_date else None,
                "source_link": record.source_link,
                "description": _clean_html(record.description),
                "content": _clean_html(record.content, max_len=500),
                "summary": _clean_html(record.summary_text) if record.summary_text else None,
            }
            for record in records
        ]


@router.get("/records/{record_id}")
async def get_record(record_id: str) -> dict:
    from src.main import get_db_repos
    async with get_db_repos() as (record_repo, job_repo):
        record = await record_repo.get(record_id)
        if record is None:
            raise HTTPException(status_code=404, detail="Record not found")
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
            "summary": _clean_html(record.summary_text) if record.summary_text else None,
        }
