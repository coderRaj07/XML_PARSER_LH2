from typing import Optional

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from src.application.interfaces.repositories import RecordRepository
from src.domain.entities import Record, Summary
from src.infrastructure.db.models import RecordModel, SummaryModel, TaskModel


class PostgresRecordRepository(RecordRepository):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, record: Record) -> Record:
        model = self._to_model(record)
        self._session.add(model)
        await self._session.flush()
        return record

    async def create_many(self, records: list[Record]) -> list[Record]:
        models = [self._to_model(r) for r in records]
        self._session.add_all(models)
        await self._session.flush()
        return records

    async def update_content(self, record_id: str, content: str, s3_key: str) -> None:
        stmt = select(RecordModel).where(RecordModel.id == record_id)
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is not None:
            model.content = content
            model.full_content_s3_key = s3_key
            await self._session.flush()

    async def list_by_task(self, task_id: str) -> list[Record]:
        stmt = (
            select(RecordModel)
            .where(RecordModel.task_id == task_id)
            .options(joinedload(RecordModel.summaries))
        )
        result = await self._session.execute(stmt)
        return [self._to_domain(m) for m in result.scalars().unique().all()]

    async def get(self, record_id: str) -> Optional[Record]:
        stmt = (
            select(RecordModel)
            .where(RecordModel.id == record_id)
            .options(joinedload(RecordModel.summaries))
        )
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is None:
            return None
        return self._to_domain(model)

    async def save_summary(self, summary: Summary) -> Summary:
        model = SummaryModel(
            id=summary.id,
            record_id=summary.record_id,
            summary_text=summary.summary_text,
            summary_type=summary.summary_type,
            model_used=summary.model_used,
        )
        self._session.add(model)
        await self._session.flush()
        return summary

    async def save_summaries_many(self, summaries: list[Summary]) -> list[Summary]:
        await self._session.execute(
            insert(SummaryModel),
            [
                {
                    "id": s.id,
                    "record_id": s.record_id,
                    "summary_text": s.summary_text,
                    "summary_type": s.summary_type,
                    "model_used": s.model_used,
                }
                for s in summaries
            ],
        )
        await self._session.flush()
        return summaries

    async def get_summary(self, record_id: str) -> Optional[Summary]:
        stmt = select(SummaryModel).where(SummaryModel.record_id == record_id)
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is None:
            return None
        return Summary(
            id=model.id,
            record_id=model.record_id,
            summary_text=model.summary_text,
            summary_type=model.summary_type,
            model_used=model.model_used,
        )

    async def list_by_job(
        self, job_id: str, offset: int = 0, limit: int = 50
    ) -> list[Record]:
        stmt = (
            select(RecordModel)
            .join(TaskModel, TaskModel.id == RecordModel.task_id)
            .where(TaskModel.job_id == job_id)
            .options(joinedload(RecordModel.summaries))
            .offset(offset)
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return [self._to_domain(m) for m in result.scalars().unique().all()]

    async def commit(self) -> None:
        await self._session.commit()

    async def rollback(self) -> None:
        await self._session.rollback()

    async def close(self) -> None:
        await self._session.close()

    @staticmethod
    def _to_model(record: Record) -> RecordModel:
        return RecordModel(
            id=record.id,
            task_id=record.task_id,
            title=record.title,
            author=record.author,
            published_date=record.published_date,
            source_link=record.source_link,
            description=record.description,
            content=record.content,
            full_content_s3_key=record.full_content_s3_key,
        )

    @staticmethod
    def _to_domain(model: RecordModel) -> Record:
        summary = model.summaries[0] if model.summaries else None
        return Record(
            id=model.id,
            task_id=model.task_id,
            title=model.title,
            author=model.author,
            published_date=model.published_date,
            source_link=model.source_link,
            description=model.description,
            content=model.content,
            full_content_s3_key=model.full_content_s3_key,
            summary_text=summary.summary_text if summary else None,
            summary_type=summary.summary_type if summary else None,
            model_used=summary.model_used if summary else None,
        )
