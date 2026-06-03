import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import Column, DateTime, ForeignKey, Integer, LargeBinary, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class JobModel(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    total_tasks: Mapped[int] = mapped_column(Integer, default=0)
    completed_tasks: Mapped[int] = mapped_column(Integer, default=0)
    failed_tasks: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    tasks: Mapped[list["TaskModel"]] = relationship("TaskModel", back_populates="job", cascade="all, delete-orphan")


class TaskModel(Base):
    __tablename__ = "tasks"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    job_id: Mapped[str] = mapped_column(String(36), ForeignKey("jobs.id"), nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    job: Mapped["JobModel"] = relationship("JobModel", back_populates="tasks")
    records: Mapped[list["RecordModel"]] = relationship("RecordModel", back_populates="task", cascade="all, delete-orphan")


class RecordModel(Base):
    __tablename__ = "records"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    task_id: Mapped[str] = mapped_column(String(36), ForeignKey("tasks.id"), nullable=False)
    title: Mapped[str] = mapped_column(Text, default="")
    author: Mapped[str] = mapped_column(Text, default="")
    published_date: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    source_link: Mapped[str] = mapped_column(Text, default="")
    description: Mapped[str] = mapped_column(Text, default="")
    content: Mapped[str] = mapped_column(Text, default="")
    full_content: Mapped[Optional[bytes]] = mapped_column(LargeBinary, nullable=True)

    task: Mapped["TaskModel"] = relationship("TaskModel", back_populates="records")
    summaries: Mapped[list["SummaryModel"]] = relationship("SummaryModel", back_populates="record", cascade="all, delete-orphan")


class SummaryModel(Base):
    __tablename__ = "summaries"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    record_id: Mapped[str] = mapped_column(String(36), ForeignKey("records.id"), nullable=False)
    summary_text: Mapped[str] = mapped_column(Text, default="")
    summary_type: Mapped[str] = mapped_column(String(50), default="")
    model_used: Mapped[str] = mapped_column(String(100), default="")

    record: Mapped["RecordModel"] = relationship("RecordModel", back_populates="summaries")
