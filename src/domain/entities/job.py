from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from src.domain.enums.job_status import JobStatus


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@dataclass
class Job:
    id: str = field(default_factory=lambda: str(uuid4()))
    status: JobStatus = JobStatus.PENDING
    total_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    created_at: datetime = field(default_factory=_utcnow)
    completed_at: Optional[datetime] = None

    def mark_completed(self) -> None:
        self.status = JobStatus.COMPLETED
        self.completed_at = _utcnow()

    def mark_failed(self) -> None:
        self.status = JobStatus.FAILED
        self.completed_at = _utcnow()

    def update_progress(self, completed: int, failed: int) -> None:
        self.completed_tasks = completed
        self.failed_tasks = failed
        if self.completed_tasks + self.failed_tasks == self.total_tasks:
            self.status = JobStatus.COMPLETED
            self.completed_at = _utcnow()
