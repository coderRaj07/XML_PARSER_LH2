from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from src.domain.enums.task_status import TaskStatus


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@dataclass
class Task:
    id: str = field(default_factory=lambda: str(uuid4()))
    job_id: str = ""
    url: str = ""
    status: TaskStatus = TaskStatus.PENDING
    attempts: int = 0
    error: Optional[str] = None
    created_at: datetime = field(default_factory=_utcnow)

    def increment_attempts(self) -> None:
        self.attempts += 1

    def mark_completed(self) -> None:
        self.status = TaskStatus.COMPLETED

    def mark_failed(self, error: str) -> None:
        self.status = TaskStatus.FAILED
        self.error = error
