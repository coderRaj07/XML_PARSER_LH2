from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from uuid import uuid4


@dataclass
class Record:
    id: str = field(default_factory=lambda: str(uuid4()))
    task_id: str = ""
    title: str = ""
    author: str = ""
    published_date: Optional[datetime] = None
    description: str = ""
    content: str = ""
