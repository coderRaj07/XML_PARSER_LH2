from dataclasses import dataclass, field
from uuid import uuid4


@dataclass
class Summary:
    id: str = field(default_factory=lambda: str(uuid4()))
    record_id: str = ""
    summary_text: str = ""
    summary_type: str = ""
    model_used: str = ""
