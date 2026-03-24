from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from app.schemas.mqtt_payloads import TaskStatus, TaskType


@dataclass
class Task:
    """Represents a processing task dispatched to a worker."""
    task_id: str
    task_type: TaskType
    worker_id: Optional[str] = None
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    processing_time_ms: Optional[float] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
