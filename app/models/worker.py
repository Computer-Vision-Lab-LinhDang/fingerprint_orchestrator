from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from app.schemas.mqtt_payloads import WorkerStatus


@dataclass
class WorkerInfo:
    """Represents the state of a connected GPU worker."""
    worker_id: str
    status: WorkerStatus = WorkerStatus.OFFLINE
    last_heartbeat: float = 0.0
    gpu_memory_used_mb: Optional[float] = None
    gpu_memory_total_mb: Optional[float] = None
    current_task_id: Optional[str] = None
    task_count: int = 0
    loaded_models: dict = field(default_factory=dict)  # {"embedding": "embedding_v1", ...}
