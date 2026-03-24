from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


# ── Enums ────────────────────────────────────────────────────
class TaskType(str, Enum):
    EMBED = "embed"
    MATCH = "match"


class TaskStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class WorkerStatus(str, Enum):
    ONLINE = "online"
    IDLE = "idle"
    BUSY = "busy"
    OFFLINE = "offline"


# ── Outgoing payloads (Orchestrator → Worker) ────────────────
class TaskPayload(BaseModel):
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: TaskType
    image_url: str = Field(..., description="Fingerprint image URL on MinIO")
    model_name: str = Field(default="default", description="Embedding model name")
    extra: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class MatchPayload(BaseModel):
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: TaskType = TaskType.MATCH
    query_vector: list[float] = Field(..., description="Query embedding vector")
    candidate_vectors: list[list[float]] = Field(
        default_factory=list,
        description="Candidate vectors for direct comparison",
    )
    top_k: int = Field(default=5, description="Number of results to return")
    threshold: float = Field(default=0.7, description="Minimum similarity threshold")
    created_at: datetime = Field(default_factory=datetime.utcnow)


# ── Incoming payloads (Worker → Orchestrator) ────────────────
class TaskResult(BaseModel):
    task_id: str
    worker_id: str
    status: TaskStatus
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    processing_time_ms: Optional[float] = None
    completed_at: datetime = Field(default_factory=datetime.utcnow)


class EmbedResult(BaseModel):
    task_id: str
    worker_id: str
    status: TaskStatus
    vector: Optional[list[float]] = None
    model_name: str = ""
    error: Optional[str] = None
    processing_time_ms: Optional[float] = None


class MatchResult(BaseModel):
    task_id: str
    worker_id: str
    status: TaskStatus
    matches: list[dict[str, Any]] = Field(default_factory=list)
    error: Optional[str] = None
    processing_time_ms: Optional[float] = None


# ── Heartbeat ────────────────────────────────────────────────
class HeartbeatPayload(BaseModel):
    worker_id: str
    status: WorkerStatus
    gpu_memory_used_mb: Optional[float] = None
    gpu_memory_total_mb: Optional[float] = None
    current_task_id: Optional[str] = None
    uptime_seconds: Optional[float] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
