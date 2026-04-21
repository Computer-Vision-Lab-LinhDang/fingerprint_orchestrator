from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


# ── Enums ────────────────────────────────────────────────────
# Note: We use model_config to disable protected namespace warnings
# because our domain uses "model_type" and "model_name" fields.
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
    model_config = {"protected_namespaces": ()}
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: TaskType
    image_url: str = Field(..., description="Fingerprint image URL on MinIO")
    model_name: str = Field(default="default", description="Embedding model name")
    extra: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class ModelUpdateCommand(BaseModel):
    """Orchestrator → Worker: command to download a new model."""
    model_config = {"protected_namespaces": ()}
    model_type: str = Field(..., description="embedding, matching, or pad")
    model_name: str = Field(..., description="e.g. embedding_v1")
    version: str = Field(..., description="e.g. v1")
    download_url: str = Field(..., description="Presigned URL to download model")
    s3_path: str = Field(default="", description="Original S3 path")
    relative_path: str = Field(
        default="",
        description="Path inside the model type folder, e.g. embedding_v1/model.onnx",
    )


class ModelStatusReport(BaseModel):
    """Worker → Orchestrator: model download status report."""
    model_config = {"protected_namespaces": ()}
    worker_id: str
    model_type: str
    model_name: str
    version: str
    status: str  # "downloading", "ready", "failed"
    error: Optional[str] = None


class EnrollmentUploadCommand(BaseModel):
    """Orchestrator → Worker: upload a cached enrollment image to MinIO."""

    fp_id: int
    fingerprint_id: str
    object_name: str
    upload_url: str
    content_type: str = "image/tiff"


class EnrollmentUploadStatus(BaseModel):
    """Worker → Orchestrator: status of a presigned image upload."""

    worker_id: str
    fp_id: int
    fingerprint_id: str
    object_name: str = ""
    status: str


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
    loaded_models: dict[str, str] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
