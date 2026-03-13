from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


# ── Registration ─────────────────────────────────────────────
class RegisterRequest(BaseModel):
    user_id: str = Field(..., description="User identifier")
    finger_id: str = Field(
        default="right_index",
        description="Finger name (e.g. right_index, left_thumb)",
    )
    image_base64: str = Field(..., description="Base64-encoded fingerprint image")
    metadata: dict[str, Any] = Field(default_factory=dict)


class RegisterResponse(BaseModel):
    success: bool
    message: str
    user_id: Optional[str] = None
    fingerprint_id: Optional[str] = None


# ── Verification ─────────────────────────────────────────────
class VerifyRequest(BaseModel):
    image_base64: str = Field(..., description="Base64-encoded fingerprint image")
    top_k: int = Field(default=5, description="Number of results to return")
    threshold: float = Field(default=0.7, description="Minimum similarity threshold")


class VerifyResponse(BaseModel):
    success: bool
    matched: bool = False
    message: str
    matches: list[MatchItem] = Field(default_factory=list)


class MatchItem(BaseModel):
    user_id: str
    finger_id: str
    similarity: float
    fingerprint_id: str


# Rebuild VerifyResponse to reference MatchItem after it's defined
VerifyResponse.model_rebuild()


# ── Health / Status ──────────────────────────────────────────
class HealthResponse(BaseModel):
    status: str = "ok"
    active_workers: int = 0
    mqtt_connected: bool = False
