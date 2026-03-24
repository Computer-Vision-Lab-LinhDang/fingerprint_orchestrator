from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


# ── Registration ─────────────────────────────────────────────
class RegisterResponse(BaseModel):
    success: bool
    message: str
    user_id: Optional[str] = None
    fingerprint_id: Optional[str] = None


# ── Verification ─────────────────────────────────────────────
class MatchItem(BaseModel):
    user_id: str
    finger_id: str
    similarity: float
    fingerprint_id: str


class VerifyResponse(BaseModel):
    success: bool
    matched: bool = False
    message: str
    matches: list[MatchItem] = Field(default_factory=list)


# ── Health / Status ──────────────────────────────────────────
class HealthResponse(BaseModel):
    status: str = "ok"
    active_workers: int = 0
    mqtt_connected: bool = False
