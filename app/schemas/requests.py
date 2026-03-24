from __future__ import annotations

from typing import Any

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


# ── Verification ─────────────────────────────────────────────
class VerifyRequest(BaseModel):
    image_base64: str = Field(..., description="Base64-encoded fingerprint image")
    top_k: int = Field(default=5, description="Number of results to return")
    threshold: float = Field(default=0.7, description="Minimum similarity threshold")
