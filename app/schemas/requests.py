from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


# ── Registration ─────────────────────────────────────────────
class RegisterRequest(BaseModel):
    employee_id: str = Field(..., description="Employee identifier")
    full_name: str = Field(default="", description="Full name of the user")
    department: str = Field(default="", description="Department")
    finger_index: int = Field(
        default=1,
        description="Finger index (0-9, matches Worker convention)",
    )
    image_base64: str = Field(..., description="Base64-encoded fingerprint image")


# ── Verification ─────────────────────────────────────────────
class VerifyRequest(BaseModel):
    image_base64: str = Field(..., description="Base64-encoded fingerprint image")
    top_k: int = Field(default=5, description="Number of results to return")
    threshold: float = Field(default=0.7, description="Minimum similarity threshold")
