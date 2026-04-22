from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


@dataclass
class Fingerprint:
    """Fingerprint entity — maps to the `fingerprints` table."""
    fingerprint_id: str
    user_id: str
    finger_index: int = 1
    embedding: Optional[list[float]] = None
    model_version: str = "default"
    quality_score: float = 0.0
    image_path: str = ""
    image_hash: str = ""
    is_active: bool = True
    created_at: Optional[datetime] = None
