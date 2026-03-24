from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


@dataclass
class Fingerprint:
    """Fingerprint entity — maps to the `fingerprints` table."""
    fingerprint_id: str
    user_id: str
    finger_id: str
    embedding: Optional[list[float]] = None
    model_name: str = "default"
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: Optional[datetime] = None
