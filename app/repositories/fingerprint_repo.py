# TODO: Fingerprint repository — enable when PostgreSQL + pgvector is ready

from __future__ import annotations

import logging
from typing import Any, Optional

from app.db.database import get_database
from app.core.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class FingerprintRepository:
    """Data access layer for fingerprint CRUD operations."""

    def __init__(self) -> None:
        self._db = get_database()

    async def save(
        self,
        fingerprint_id: str,
        user_id: str,
        finger_id: str,
        embedding: list[float],
        model_name: str = "default",
        image_path: str = "",
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        try:
            async with self._db.acquire() as conn:
                vector_str = "[" + ",".join(str(v) for v in embedding) + "]"
                await conn.execute(
                    """
                    INSERT INTO fingerprints
                        (fingerprint_id, user_id, finger_type, embedding, model_version, image_path)
                    VALUES ($1, $2, $3, $4::vector, $5, $6)
                    ON CONFLICT (fingerprint_id) DO UPDATE SET
                        embedding      = EXCLUDED.embedding,
                        model_version  = EXCLUDED.model_version,
                        image_path     = EXCLUDED.image_path
                    """,
                    fingerprint_id,
                    user_id,
                    finger_id,
                    vector_str,
                    model_name,
                    image_path,
                )
                logger.info("Saved fingerprint: %s (user=%s)", fingerprint_id, user_id)
                return fingerprint_id
        except Exception as exc:
            raise DatabaseError(f"Failed to save fingerprint: {exc}") from exc

    async def search_similar(
        self,
        query_vector: list[float],
        top_k: int = 5,
        threshold: float = 0.7,
    ) -> list[dict[str, Any]]:
        """Search for similar fingerprints using cosine distance (pgvector)."""
        try:
            async with self._db.acquire() as conn:
                vector_str = "[" + ",".join(str(v) for v in query_vector) + "]"
                rows = await conn.fetch(
                    """
                    SELECT
                        fingerprint_id,
                        user_id,
                        finger_id,
                        1 - (embedding <=> $1::vector) AS similarity
                    FROM fingerprints
                    WHERE 1 - (embedding <=> $1::vector) >= $2
                    ORDER BY similarity DESC
                    LIMIT $3
                    """,
                    vector_str,
                    threshold,
                    top_k,
                )
                return [dict(row) for row in rows]
        except Exception as exc:
            raise DatabaseError(f"Similarity search failed: {exc}") from exc

    async def get_by_id(self, fingerprint_id: str) -> Optional[dict[str, Any]]:
        async with self._db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM fingerprints WHERE fingerprint_id = $1",
                fingerprint_id,
            )
            return dict(row) if row else None

    async def delete(self, fingerprint_id: str) -> bool:
        async with self._db.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM fingerprints WHERE fingerprint_id = $1",
                fingerprint_id,
            )
            return result == "DELETE 1"


# ── Singleton ────────────────────────────────────────────────
_repo: Optional[FingerprintRepository] = None


def get_fingerprint_repo() -> FingerprintRepository:
    global _repo
    if _repo is None:
        _repo = FingerprintRepository()
    return _repo
