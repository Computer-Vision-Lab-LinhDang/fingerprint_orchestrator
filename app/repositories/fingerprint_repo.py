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
        quality_score: float = 0.0,
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        try:
            async with self._db.acquire() as conn:
                # Adapt vector size to the actual pgvector dimension in DB.
                target_dim = await conn.fetchval(
                    """
                    SELECT COALESCE(
                        (regexp_match(format_type(a.atttypid, a.atttypmod), 'vector\\((\\d+)\\)'))[1]::int,
                        $1
                    )
                    FROM pg_attribute a
                    JOIN pg_class c ON c.oid = a.attrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = 'fingerprints'
                      AND a.attname = 'embedding'
                      AND n.nspname = current_schema()
                    LIMIT 1
                    """,
                    len(embedding),
                )
                if not isinstance(target_dim, int) or target_dim <= 0:
                    target_dim = len(embedding)

                vector = list(embedding)
                if len(vector) > target_dim:
                    vector = vector[:target_dim]
                elif len(vector) < target_dim:
                    vector = vector + [0.0] * (target_dim - len(vector))

                vector_str = "[" + ",".join(str(v) for v in vector) + "]"
                await conn.execute(
                    """
                    INSERT INTO fingerprints
                        (fingerprint_id, user_id, finger_type, embedding, model_version, image_path, quality_score)
                    VALUES ($1, $2, $3, $4::vector, $5, $6, $7)
                    ON CONFLICT (fingerprint_id) DO UPDATE SET
                        embedding      = EXCLUDED.embedding,
                        model_version  = EXCLUDED.model_version,
                        image_path     = EXCLUDED.image_path,
                        quality_score  = EXCLUDED.quality_score
                    """,
                    fingerprint_id,
                    user_id,
                    finger_id,
                    vector_str,
                    model_name,
                    image_path,
                    quality_score,
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

    async def update_image_path(self, fingerprint_id: str, image_path: str) -> bool:
        async with self._db.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE fingerprints
                SET image_path = $2
                WHERE fingerprint_id = $1
                """,
                fingerprint_id,
                image_path,
            )
        return result != "UPDATE 0"

    async def delete(self, fingerprint_id: str) -> bool:
        async with self._db.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM fingerprints WHERE fingerprint_id = $1",
                fingerprint_id,
            )
            return result == "DELETE 1"

    async def list_by_user(self, user_id: str = None) -> list[dict[str, Any]]:
        async with self._db.acquire() as conn:
            if user_id:
                rows = await conn.fetch(
                    """SELECT f.*, u.name as user_name
                       FROM fingerprints f
                       JOIN users u ON u.user_id = f.user_id
                       WHERE f.user_id = $1
                       ORDER BY f.created_at DESC""",
                    user_id,
                )
            else:
                rows = await conn.fetch(
                    """SELECT f.*, u.name as user_name
                       FROM fingerprints f
                       JOIN users u ON u.user_id = f.user_id
                       ORDER BY f.created_at DESC"""
                )
        return [dict(r) for r in rows]

    async def get_image_paths_by_user(self, user_id: str) -> list[str]:
        async with self._db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT image_path FROM fingerprints WHERE user_id = $1 AND image_path != ''",
                user_id,
            )
        return [row["image_path"] for row in rows]


# ── Singleton ────────────────────────────────────────────────
_repo: Optional[FingerprintRepository] = None


def get_fingerprint_repo() -> FingerprintRepository:
    global _repo
    if _repo is None:
        _repo = FingerprintRepository()
    return _repo
