from __future__ import annotations

import logging
from typing import Any, Optional

from app.core.exceptions import DatabaseError
from app.db.database import get_database

logger = logging.getLogger(__name__)

EMBEDDING_DIM = 256


class FingerprintRepository:
    """Data access layer for fingerprint CRUD operations."""

    def __init__(self) -> None:
        self._db = get_database()

    async def save(
        self,
        fingerprint_id: str,
        user_id: str,
        finger_index: int = 1,
        embedding: list[float] | None = None,
        model_version: str = "default",
        image_path: str = "",
        image_hash: str = "",
        quality_score: float = 0.0,
    ) -> str:
        try:
            async with self._db.acquire() as conn:
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
                    EMBEDDING_DIM,
                )
                if not isinstance(target_dim, int) or target_dim <= 0:
                    target_dim = EMBEDDING_DIM

                if embedding:
                    vector = list(embedding)
                    if len(vector) > target_dim:
                        vector = vector[:target_dim]
                    elif len(vector) < target_dim:
                        vector = vector + [0.0] * (target_dim - len(vector))
                    vector_str = "[" + ",".join(str(v) for v in vector) + "]"
                else:
                    vector_str = None

                await conn.execute(
                    """
                    INSERT INTO fingerprints
                        (fingerprint_id, user_id, finger_index, embedding,
                         model_version, image_path, image_hash, quality_score,
                         is_active, deleted_at)
                    VALUES ($1, $2, $3, $4::vector, $5, $6, $7, $8, TRUE, NULL)
                    ON CONFLICT (fingerprint_id) DO UPDATE SET
                        user_id         = EXCLUDED.user_id,
                        finger_index    = EXCLUDED.finger_index,
                        embedding       = EXCLUDED.embedding,
                        model_version   = EXCLUDED.model_version,
                        image_path      = EXCLUDED.image_path,
                        image_hash      = EXCLUDED.image_hash,
                        quality_score   = EXCLUDED.quality_score,
                        is_active       = TRUE,
                        deleted_at      = NULL
                    """,
                    fingerprint_id,
                    user_id,
                    finger_index,
                    vector_str,
                    model_version,
                    image_path,
                    image_hash,
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
        try:
            async with self._db.acquire() as conn:
                vector_str = "[" + ",".join(str(v) for v in query_vector) + "]"
                rows = await conn.fetch(
                    """
                    SELECT
                        fingerprint_id,
                        user_id,
                        finger_index,
                        1 - (embedding <=> $1::vector) AS similarity
                    FROM fingerprints
                    WHERE is_active = TRUE
                      AND 1 - (embedding <=> $1::vector) >= $2
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

    async def get_by_id(
        self, fingerprint_id: str, active_only: bool = False
    ) -> Optional[dict[str, Any]]:
        sql = "SELECT * FROM fingerprints WHERE fingerprint_id = $1"
        if active_only:
            sql += " AND is_active = TRUE"
        async with self._db.acquire() as conn:
            row = await conn.fetchrow(sql, fingerprint_id)
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

    async def soft_delete(self, fingerprint_id: str) -> bool:
        async with self._db.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE fingerprints
                SET is_active = FALSE, deleted_at = NOW()
                WHERE fingerprint_id = $1 AND is_active = TRUE
                """,
                fingerprint_id,
            )
        return result != "UPDATE 0"

    async def soft_delete_by_user(self, user_id: str) -> str:
        async with self._db.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE fingerprints
                SET is_active = FALSE, deleted_at = NOW()
                WHERE user_id = $1 AND is_active = TRUE
                """,
                user_id,
            )
        return result

    async def list_by_user(
        self, user_id: Optional[str] = None, active_only: bool = False
    ) -> list[dict[str, Any]]:
        conditions = []
        params = []

        if user_id:
            conditions.append("f.user_id = ${}".format(len(params) + 1))
            params.append(user_id)
        if active_only:
            conditions.append("f.is_active = TRUE")

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        async with self._db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT f.*, u.full_name as user_name
                FROM fingerprints f
                JOIN users u ON u.user_id = f.user_id
                {where_clause}
                ORDER BY f.created_at DESC
                """.format(where_clause=where_clause),
                *params
            )
        return [dict(r) for r in rows]

    async def get_image_paths_by_user(self, user_id: str) -> list[str]:
        async with self._db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT image_path
                FROM fingerprints
                WHERE user_id = $1 AND image_path != '' AND is_active = TRUE
                """,
                user_id,
            )
        return [row["image_path"] for row in rows]

    async def list_all_with_embeddings(self, active_only: bool = True) -> list[dict[str, Any]]:
        where_clause = "WHERE is_active = TRUE" if active_only else ""
        async with self._db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT fingerprint_id, user_id, finger_index,
                       embedding::text as embedding_text,
                       model_version, quality_score, image_path, image_hash,
                       is_active, deleted_at
                FROM fingerprints
                {where_clause}
                ORDER BY created_at
                """.format(where_clause=where_clause)
            )
        results = []
        for row in rows:
            data = dict(row)
            emb_text = data.pop("embedding_text", "")
            if emb_text:
                data["embedding"] = [float(v) for v in emb_text.strip("[]").split(",")]
            else:
                data["embedding"] = []
            results.append(data)
        return results


_repo: Optional[FingerprintRepository] = None


def get_fingerprint_repo() -> FingerprintRepository:
    global _repo
    if _repo is None:
        _repo = FingerprintRepository()
    return _repo
