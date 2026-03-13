# TODO: Database client — enable when PostgreSQL + pgvector is ready

from __future__ import annotations

import logging
from typing import Any, Optional
from contextlib import asynccontextmanager

import asyncpg

from app.core.config import get_settings
from app.core.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class DBClient:
    def __init__(self) -> None:
        self._pool: Optional[asyncpg.Pool] = None
        self._settings = get_settings()

    # ── Connection pool ──────────────────────────────────────
    async def connect(self) -> None:
        try:
            self._pool = await asyncpg.create_pool(
                host=self._settings.DB_HOST,
                port=self._settings.DB_PORT,
                user=self._settings.DB_USER,
                password=self._settings.DB_PASSWORD,
                database=self._settings.DB_NAME,
                min_size=2,
                max_size=10,
            )
            logger.info("Connected to database: %s", self._settings.DB_NAME)
        except Exception as exc:
            raise DatabaseError(f"Database connection failed: {exc}") from exc

    async def disconnect(self) -> None:
        if self._pool:
            await self._pool.close()
            logger.info("Disconnected from database.")

    @asynccontextmanager
    async def acquire(self):
        if not self._pool:
            raise DatabaseError("Database not connected. Call connect() first.")
        async with self._pool.acquire() as conn:
            yield conn

    # ── Init schema ──────────────────────────────────────────
    async def init_schema(self) -> None:
        async with self.acquire() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS fingerprints (
                    id              SERIAL PRIMARY KEY,
                    fingerprint_id  VARCHAR(255) UNIQUE NOT NULL,
                    user_id         VARCHAR(255) NOT NULL,
                    finger_id       VARCHAR(50)  NOT NULL,
                    embedding       vector(512),
                    model_name      VARCHAR(255) DEFAULT 'default',
                    metadata        JSONB DEFAULT '{}',
                    created_at      TIMESTAMP DEFAULT NOW()
                );
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_fingerprints_user
                ON fingerprints (user_id);
            """)
            logger.info("Database schema ready.")

    # ── CRUD Operations ──────────────────────────────────────
    async def save_fingerprint(
        self,
        fingerprint_id: str,
        user_id: str,
        finger_id: str,
        embedding: list[float],
        model_name: str = "default",
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        try:
            async with self.acquire() as conn:
                vector_str = "[" + ",".join(str(v) for v in embedding) + "]"
                await conn.execute(
                    """
                    INSERT INTO fingerprints
                        (fingerprint_id, user_id, finger_id, embedding, model_name, metadata)
                    VALUES ($1, $2, $3, $4::vector, $5, $6::jsonb)
                    ON CONFLICT (fingerprint_id) DO UPDATE SET
                        embedding  = EXCLUDED.embedding,
                        model_name = EXCLUDED.model_name,
                        metadata   = EXCLUDED.metadata
                    """,
                    fingerprint_id,
                    user_id,
                    finger_id,
                    vector_str,
                    model_name,
                    str(metadata or {}),
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
            async with self.acquire() as conn:
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

    async def get_fingerprint(self, fingerprint_id: str) -> Optional[dict[str, Any]]:
        async with self.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM fingerprints WHERE fingerprint_id = $1",
                fingerprint_id,
            )
            return dict(row) if row else None

    async def delete_fingerprint(self, fingerprint_id: str) -> bool:
        async with self.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM fingerprints WHERE fingerprint_id = $1",
                fingerprint_id,
            )
            return result == "DELETE 1"


# ── Singleton ────────────────────────────────────────────────
_db_client: Optional[DBClient] = None


def get_db_client() -> DBClient:
    global _db_client
    if _db_client is None:
        _db_client = DBClient()
    return _db_client
