# TODO: Database connection — enable when PostgreSQL + pgvector is ready

from __future__ import annotations

import logging
from typing import Optional
from contextlib import asynccontextmanager

import asyncpg

from app.core.config import get_settings
from app.core.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class Database:
    """Manages the asyncpg connection pool."""

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


# ── Singleton ────────────────────────────────────────────────
_database: Optional[Database] = None


def get_database() -> Database:
    global _database
    if _database is None:
        _database = Database()
    return _database
