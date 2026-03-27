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

            # Users table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id              SERIAL PRIMARY KEY,
                    user_id         VARCHAR(255) UNIQUE NOT NULL,
                    username        VARCHAR(255) UNIQUE NOT NULL,
                    name            VARCHAR(255) NOT NULL DEFAULT '',
                    metadata        JSONB DEFAULT '{}',
                    created_at      TIMESTAMP DEFAULT NOW(),
                    updated_at      TIMESTAMP DEFAULT NOW()
                );
            """)

            # Migration: add username column if it doesn't exist (for existing DBs)
            col_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'users' AND column_name = 'username'
                )
            """)
            if not col_exists:
                await conn.execute("""
                    ALTER TABLE users ADD COLUMN username VARCHAR(255) UNIQUE;
                """)
                # Backfill: set username = user_id for existing rows
                await conn.execute("""
                    UPDATE users SET username = user_id WHERE username IS NULL;
                """)
                await conn.execute("""
                    ALTER TABLE users ALTER COLUMN username SET NOT NULL;
                """)
                logger.info("Migrated users table: added username column")

            # Fingerprints table
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

            # Migration: add image_path column if missing
            img_col_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'fingerprints' AND column_name = 'image_path'
                )
            """)
            if not img_col_exists:
                await conn.execute("""
                    ALTER TABLE fingerprints ADD COLUMN image_path VARCHAR(512) DEFAULT '';
                """)
                logger.info("Migrated fingerprints table: added image_path column")

            logger.info("Database schema ready.")


# ── Singleton ────────────────────────────────────────────────
_database: Optional[Database] = None


def get_database() -> Database:
    global _database
    if _database is None:
        _database = Database()
    return _database
