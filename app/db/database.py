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

            # Users table (unified with Worker schema)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id              SERIAL PRIMARY KEY,
                    user_id         VARCHAR(255) UNIQUE NOT NULL,
                    employee_id     VARCHAR(255) UNIQUE NOT NULL,
                    full_name       VARCHAR(255) NOT NULL DEFAULT '',
                    department      VARCHAR(255) NOT NULL DEFAULT '',
                    role            VARCHAR(50) NOT NULL DEFAULT 'user',
                    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
                    deleted_at      TIMESTAMP NULL,
                    created_at      TIMESTAMP DEFAULT NOW(),
                    updated_at      TIMESTAMP DEFAULT NOW()
                );
            """)

            # Migration: rename legacy columns if they exist
            for old_col, new_col, col_type in [
                ("username", "employee_id", "VARCHAR(255)"),
                ("name", "full_name", "VARCHAR(255)"),
            ]:
                old_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'users' AND column_name = $1
                    )
                """, old_col)
                if old_exists:
                    new_exists = await conn.fetchval("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = 'users' AND column_name = $1
                        )
                    """, new_col)
                    if not new_exists:
                        await conn.execute(
                            f"ALTER TABLE users RENAME COLUMN {old_col} TO {new_col};"
                        )
                        logger.info("Migrated users table: %s → %s", old_col, new_col)

            # Add missing columns from unified schema
            for col_name, col_def in [
                ("department", "VARCHAR(255) NOT NULL DEFAULT ''"),
                ("role", "VARCHAR(50) NOT NULL DEFAULT 'user'"),
                ("is_active", "BOOLEAN NOT NULL DEFAULT TRUE"),
                ("deleted_at", "TIMESTAMP NULL"),
            ]:
                col_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'users' AND column_name = $1
                    )
                """, col_name)
                if not col_exists:
                    await conn.execute(
                        f"ALTER TABLE users ADD COLUMN {col_name} {col_def};"
                    )
                    logger.info("Migrated users table: added %s", col_name)

            # Drop legacy metadata column if it exists
            meta_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'users' AND column_name = 'metadata'
                )
            """)
            if meta_exists:
                await conn.execute("ALTER TABLE users DROP COLUMN metadata;")
                logger.info("Migrated users table: dropped metadata column")

            # Fingerprints table (unified with Worker schema)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS fingerprints (
                    id              SERIAL PRIMARY KEY,
                    fingerprint_id  VARCHAR(255) UNIQUE NOT NULL,
                    user_id         VARCHAR(255) NOT NULL,
                    finger_index    INTEGER NOT NULL DEFAULT 1,
                    embedding       vector(256),
                    model_version   VARCHAR(255) DEFAULT 'default',
                    quality_score   REAL NOT NULL DEFAULT 0,
                    image_path      VARCHAR(512) DEFAULT '',
                    image_hash      VARCHAR(255) DEFAULT '',
                    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
                    deleted_at      TIMESTAMP NULL,
                    created_at      TIMESTAMP DEFAULT NOW()
                );
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_fingerprints_user
                ON fingerprints (user_id);
            """)

            finger_index_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'fingerprints' AND column_name = 'finger_index'
                )
            """)
            finger_id_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'fingerprints' AND column_name = 'finger_id'
                )
            """)
            finger_type_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'fingerprints' AND column_name = 'finger_type'
                )
            """)

            if not finger_index_exists:
                await conn.execute("""
                    ALTER TABLE fingerprints ADD COLUMN finger_index INTEGER;
                """)

                if finger_type_exists:
                    await conn.execute("""
                        UPDATE fingerprints
                        SET finger_index = CASE LOWER(COALESCE(finger_type, ''))
                            WHEN 'right_thumb' THEN 0
                            WHEN 'right_index' THEN 1
                            WHEN 'right_middle' THEN 2
                            WHEN 'right_ring' THEN 3
                            WHEN 'right_little' THEN 4
                            WHEN 'left_thumb' THEN 5
                            WHEN 'left_index' THEN 6
                            WHEN 'left_middle' THEN 7
                            WHEN 'left_ring' THEN 8
                            WHEN 'left_little' THEN 9
                            ELSE 1
                        END
                        WHERE finger_index IS NULL;
                    """)
                    logger.info("Migrated fingerprints: populated finger_index from finger_type")
                elif finger_id_exists:
                    await conn.execute("""
                        UPDATE fingerprints
                        SET finger_index = CASE
                            WHEN NULLIF(TRIM(finger_id), '') ~ '^[0-9]+$'
                                THEN LEAST(9, GREATEST(0, TRIM(finger_id)::INTEGER))
                            ELSE 1
                        END
                        WHERE finger_index IS NULL;
                    """)
                    logger.info("Migrated fingerprints: populated finger_index from finger_id")

                await conn.execute("""
                    UPDATE fingerprints
                    SET finger_index = 1
                    WHERE finger_index IS NULL;
                """)
                await conn.execute("""
                    ALTER TABLE fingerprints ALTER COLUMN finger_index SET DEFAULT 1;
                """)
                await conn.execute("""
                    ALTER TABLE fingerprints ALTER COLUMN finger_index SET NOT NULL;
                """)
                logger.info("Migrated fingerprints: added finger_index column")

            # Add missing columns
            for col_name, col_def in [
                ("image_path", "VARCHAR(512) DEFAULT ''"),
                ("image_hash", "VARCHAR(255) DEFAULT ''"),
                ("is_active", "BOOLEAN NOT NULL DEFAULT TRUE"),
                ("deleted_at", "TIMESTAMP NULL"),
                ("quality_score", "REAL NOT NULL DEFAULT 0"),
            ]:
                col_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'fingerprints' AND column_name = $1
                    )
                """, col_name)
                if not col_exists:
                    await conn.execute(
                        f"ALTER TABLE fingerprints ADD COLUMN {col_name} {col_def};"
                    )
                    logger.info("Migrated fingerprints: added %s", col_name)

            logger.info("Database schema ready.")


# ── Singleton ────────────────────────────────────────────────
_database: Optional[Database] = None


def get_database() -> Database:
    global _database
    if _database is None:
        _database = Database()
    return _database
