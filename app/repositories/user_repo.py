from __future__ import annotations

import logging
from typing import Any, Optional

from app.core.exceptions import DatabaseError
from app.db.database import get_database

logger = logging.getLogger(__name__)


class UserRepository:
    def __init__(self) -> None:
        self._db = get_database()

    async def list_all(self, active_only: bool = False) -> list[dict[str, Any]]:
        where_clause = "WHERE u.is_active = TRUE" if active_only else ""
        async with self._db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    u.*,
                    COUNT(f.id) FILTER (WHERE f.is_active = TRUE) AS fingerprint_count
                FROM users u
                LEFT JOIN fingerprints f ON f.user_id = u.user_id
                {where_clause}
                GROUP BY u.id
                ORDER BY u.created_at DESC
                """.format(where_clause=where_clause)
            )
        return [dict(r) for r in rows]

    async def find_by_id(
        self, user_id: str, active_only: bool = False
    ) -> Optional[dict[str, Any]]:
        sql = "SELECT * FROM users WHERE user_id = $1"
        if active_only:
            sql += " AND is_active = TRUE"
        async with self._db.acquire() as conn:
            row = await conn.fetchrow(sql, user_id)
        return dict(row) if row else None

    async def find_by_employee_id(
        self, employee_id: str, active_only: bool = False
    ) -> Optional[dict[str, Any]]:
        sql = "SELECT * FROM users WHERE employee_id = $1"
        if active_only:
            sql += " AND is_active = TRUE"
        async with self._db.acquire() as conn:
            row = await conn.fetchrow(sql, employee_id)
        return dict(row) if row else None

    async def create(
        self,
        user_id: str,
        employee_id: str,
        full_name: str,
        department: str = "",
        role: str = "user",
    ) -> str:
        try:
            async with self._db.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO users (
                        user_id, employee_id, full_name, department, role,
                        is_active, deleted_at, updated_at
                    )
                    VALUES ($1, $2, $3, $4, $5, TRUE, NULL, NOW())
                    """,
                    user_id,
                    employee_id,
                    full_name,
                    department,
                    role,
                )
            logger.info(
                "Created user: %s (employee_id=%s, id=%s)",
                full_name,
                employee_id,
                user_id,
            )
            return user_id
        except Exception as exc:
            raise DatabaseError(f"Failed to create user: {exc}") from exc

    async def reactivate(
        self,
        user_id: str,
        full_name: Optional[str] = None,
        department: Optional[str] = None,
        role: Optional[str] = None,
    ) -> bool:
        async with self._db.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE users
                SET
                    full_name = COALESCE($2, full_name),
                    department = COALESCE($3, department),
                    role = COALESCE($4, role),
                    is_active = TRUE,
                    deleted_at = NULL,
                    updated_at = NOW()
                WHERE user_id = $1
                """,
                user_id,
                full_name,
                department,
                role,
            )
        return result != "UPDATE 0"

    async def soft_delete(self, user_id: str) -> bool:
        async with self._db.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE users
                SET is_active = FALSE, deleted_at = NOW(), updated_at = NOW()
                WHERE user_id = $1 AND is_active = TRUE
                """,
                user_id,
            )
        return result != "UPDATE 0"

    async def get_employee_id(self, user_id: str) -> Optional[str]:
        async with self._db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT employee_id FROM users WHERE user_id = $1", user_id
            )
        return row["employee_id"] if row else None

    find_by_username = find_by_employee_id
    get_username = get_employee_id


_repo: Optional[UserRepository] = None


def get_user_repo() -> UserRepository:
    global _repo
    if _repo is None:
        _repo = UserRepository()
    return _repo
