from __future__ import annotations

import logging
from typing import Any, Optional

from app.db.database import get_database
from app.core.exceptions import DatabaseError

logger = logging.getLogger(__name__)


class UserRepository:

    def __init__(self) -> None:
        self._db = get_database()

    async def list_all(self) -> list[dict[str, Any]]:
        async with self._db.acquire() as conn:
            rows = await conn.fetch("""
                SELECT u.*, COUNT(f.id) as fingerprint_count
                FROM users u
                LEFT JOIN fingerprints f ON f.user_id = u.user_id
                GROUP BY u.id
                ORDER BY u.created_at DESC
            """)
        return [dict(r) for r in rows]

    async def find_by_id(self, user_id: str) -> Optional[dict[str, Any]]:
        async with self._db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM users WHERE user_id = $1", user_id
            )
        return dict(row) if row else None

    async def find_by_employee_id(self, employee_id: str) -> Optional[dict[str, Any]]:
        async with self._db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM users WHERE employee_id = $1", employee_id
            )
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
                    INSERT INTO users (user_id, employee_id, full_name, department, role)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    user_id, employee_id, full_name, department, role,
                )
            logger.info(
                "Created user: %s (employee_id=%s, id=%s)",
                full_name, employee_id, user_id,
            )
            return user_id
        except Exception as exc:
            raise DatabaseError(f"Failed to create user: {exc}") from exc

    async def delete(self, user_id: str) -> bool:
        async with self._db.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM users WHERE user_id = $1", user_id
            )
        return result != "DELETE 0"

    async def get_employee_id(self, user_id: str) -> Optional[str]:
        async with self._db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT employee_id FROM users WHERE user_id = $1", user_id
            )
        return row["employee_id"] if row else None

    # Legacy alias for code that still uses the old name
    find_by_username = find_by_employee_id
    get_username = get_employee_id


_repo: Optional[UserRepository] = None


def get_user_repo() -> UserRepository:
    global _repo
    if _repo is None:
        _repo = UserRepository()
    return _repo
