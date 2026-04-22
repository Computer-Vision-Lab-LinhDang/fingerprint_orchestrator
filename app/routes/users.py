from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from app.mqtt.handlers import broadcast_user_deleted
from app.repositories.user_repo import get_user_repo
from app.repositories.fingerprint_repo import get_fingerprint_repo
from app.schemas.mqtt_payloads import UserDeletedEvent

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["Users"])


def _serialize_user(user: dict) -> dict:
    employee_id = user.get("employee_id", "") or ""
    full_name = user.get("full_name", "") or ""
    return {
        **user,
        # Backward-compatible aliases for dashboard/frontend code that still
        # expects the legacy field names.
        "username": employee_id,
        "name": full_name,
        "metadata": user.get("metadata", {}) or {},
    }


@router.get("/users")
async def list_users():
    repo = get_user_repo()
    users = await repo.list_all()
    return [_serialize_user(user) for user in users]


@router.get("/users/{user_id}")
async def get_user(user_id: str):
    user_repo = get_user_repo()
    fp_repo = get_fingerprint_repo()

    user = await user_repo.find_by_id(user_id)
    if not user:
        raise HTTPException(404, "User not found")

    fingerprints = await fp_repo.list_by_user(user_id)
    return {"user": _serialize_user(user), "fingerprints": fingerprints}


@router.delete("/users/{user_id}")
async def delete_user(user_id: str):
    user_repo = get_user_repo()
    fp_repo = get_fingerprint_repo()
    user = await user_repo.find_by_id(user_id)
    if not user:
        raise HTTPException(404, "User not found")

    employee_id = user.get("employee_id", "")
    deleted = await user_repo.soft_delete(user_id)
    if not deleted:
        raise HTTPException(404, "User not found")
    await fp_repo.soft_delete_by_user(user_id)

    try:
        await broadcast_user_deleted(
            UserDeletedEvent(
                user_id=user_id,
                employee_id=employee_id,
                full_name=user.get("full_name", ""),
            )
        )
    except Exception as exc:
        logger.warning("Failed to broadcast user delete for %s: %s", user_id, exc)
    return {"status": "deleted", "user_id": user_id}
