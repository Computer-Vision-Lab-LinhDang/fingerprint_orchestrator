from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from app.core.config import get_settings
from app.repositories.user_repo import get_user_repo
from app.repositories.fingerprint_repo import get_fingerprint_repo
from app.repositories.storage_repo import get_storage_repo

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["Users"])
settings = get_settings()


@router.get("/users")
async def list_users():
    repo = get_user_repo()
    return await repo.list_all()


@router.get("/users/{user_id}")
async def get_user(user_id: str):
    user_repo = get_user_repo()
    fp_repo = get_fingerprint_repo()

    user = await user_repo.find_by_id(user_id)
    if not user:
        raise HTTPException(404, "User not found")

    fingerprints = await fp_repo.list_by_user(user_id)
    return {"user": user, "fingerprints": fingerprints}


@router.delete("/users/{user_id}")
async def delete_user(user_id: str):
    user_repo = get_user_repo()
    fp_repo = get_fingerprint_repo()
    storage = get_storage_repo()

    employee_id = await user_repo.get_employee_id(user_id)
    image_paths = await fp_repo.get_image_paths_by_user(user_id)

    deleted_paths = set()
    for path in image_paths:
        try:
            storage._client.remove_object(settings.MINIO_BUCKET_IMAGES, path)
            deleted_paths.add(path)
        except Exception as exc:
            logger.warning("Failed to delete image %s: %s", path, exc)

    if employee_id:
        try:
            objects = storage._client.list_objects(
                settings.MINIO_BUCKET_IMAGES, prefix=f"{employee_id}_", recursive=True
            )
            for obj in objects:
                if obj.object_name not in deleted_paths:
                    storage._client.remove_object(settings.MINIO_BUCKET_IMAGES, obj.object_name)
        except Exception as exc:
            logger.warning("Failed to scan MinIO for user images: %s", exc)

    deleted = await user_repo.delete(user_id)
    if not deleted:
        raise HTTPException(404, "User not found")
    return {"status": "deleted", "user_id": user_id}
