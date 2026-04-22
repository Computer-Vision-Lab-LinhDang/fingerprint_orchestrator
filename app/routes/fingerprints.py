from __future__ import annotations

import io
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.core.config import get_settings
from app.mqtt.handlers import broadcast_fingerprint_deleted
from app.repositories.fingerprint_repo import get_fingerprint_repo
from app.repositories.storage_repo import get_storage_repo
from app.repositories.user_repo import get_user_repo
from app.schemas.mqtt_payloads import FingerprintDeletedEvent

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["Fingerprints"])
settings = get_settings()


@router.get("/fingerprints")
async def list_fingerprints(user_id: Optional[str] = Query(None)):
    repo = get_fingerprint_repo()
    return await repo.list_by_user(user_id)


@router.delete("/fingerprints/{fingerprint_id}")
async def delete_fingerprint(fingerprint_id: str):
    repo = get_fingerprint_repo()
    storage = get_storage_repo()
    user_repo = get_user_repo()

    fp = await repo.get_by_id(fingerprint_id)
    if not fp:
        raise HTTPException(404, "Fingerprint not found")

    user = await user_repo.find_by_id(fp["user_id"])

    if fp.get("image_path"):
        try:
            storage._client.remove_object(settings.MINIO_BUCKET_IMAGES, fp["image_path"])
        except Exception as exc:
            logger.warning("Failed to delete image %s: %s", fp["image_path"], exc)

    deleted = await repo.soft_delete(fingerprint_id)
    if not deleted:
        raise HTTPException(404, "Fingerprint not found")
    try:
        await broadcast_fingerprint_deleted(
            FingerprintDeletedEvent(
                fingerprint_id=fingerprint_id,
                user_id=fp["user_id"],
                employee_id=(user or {}).get("employee_id", ""),
                finger_index=int(fp.get("finger_index", 0) or 0),
            )
        )
    except Exception as exc:
        logger.warning("Failed to broadcast fingerprint delete for %s: %s", fingerprint_id, exc)
    return {"status": "deleted", "fingerprint_id": fingerprint_id}


@router.get("/fingerprints/{fingerprint_id}/image")
async def get_fingerprint_image(fingerprint_id: str):
    repo = get_fingerprint_repo()
    fp = await repo.get_by_id(fingerprint_id)
    if not fp or not fp.get("image_path"):
        raise HTTPException(404, "Image not found")

    storage = get_storage_repo()
    url = storage.get_presigned_url(fp["image_path"])
    return {"url": url, "image_path": fp["image_path"]}


@router.get("/images/proxy")
async def image_proxy(path: str = Query(...)):
    storage = get_storage_repo()
    try:
        response = storage._client.get_object(settings.MINIO_BUCKET_IMAGES, path)
        image_bytes = response.read()
        response.close()
        response.release_conn()

        ext = path.lower().rsplit(".", 1)[-1] if "." in path else ""

        if ext == "png":
            return StreamingResponse(io.BytesIO(image_bytes), media_type="image/png")
        if ext in ("jpg", "jpeg"):
            return StreamingResponse(io.BytesIO(image_bytes), media_type="image/jpeg")

        from PIL import Image
        img = Image.open(io.BytesIO(image_bytes))
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)
        return StreamingResponse(buf, media_type="image/png")
    except Exception as exc:
        raise HTTPException(404, f"Image not found: {exc}")


@router.get("/images/list")
async def list_images(user_id: Optional[str] = Query(None)):
    storage = get_storage_repo()
    try:
        objects = storage._client.list_objects(
            settings.MINIO_BUCKET_IMAGES, prefix=user_id or "", recursive=True
        )
        return [
            {
                "name": obj.object_name,
                "size": obj.size,
                "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
            }
            for obj in objects
        ]
    except Exception as exc:
        raise HTTPException(500, f"MinIO error: {exc}")
