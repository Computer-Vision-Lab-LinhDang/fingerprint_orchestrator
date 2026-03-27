"""
Dashboard API routes — CRUD for users, fingerprints, workers, models, logs.
"""

from __future__ import annotations

import logging
import time
from datetime import timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.core.config import get_settings
from app.db.database import get_database
from app.repositories.storage_repo import get_storage_repo
from app.services.worker_service import get_worker_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["Dashboard"])
settings = get_settings()


# ── Stats ────────────────────────────────────────────────────
@router.get("/stats")
async def get_stats():
    db = get_database()
    worker_svc = get_worker_service()

    async with db.acquire() as conn:
        user_count = await conn.fetchval("SELECT COUNT(*) FROM users")
        fp_count = await conn.fetchval("SELECT COUNT(*) FROM fingerprints")

    return {
        "users": user_count,
        "fingerprints": fp_count,
        "workers_online": worker_svc.active_count,
        "workers_total": len(worker_svc.workers),
    }


# ── Users ────────────────────────────────────────────────────
@router.get("/users")
async def list_users():
    db = get_database()
    async with db.acquire() as conn:
        rows = await conn.fetch("""
            SELECT u.*, COUNT(f.id) as fingerprint_count
            FROM users u
            LEFT JOIN fingerprints f ON f.user_id = u.user_id
            GROUP BY u.id
            ORDER BY u.created_at DESC
        """)
    return [dict(r) for r in rows]


@router.get("/users/{user_id}")
async def get_user(user_id: str):
    db = get_database()
    async with db.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT * FROM users WHERE user_id = $1", user_id
        )
        if not user:
            raise HTTPException(404, "User not found")

        fingerprints = await conn.fetch(
            "SELECT * FROM fingerprints WHERE user_id = $1 ORDER BY created_at DESC",
            user_id,
        )

    return {
        "user": dict(user),
        "fingerprints": [dict(f) for f in fingerprints],
    }


@router.delete("/users/{user_id}")
async def delete_user(user_id: str):
    db = get_database()
    async with db.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM users WHERE user_id = $1", user_id
        )
    if result == "DELETE 0":
        raise HTTPException(404, "User not found")
    return {"status": "deleted", "user_id": user_id}


# ── Fingerprints ─────────────────────────────────────────────
@router.get("/fingerprints")
async def list_fingerprints(
    user_id: Optional[str] = Query(None),
):
    db = get_database()
    async with db.acquire() as conn:
        if user_id:
            rows = await conn.fetch(
                """SELECT f.*, u.name as user_name
                   FROM fingerprints f
                   JOIN users u ON u.user_id = f.user_id
                   WHERE f.user_id = $1
                   ORDER BY f.created_at DESC""",
                user_id,
            )
        else:
            rows = await conn.fetch(
                """SELECT f.*, u.name as user_name
                   FROM fingerprints f
                   JOIN users u ON u.user_id = f.user_id
                   ORDER BY f.created_at DESC"""
            )
    return [dict(r) for r in rows]


@router.delete("/fingerprints/{fingerprint_id}")
async def delete_fingerprint(fingerprint_id: str):
    db = get_database()
    async with db.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM fingerprints WHERE fingerprint_id = $1", fingerprint_id
        )
    if result == "DELETE 0":
        raise HTTPException(404, "Fingerprint not found")
    return {"status": "deleted", "fingerprint_id": fingerprint_id}


@router.get("/fingerprints/{fingerprint_id}/image")
async def get_fingerprint_image(fingerprint_id: str):
    """Get presigned URL for fingerprint image."""
    db = get_database()
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT image_path FROM fingerprints WHERE fingerprint_id = $1",
            fingerprint_id,
        )
    if not row or not row["image_path"]:
        raise HTTPException(404, "Image not found")

    storage = get_storage_repo()
    url = storage.get_presigned_url(row["image_path"])
    return {"url": url, "image_path": row["image_path"]}


# ── Workers ──────────────────────────────────────────────────
@router.get("/workers")
async def list_workers():
    service = get_worker_service()
    return service.get_summary()


# ── Models (MinIO) ───────────────────────────────────────────
@router.get("/models")
async def list_models():
    """List models from DB."""
    db = get_database()
    async with db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM models ORDER BY created_at DESC"
        )
    return [dict(r) for r in rows]


@router.get("/models/minio")
async def list_minio_models(prefix: str = Query("", description="Folder prefix")):
    """List model files/folders from MinIO bucket (folder-style browsing)."""
    storage = get_storage_repo()
    try:
        # First try non-recursive (folder-style) listing
        objects = list(storage._client.list_objects(
            settings.MINIO_BUCKET_MODELS,
            prefix=prefix or "",
            recursive=False,
        ))

        folders = []
        files = []

        if objects:
            for obj in objects:
                if obj.is_dir:
                    folders.append({"name": obj.object_name, "type": "folder"})
                else:
                    files.append({
                        "name": obj.object_name,
                        "type": "file",
                        "size": obj.size,
                        "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
                    })
        else:
            # Fallback: recursive listing and build folder structure from paths
            all_objects = storage._client.list_objects(
                settings.MINIO_BUCKET_MODELS, prefix=prefix or "", recursive=True,
            )
            seen_folders = set()
            for obj in all_objects:
                name = obj.object_name
                # Strip the current prefix
                relative = name[len(prefix):] if prefix else name
                if "/" in relative:
                    # This is inside a subfolder
                    folder_name = prefix + relative.split("/")[0] + "/"
                    if folder_name not in seen_folders:
                        seen_folders.add(folder_name)
                        folders.append({"name": folder_name, "type": "folder"})
                else:
                    files.append({
                        "name": obj.object_name,
                        "type": "file",
                        "size": obj.size,
                        "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
                    })

        return {"prefix": prefix, "folders": folders, "files": files}
    except Exception as exc:
        logger.error("MinIO listing error: %s", exc)
        raise HTTPException(500, f"MinIO error: {exc}")


@router.post("/models/deploy")
async def deploy_model(body: dict):
    """Deploy a model from MinIO to a worker via MQTT."""
    import json
    from datetime import timedelta

    worker_id = body.get("worker_id")
    s3_path = body.get("s3_path")  # e.g. "embedding/embedding_v2.onnx"
    model_type = body.get("model_type", "embedding")

    if not worker_id or not s3_path:
        raise HTTPException(400, "worker_id and s3_path required")

    # Get worker
    worker_svc = get_worker_service()
    if worker_id not in worker_svc.workers:
        raise HTTPException(404, f"Worker '{worker_id}' not found")

    # Create presigned URL for worker to download
    storage = get_storage_repo()
    if settings.MINIO_PUBLIC_ENDPOINT:
        from minio import Minio
        public_client = Minio(
            endpoint=settings.MINIO_PUBLIC_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
        )
        download_url = public_client.presigned_get_object(
            settings.MINIO_BUCKET_MODELS, s3_path, expires=timedelta(hours=2)
        )
    else:
        download_url = storage._client.presigned_get_object(
            settings.MINIO_BUCKET_MODELS, s3_path, expires=timedelta(hours=2)
        )

    model_name = s3_path.rsplit("/", 1)[-1] if "/" in s3_path else s3_path

    # Publish MQTT command
    import paho.mqtt.client as mqtt_client
    client = mqtt_client.Client(client_id="dashboard-deploy", protocol=mqtt_client.MQTTv311)
    client.connect(settings.MQTT_BROKER_HOST, settings.MQTT_BROKER_PORT, 10)

    payload = json.dumps({
        "model_type": model_type,
        "model_name": model_name,
        "version": "latest",
        "download_url": download_url,
        "s3_path": s3_path,
    })

    topic = f"task/{worker_id}/model/update"
    result = client.publish(topic, payload, qos=1)
    client.disconnect()

    if result.rc == 0:
        logger.info("Deploy model '%s' to worker '%s'", s3_path, worker_id)
        return {"status": "sent", "worker_id": worker_id, "model": model_name}
    else:
        raise HTTPException(500, "Failed to send deploy command")


# ── Logs ─────────────────────────────────────────────────────
# In-memory log buffer for dashboard
_log_buffer: list[dict] = []
_MAX_LOGS = 200


def add_log(level: str, source: str, message: str):
    """Add a log entry to the in-memory buffer."""
    _log_buffer.append({
        "timestamp": time.time(),
        "level": level,
        "source": source,
        "message": message,
    })
    if len(_log_buffer) > _MAX_LOGS:
        _log_buffer.pop(0)


@router.get("/logs")
async def get_logs(limit: int = Query(50, le=200)):
    return _log_buffer[-limit:]


# ── Image Proxy (supports .tif, .png, .jpeg) ─────────────────
@router.get("/images/proxy")
async def image_proxy(path: str = Query(...)):
    """Download image from MinIO. Convert .tif to PNG, pass-through others."""
    import io
    from fastapi.responses import StreamingResponse

    storage = get_storage_repo()
    try:
        response = storage._client.get_object(
            settings.MINIO_BUCKET_IMAGES, path
        )
        image_bytes = response.read()
        response.close()
        response.release_conn()

        ext = path.lower().rsplit(".", 1)[-1] if "." in path else ""

        # PNG/JPEG: pass-through directly (browser-native)
        if ext in ("png",):
            return StreamingResponse(io.BytesIO(image_bytes), media_type="image/png")
        if ext in ("jpg", "jpeg"):
            return StreamingResponse(io.BytesIO(image_bytes), media_type="image/jpeg")

        # TIF/BMP/others: convert to PNG via Pillow
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
    """List fingerprint images from MinIO, optionally filtered by user."""
    storage = get_storage_repo()
    try:
        objects = storage._client.list_objects(
            settings.MINIO_BUCKET_IMAGES, prefix=user_id or "", recursive=True
        )
        result = []
        for obj in objects:
            result.append({
                "name": obj.object_name,
                "size": obj.size,
                "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
            })
        return result
    except Exception as exc:
        raise HTTPException(500, f"MinIO error: {exc}")

