from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timedelta
from typing import Optional

import aiomqtt

from app.repositories.fingerprint_repo import get_fingerprint_repo
from app.repositories.storage_repo import get_storage_repo
from app.repositories.user_repo import get_user_repo
from app.services.worker_service import get_worker_service
from app.mqtt.publisher import get_publisher
from app.schemas.mqtt_payloads import TaskPayload, TaskType
from app.core.config import get_settings

logger = logging.getLogger(__name__)

_pending_registrations: dict = {}


def get_pending_registrations() -> dict:
    return _pending_registrations


async def register_fingerprint(
    client: aiomqtt.Client,
    username: str,
    fullname: str,
    image_base64: str,
    finger_type: str = "right_thumb",
    image_filename: str = "",
    content_type: str = "image/tiff",
) -> dict:
    storage = get_storage_repo()
    worker_svc = get_worker_service()
    publisher = get_publisher()
    settings = get_settings()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if image_filename:
        ext = image_filename.rsplit(".", 1)[-1] if "." in image_filename else "tif"
        object_name = f"{username}_{finger_type}_{timestamp}.{ext}"
    else:
        object_name = f"{username}_{timestamp}.tif"

    storage.upload_image(
        filename=object_name,
        image_base64=image_base64,
        content_type=content_type,
    )

    if settings.MINIO_PUBLIC_ENDPOINT:
        from minio import Minio
        public_client = Minio(
            endpoint=settings.MINIO_PUBLIC_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
        )
        image_url = public_client.presigned_get_object(
            settings.MINIO_BUCKET_IMAGES,
            object_name,
            expires=timedelta(hours=1),
        )
    else:
        image_url = storage.get_presigned_url(object_name)

    worker_id = worker_svc.select_idle_worker(task_type="embed")

    task_id = str(uuid.uuid4())
    payload = TaskPayload(
        task_id=task_id,
        task_type=TaskType.EMBED,
        image_url=image_url,
        extra={
            "username": username,
            "fullname": fullname,
            "finger_type": finger_type,
            "image_path": object_name,
        },
    )

    _pending_registrations[task_id] = {
        "task_id": task_id,
        "username": username,
        "fullname": fullname,
        "finger_type": finger_type,
        "image_path": object_name,
        "worker_id": worker_id,
        "status": "processing",
        "created_at": datetime.now().isoformat(),
    }

    await publisher.send_embed_task(client, worker_id, payload)
    worker_svc.mark_busy(worker_id, task_id)

    logger.info(
        "Registration task '%s' → worker '%s' for user '%s'",
        task_id, worker_id, username,
    )

    return {
        "task_id": task_id,
        "username": username,
        "fullname": fullname,
        "worker_id": worker_id,
        "status": "processing",
        "message": "Task dispatched to worker. Await result.",
    }


async def handle_embed_result(task_id: str, result: dict) -> Optional[dict]:
    registration = _pending_registrations.pop(task_id, None)
    if not registration:
        logger.warning("No pending registration for task '%s'", task_id)
        return None

    status = result.get("status", "unknown")
    if status != "completed":
        error = result.get("error", "Unknown error")
        logger.error("Registration task '%s' failed: %s", task_id, error)
        return {"task_id": task_id, "status": "failed", "error": error}

    result_data = result.get("result", {})
    vector = result_data.get("vector", [])
    vector_dim = result_data.get("vector_dim", 0)
    model_name = result_data.get("model_name", "")
    processing_time_ms = result_data.get("processing_time_ms", 0)

    if not vector:
        logger.error("Empty vector for task '%s'", task_id)
        return {"task_id": task_id, "status": "failed", "error": "Empty embedding vector"}

    user_repo = get_user_repo()
    fp_repo = get_fingerprint_repo()

    username = registration["username"]
    fullname = registration["fullname"]

    existing_user = await user_repo.find_by_username(username)
    if existing_user:
        user_id = existing_user["user_id"]
    else:
        user_id = str(uuid.uuid4())
        await user_repo.create(user_id, username, fullname)

    fingerprint_id = "fp_" + str(uuid.uuid4())[:8]
    await fp_repo.save(
        fingerprint_id=fingerprint_id,
        user_id=user_id,
        finger_id=registration["finger_type"],
        embedding=vector,
        model_name=model_name,
        image_path=registration.get("image_path", ""),
    )

    logger.info(
        "Registration complete: user=%s, fp=%s, %dD vector, %.1fms",
        username, fingerprint_id, vector_dim, processing_time_ms,
    )

    return {
        "task_id": task_id,
        "status": "completed",
        "username": username,
        "fullname": fullname,
        "fingerprint_id": fingerprint_id,
        "vector_dim": vector_dim,
        "processing_time_ms": processing_time_ms,
        "edge_id": registration.get("edge_id"),
        "edge_task_id": registration.get("edge_task_id"),
    }
