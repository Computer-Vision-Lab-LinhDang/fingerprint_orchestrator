from __future__ import annotations

import json
import logging
from datetime import timedelta
from typing import Optional

import paho.mqtt.client as mqtt_client
from fastapi import APIRouter, HTTPException, Query

from app.core.config import get_settings
from app.db.database import get_database
from app.repositories.storage_repo import get_storage_repo
from app.services.worker_service import get_worker_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["Models"])
settings = get_settings()
SUPPORTED_MODEL_TYPES = {"embedding", "matching", "pad"}


def _extract_model_payload_fields(model_type: str, s3_path: str) -> tuple[str, str, str]:
    prefix = f"{model_type}/"
    if not s3_path.startswith(prefix):
        raise HTTPException(
            400,
            f"s3_path must be under '{prefix}' for model_type '{model_type}'",
        )

    relative_path = s3_path[len(prefix):].strip("/")
    if not relative_path:
        raise HTTPException(400, "s3_path must point to a model file")

    relative_obj = relative_path.split("/")
    if len(relative_obj) >= 2:
        model_name = relative_obj[0]
    else:
        model_name = relative_obj[-1].rsplit(".", 1)[0]

    version = model_name.split("_")[-1] if "_" in model_name else "latest"
    return model_name, version, relative_path


@router.get("/models")
async def list_models():
    db = get_database()
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM models ORDER BY created_at DESC")
    return [dict(r) for r in rows]


@router.get("/models/minio")
async def list_minio_models(prefix: str = Query("", description="Folder prefix")):
    storage = get_storage_repo()
    try:
        objects = list(storage._client.list_objects(
            settings.MINIO_BUCKET_MODELS, prefix=prefix or "", recursive=False,
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
            all_objects = storage._client.list_objects(
                settings.MINIO_BUCKET_MODELS, prefix=prefix or "", recursive=True,
            )
            seen_folders = set()
            for obj in all_objects:
                name = obj.object_name
                relative = name[len(prefix):] if prefix else name
                if "/" in relative:
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
    worker_id = body.get("worker_id")
    s3_path = body.get("s3_path")
    model_type = body.get("model_type", "embedding")

    if not worker_id or not s3_path:
        raise HTTPException(400, "worker_id and s3_path required")
    if model_type not in SUPPORTED_MODEL_TYPES:
        raise HTTPException(400, f"model_type must be one of {sorted(SUPPORTED_MODEL_TYPES)}")

    worker_svc = get_worker_service()
    if worker_id not in worker_svc.workers:
        raise HTTPException(404, f"Worker '{worker_id}' not found")

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

    model_name, version, relative_path = _extract_model_payload_fields(model_type, s3_path)

    client = mqtt_client.Client(client_id="dashboard-deploy", protocol=mqtt_client.MQTTv311)
    client.connect(settings.MQTT_BROKER_HOST, settings.MQTT_BROKER_PORT, 10)

    payload = json.dumps({
        "model_type": model_type,
        "model_name": model_name,
        "version": version,
        "download_url": download_url,
        "s3_path": s3_path,
        "relative_path": relative_path,
    })

    topic = f"task/{worker_id}/model/update"
    result = client.publish(topic, payload, qos=1)
    client.disconnect()

    if result.rc == 0:
        logger.info("Deploy model '%s' to worker '%s'", s3_path, worker_id)
        return {
            "status": "sent",
            "worker_id": worker_id,
            "model_type": model_type,
            "model": model_name,
            "relative_path": relative_path,
        }
    else:
        raise HTTPException(500, "Failed to send deploy command")
