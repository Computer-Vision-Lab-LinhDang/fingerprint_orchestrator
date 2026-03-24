from __future__ import annotations

from fastapi import APIRouter

from app.services.worker_service import get_worker_service

router = APIRouter(tags=["System"])


@router.get("/health")
async def health(mqtt_connected: bool = False):
    """System health check."""
    service = get_worker_service()
    return {
        "status": "ok",
        "active_workers": service.active_count,
        "mqtt_connected": mqtt_connected,
    }


@router.get("/workers")
async def list_workers():
    """List all known workers and their status."""
    service = get_worker_service()
    return {"workers": service.get_summary()}
