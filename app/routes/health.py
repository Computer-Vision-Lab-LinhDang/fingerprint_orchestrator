from __future__ import annotations

from fastapi import APIRouter

from app.mqtt.broker import get_mqtt_broker
from app.services.worker_service import get_worker_service

router = APIRouter(tags=["System"])


@router.get("/health")
async def health():
    """System health check."""
    service = get_worker_service()
    broker = get_mqtt_broker()
    return {
        "status": "ok" if broker.is_connected else "degraded",
        "active_workers": service.active_count,
        "mqtt_connected": broker.is_connected,
    }


@router.get("/workers")
async def list_workers():
    """List all known workers and their status."""
    service = get_worker_service()
    return {"workers": service.get_summary()}
