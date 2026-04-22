from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from fastapi import APIRouter, Query

from app.db.database import get_database
from app.mqtt.broker import get_mqtt_broker
from app.services.worker_service import get_worker_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["Dashboard"])

_log_buffer: list[dict] = []
_MAX_LOGS = 200
_LOG_FILE = Path.cwd() / "logs" / "worker_events.log"


def add_log(level: str, source: str, message: str):
    _log_buffer.append({
        "timestamp": time.time(),
        "level": level,
        "source": source,
        "message": message,
    })
    if len(_log_buffer) > _MAX_LOGS:
        _log_buffer.pop(0)


def _read_recent_worker_events(limit: int) -> list[dict]:
    if not _LOG_FILE.exists():
        return []

    try:
        lines = _LOG_FILE.read_text(encoding="utf-8").splitlines()
    except Exception as exc:
        logger.error("Failed to read %s: %s", _LOG_FILE, exc)
        return []

    entries: list[dict] = []
    for raw_line in lines[-limit:]:
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            entries.append(json.loads(raw_line))
        except json.JSONDecodeError:
            logger.warning("Skipping malformed worker event log line")
    return entries


@router.get("/stats")
async def get_stats():
    db = get_database()
    worker_svc = get_worker_service()
    workers = worker_svc.get_summary()

    async with db.acquire() as conn:
        user_count = await conn.fetchval("SELECT COUNT(*) FROM users")
        fp_count = await conn.fetchval("SELECT COUNT(*) FROM fingerprints")

    return {
        "users": user_count,
        "fingerprints": fp_count,
        "workers_online": worker_svc.active_count,
        "workers_total": len(worker_svc.workers),
        "workers_busy": sum(1 for worker in workers if worker["status"] == "busy"),
        "workers_offline": sum(1 for worker in workers if worker["status"] == "offline"),
        "models_loaded": sum(len(worker.get("loaded_models", {})) for worker in workers),
    }


@router.get("/workers")
async def list_workers():
    service = get_worker_service()
    return service.get_summary()


@router.get("/overview")
async def get_overview(log_limit: int = Query(20, ge=1, le=100)):
    broker = get_mqtt_broker()
    stats = await get_stats()
    workers = get_worker_service().get_summary()
    logs = _read_recent_worker_events(log_limit)
    return {
        "health": {
            "status": "ok" if broker.is_connected else "degraded",
            "mqtt_connected": broker.is_connected,
            "active_workers": stats["workers_online"],
        },
        "stats": stats,
        "workers": workers,
        "logs": logs,
        "updated_at": time.time(),
    }


@router.get("/logs")
async def get_logs(limit: int = Query(50, le=200)):
    return _read_recent_worker_events(limit)
