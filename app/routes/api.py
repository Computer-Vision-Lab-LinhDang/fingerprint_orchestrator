from __future__ import annotations

import logging
import time

from fastapi import APIRouter, Query

from app.db.database import get_database
from app.services.worker_service import get_worker_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["Dashboard"])

_log_buffer: list[dict] = []
_MAX_LOGS = 200


def add_log(level: str, source: str, message: str):
    _log_buffer.append({
        "timestamp": time.time(),
        "level": level,
        "source": source,
        "message": message,
    })
    if len(_log_buffer) > _MAX_LOGS:
        _log_buffer.pop(0)


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


@router.get("/workers")
async def list_workers():
    service = get_worker_service()
    return service.get_summary()


@router.get("/logs")
async def get_logs(limit: int = Query(50, le=200)):
    return _log_buffer[-limit:]
