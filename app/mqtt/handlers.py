from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Optional

import aiomqtt

from app.schemas.mqtt_payloads import (
    HeartbeatPayload,
    TaskResult,
    WorkerStatus,
)
from app.services.worker_service import get_worker_service
from app.services.task_service import store_result

logger = logging.getLogger(__name__)

# ── File logger setup ────────────────────────────────────────
LOG_DIR = os.path.join(os.getcwd(), "logs")
LOG_FILE = os.path.join(LOG_DIR, "worker_events.log")

os.makedirs(LOG_DIR, exist_ok=True)


def _write_log(event_type: str, worker_id: str, data: dict) -> None:
    """Write a structured log entry to the log file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = {
        "timestamp": timestamp,
        "event_type": event_type,
        "worker_id": worker_id,
        **data,
    }
    line = json.dumps(entry, ensure_ascii=False)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as exc:
        logger.error("Failed to write log file: %s", exc)


# ── Main dispatcher ──────────────────────────────────────────
async def on_message(message: aiomqtt.Message) -> None:
    topic = str(message.topic)
    parts = topic.split("/")

    try:
        if len(parts) >= 3 and parts[0] == "worker" and parts[2] == "heartbeat":
            await _handle_heartbeat(parts[1], message)

        elif len(parts) >= 3 and parts[0] == "worker" and parts[2] == "status":
            await _handle_lwt(parts[1], message)

        elif len(parts) >= 3 and parts[0] == "worker" and parts[2] == "message":
            await _handle_worker_message(parts[1], message)

        elif len(parts) >= 2 and parts[0] == "result":
            await _handle_task_result(parts[1], message)

        else:
            logger.warning("Unknown topic: %s", topic)

    except Exception as exc:
        logger.error("Error processing message from topic '%s': %s", topic, exc)


# ── Heartbeat ────────────────────────────────────────────────
async def _handle_heartbeat(worker_id: str, message: aiomqtt.Message) -> None:
    try:
        data = json.loads(message.payload.decode())
        heartbeat = HeartbeatPayload(**data)

        service = get_worker_service()
        service.update_heartbeat(
            worker_id=worker_id,
            status=heartbeat.status,
            gpu_memory_used=heartbeat.gpu_memory_used_mb,
            gpu_memory_total=heartbeat.gpu_memory_total_mb,
            current_task_id=heartbeat.current_task_id,
        )
        logger.debug("Heartbeat from worker '%s': %s", worker_id, heartbeat.status.value)

        _write_log("heartbeat", worker_id, {
            "status": heartbeat.status.value,
            "uptime_seconds": heartbeat.uptime_seconds,
        })

    except Exception as exc:
        logger.error("Error processing heartbeat from '%s': %s", worker_id, exc)


# ── LWT (Last Will and Testament) ────────────────────────────
async def _handle_lwt(worker_id: str, message: aiomqtt.Message) -> None:
    try:
        payload = message.payload.decode() if message.payload else ""
        logger.warning("Worker '%s' OFFLINE (LWT): %s", worker_id, payload)

        service = get_worker_service()
        service.mark_offline(worker_id)

        _write_log("lwt_offline", worker_id, {"raw_payload": payload})

    except Exception as exc:
        logger.error("Error processing LWT from '%s': %s", worker_id, exc)


# ── Worker Message ───────────────────────────────────────────
async def _handle_worker_message(worker_id: str, message: aiomqtt.Message) -> None:
    try:
        data = json.loads(message.payload.decode())
        content = data.get("content", "")
        message_id = data.get("message_id", "")

        logger.info(
            "Message from worker '%s': %s",
            worker_id, content,
        )

        _write_log("message", worker_id, {
            "message_id": message_id,
            "content": content,
        })

    except Exception as exc:
        logger.error("Error processing message from '%s': %s", worker_id, exc)


# ── Task Result ──────────────────────────────────────────────
async def _handle_task_result(task_id: str, message: aiomqtt.Message) -> None:
    try:
        data = json.loads(message.payload.decode())
        result = TaskResult(**data)

        store_result(task_id, result)
        logger.info(
            "Task result '%s' from worker '%s': %s",
            task_id, result.worker_id, result.status.value,
        )

        service = get_worker_service()
        service.update_heartbeat(
            worker_id=result.worker_id,
            status=WorkerStatus.IDLE,
        )

        _write_log("task_result", result.worker_id, {
            "task_id": task_id,
            "status": result.status.value,
            "processing_time_ms": result.processing_time_ms,
            "error": result.error,
        })

    except Exception as exc:
        logger.error("Error processing result for task '%s': %s", task_id, exc)
