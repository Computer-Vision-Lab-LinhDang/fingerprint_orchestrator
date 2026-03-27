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
    ModelStatusReport,
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

        elif len(parts) >= 3 and parts[0] == "worker" and parts[2] == "model":
            if len(parts) >= 4 and parts[3] == "status":
                await _handle_model_status(parts[1], message)

        elif len(parts) >= 2 and parts[0] == "result":
            await _handle_task_result(parts[1], message)

        elif len(parts) >= 3 and parts[0] == "edge" and parts[2] == "register":
            await _handle_edge_register(parts[1], message)

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
            loaded_models=heartbeat.loaded_models,
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

        # Mark worker as idle
        service = get_worker_service()
        service.update_heartbeat(
            worker_id=result.worker_id,
            status=WorkerStatus.IDLE,
        )

        # Check if this is a registration task — save to DB
        from app.services.registration_service import handle_embed_result
        try:
            reg_result = await handle_embed_result(task_id, data)
            if reg_result:
                logger.info(
                    "Registration result for task '%s': %s",
                    task_id, reg_result.get("status"),
                )
                # Send result back to edge device
                edge_id = reg_result.get("edge_id")
                edge_task_id = reg_result.get("edge_task_id")
                if edge_id and edge_task_id:
                    from app.mqtt.broker import get_mqtt_broker
                    broker = get_mqtt_broker()
                    if broker.client:
                        # Use edge's original task_id so it can match
                        reg_result["task_id"] = edge_task_id
                        result_topic = "edge/{}/result/{}".format(edge_id, edge_task_id)
                        await broker.publish(
                            broker.client,
                            result_topic,
                            json.dumps(reg_result),
                        )
                        logger.info("Sent result to edge '%s'", edge_id)
        except Exception as reg_exc:
            logger.error(
                "Failed to process registration for task '%s': %s",
                task_id, reg_exc,
            )

        _write_log("task_result", result.worker_id, {
            "task_id": task_id,
            "status": result.status.value,
            "processing_time_ms": result.processing_time_ms,
            "error": result.error,
        })

    except Exception as exc:
        logger.error("Error processing result for task '%s': %s", task_id, exc)


# ── Model Status (Worker → Orchestrator) ────────────────────
async def _handle_model_status(worker_id: str, message: aiomqtt.Message) -> None:
    try:
        data = json.loads(message.payload.decode())
        report = ModelStatusReport(**data)

        status_icon = "✅" if report.status == "ready" else "❌" if report.status == "failed" else "⬇️"
        logger.info(
            "%s Model %s/%s on worker '%s': %s%s",
            status_icon,
            report.model_type,
            report.model_name,
            worker_id,
            report.status,
            f" - {report.error}" if report.error else "",
        )

        _write_log("model_status", worker_id, {
            "model_type": report.model_type,
            "model_name": report.model_name,
            "version": report.version,
            "status": report.status,
            "error": report.error,
        })

    except Exception as exc:
        logger.error("Error processing model status from '%s': %s", worker_id, exc)


# ── Edge Device Registration ────────────────────────────────
async def _handle_edge_register(edge_id: str, message: aiomqtt.Message) -> None:
    """Handle registration request from edge device."""
    try:
        data = json.loads(message.payload.decode())
        task_id = data.get("task_id", "")
        username = data.get("username", "")
        fullname = data.get("fullname", "")
        finger_type = data.get("finger_type", "right_thumb")
        image_base64 = data.get("image_base64", "")
        image_filename = data.get("image_filename", "")

        logger.info(
            "📥 Registration from edge '%s': user=%s, name=%s",
            edge_id, username, fullname,
        )

        if not username or not fullname or not image_base64:
            logger.error("Missing required fields in registration request")
            return

        _write_log("edge_register", edge_id, {
            "task_id": task_id,
            "username": username,
            "fullname": fullname,
            "finger_type": finger_type,
        })

        # Call registration service
        from app.services.registration_service import register_fingerprint
        from app.mqtt.broker import get_mqtt_broker

        broker = get_mqtt_broker()
        mqtt_client = broker.client

        if not mqtt_client:
            logger.error("MQTT client not available for registration")
            return

        result = await register_fingerprint(
            client=mqtt_client,
            username=username,
            fullname=fullname,
            image_base64=image_base64,
            finger_type=finger_type,
            image_filename=image_filename,
        )

        # Store edge_id and original task_id so we can send result back later
        from app.services.registration_service import get_pending_registrations
        pending = get_pending_registrations()
        if result.get("task_id") in pending:
            pending[result["task_id"]]["edge_id"] = edge_id
            pending[result["task_id"]]["edge_task_id"] = task_id  # original edge task_id

        logger.info(
            "Registration dispatched: task=%s, worker=%s",
            result.get("task_id"), result.get("worker_id"),
        )

    except Exception as exc:
        logger.error("Error processing registration from edge '%s': %s", edge_id, exc)
        import traceback
        traceback.print_exc()
