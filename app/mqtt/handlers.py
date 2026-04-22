from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime
from typing import Optional

import aiomqtt

from app.schemas.mqtt_payloads import (
    EnrollmentUploadCommand,
    EnrollmentUploadStatus,
    FingerprintDeletedEvent,
    HeartbeatPayload,
    ModelStatusReport,
    TaskResult,
    UserDeletedEvent,
    WorkerStatus,
)
from app.services.worker_service import get_worker_service

logger = logging.getLogger(__name__)

LOG_DIR = os.path.join(os.getcwd(), "logs")
LOG_FILE = os.path.join(LOG_DIR, "worker_events.log")
os.makedirs(LOG_DIR, exist_ok=True)

_pending_results: dict[str, TaskResult] = {}


def _write_log(event_type: str, worker_id: str, data: dict) -> None:
    entry = json.dumps({
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "event_type": event_type,
        "worker_id": worker_id,
        **data,
    }, ensure_ascii=False)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(entry + "\n")
    except Exception as exc:
        logger.error("Failed to write log file: %s", exc)


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
        elif len(parts) >= 3 and parts[0] == "worker" and parts[2] == "enrolled":
            await _handle_worker_enrolled(parts[1], message)
        elif len(parts) >= 5 and parts[0] == "worker" and parts[2] == "enrollment" and parts[3] == "upload" and parts[4] == "status":
            await _handle_enrollment_upload_status(parts[1], message)
        elif len(parts) >= 4 and parts[0] == "worker" and parts[2] == "model" and parts[3] == "status":
            await _handle_model_status(parts[1], message)
        elif len(parts) >= 2 and parts[0] == "result":
            await _handle_task_result(parts[1], message)
        elif len(parts) >= 3 and parts[0] == "edge" and parts[2] == "register":
            await _handle_edge_register(parts[1], message)
        elif len(parts) >= 3 and parts[0] == "edge" and parts[2] == "verify":
            await _handle_edge_verify(parts[1], message)
        else:
            logger.warning("Unknown topic: %s", topic)
    except Exception as exc:
        logger.error("Error processing message from topic '%s': %s", topic, exc)


async def _handle_heartbeat(worker_id: str, message: aiomqtt.Message) -> None:
    try:
        data = json.loads(message.payload.decode())
        heartbeat = HeartbeatPayload(**data)
        service = get_worker_service()
        previous = service.workers.get(worker_id)
        previous_status = previous.status if previous else None
        service.update_heartbeat(
            worker_id=worker_id,
            status=heartbeat.status,
            cpu_percent=heartbeat.cpu_percent,
            ram_used_mb=heartbeat.ram_used_mb,
            ram_total_mb=heartbeat.ram_total_mb,
            gpu_percent=heartbeat.gpu_percent,
            gpu_memory_used=heartbeat.gpu_memory_used_mb,
            gpu_memory_total=heartbeat.gpu_memory_total_mb,
            temperature_c=heartbeat.temperature_c,
            current_task_id=heartbeat.current_task_id,
            loaded_models=heartbeat.loaded_models,
        )
        if previous is None or previous_status == WorkerStatus.OFFLINE:
            await _request_worker_sync_check(worker_id, reason="reconnect")
        _write_log("heartbeat", worker_id, {
            "status": heartbeat.status.value,
            "uptime_seconds": heartbeat.uptime_seconds,
        })
    except Exception as exc:
        logger.error("Error processing heartbeat from '%s': %s", worker_id, exc)


async def _handle_lwt(worker_id: str, message: aiomqtt.Message) -> None:
    try:
        payload = message.payload.decode() if message.payload else ""
        logger.warning("Worker '%s' OFFLINE (LWT): %s", worker_id, payload)
        service = get_worker_service()
        service.mark_offline(worker_id)
        _write_log("lwt_offline", worker_id, {"raw_payload": payload})
    except Exception as exc:
        logger.error("Error processing LWT from '%s': %s", worker_id, exc)


async def _handle_worker_message(worker_id: str, message: aiomqtt.Message) -> None:
    try:
        data = json.loads(message.payload.decode())
        logger.info("Message from worker '%s': %s", worker_id, data.get("content", ""))
        _write_log("message", worker_id, {
            "message_id": data.get("message_id", ""),
            "content": data.get("content", ""),
        })
    except Exception as exc:
        logger.error("Error processing message from '%s': %s", worker_id, exc)


async def _handle_task_result(task_id: str, message: aiomqtt.Message) -> None:
    try:
        data = json.loads(message.payload.decode())
        result = TaskResult(**data)

        _pending_results[task_id] = result
        logger.info("Task result '%s' from worker '%s': %s", task_id, result.worker_id, result.status.value)

        service = get_worker_service()
        service.update_heartbeat(worker_id=result.worker_id, status=WorkerStatus.IDLE)

        from app.services.registration_service import handle_embed_result
        try:
            reg_result = await handle_embed_result(task_id, data)
            if reg_result:
                logger.info("Registration result for task '%s': %s", task_id, reg_result.get("status"))
                edge_id = reg_result.get("edge_id")
                edge_task_id = reg_result.get("edge_task_id")
                if edge_id and edge_task_id:
                    from app.mqtt.broker import get_mqtt_broker
                    broker = get_mqtt_broker()
                    if broker.client:
                        reg_result["task_id"] = edge_task_id
                        result_topic = f"edge/{edge_id}/result/{edge_task_id}"
                        await broker.publish(broker.client, result_topic, json.dumps(reg_result))
                        logger.info("Sent result to edge '%s'", edge_id)
        except Exception as reg_exc:
            logger.error("Failed to process registration for task '%s': %s", task_id, reg_exc)

        _write_log("task_result", result.worker_id, {
            "task_id": task_id,
            "status": result.status.value,
            "processing_time_ms": result.processing_time_ms,
            "error": result.error,
        })
    except Exception as exc:
        logger.error("Error processing result for task '%s': %s", task_id, exc)


async def _handle_model_status(worker_id: str, message: aiomqtt.Message) -> None:
    try:
        data = json.loads(message.payload.decode())
        report = ModelStatusReport(**data)

        status_icon = "✅" if report.status == "ready" else "❌" if report.status == "failed" else "⬇️"
        logger.info(
            "%s Model %s/%s on worker '%s': %s%s",
            status_icon, report.model_type, report.model_name, worker_id,
            report.status, f" - {report.error}" if report.error else "",
        )

        if report.status == "ready":
            worker_svc = get_worker_service()
            if worker_id in worker_svc.workers:
                worker_svc.workers[worker_id].loaded_models[report.model_type] = report.model_name

        _write_log("model_status", worker_id, {
            "model_type": report.model_type,
            "model_name": report.model_name,
            "version": report.version,
            "status": report.status,
            "error": report.error,
        })
    except Exception as exc:
        logger.error("Error processing model status from '%s': %s", worker_id, exc)


async def _request_worker_sync_check(worker_id: str, reason: str) -> None:
    from app.mqtt.broker import get_mqtt_broker

    broker = get_mqtt_broker()
    if not broker.client:
        return
    topic = f"task/{worker_id}/sync/check"
    payload = json.dumps({"reason": reason})
    await broker.publish(broker.client, topic, payload, qos=1)


async def _broadcast_sync_to_other_workers(source_worker_id: str, payload: dict) -> None:
    from app.mqtt.broker import get_mqtt_broker

    broker = get_mqtt_broker()
    if not broker.client:
        return

    worker_service = get_worker_service()
    for target_worker_id, worker in worker_service.workers.items():
        if target_worker_id == source_worker_id:
            continue
        if worker.status == WorkerStatus.OFFLINE:
            continue
        topic = f"task/{target_worker_id}/sync"
        await broker.publish(broker.client, topic, json.dumps(payload), qos=1)


async def broadcast_user_deleted(payload: UserDeletedEvent) -> None:
    """Broadcast a user deletion event to all currently online workers."""
    from app.mqtt.broker import get_mqtt_broker

    broker = get_mqtt_broker()
    if not broker.client:
        return

    worker_service = get_worker_service()
    encoded = payload.model_dump_json()
    for target_worker_id, worker in worker_service.workers.items():
        if worker.status == WorkerStatus.OFFLINE:
            continue
        topic = f"task/{target_worker_id}/sync/delete-user"
        await broker.publish(broker.client, topic, encoded, qos=1)


async def broadcast_fingerprint_deleted(payload: FingerprintDeletedEvent) -> None:
    """Broadcast a fingerprint deletion event to all currently online workers."""
    from app.mqtt.broker import get_mqtt_broker

    broker = get_mqtt_broker()
    if not broker.client:
        return

    worker_service = get_worker_service()
    encoded = payload.model_dump_json()
    for target_worker_id, worker in worker_service.workers.items():
        if worker.status == WorkerStatus.OFFLINE:
            continue
        topic = f"task/{target_worker_id}/sync/delete-fingerprint"
        await broker.publish(broker.client, topic, encoded, qos=1)


async def _dispatch_enrollment_upload_command(
    worker_id: str,
    payload: EnrollmentUploadCommand,
) -> None:
    from app.mqtt.broker import get_mqtt_broker

    broker = get_mqtt_broker()
    if not broker.client:
        return
    topic = f"task/{worker_id}/enrollment/upload"
    await broker.publish(
        broker.client,
        topic,
        payload.json(),
        qos=1,
    )


async def _handle_enrollment_upload_status(worker_id: str, message: aiomqtt.Message) -> None:
    try:
        data = json.loads(message.payload.decode())
        status = EnrollmentUploadStatus(**data)

        if status.status == "uploaded" and status.object_name:
            from app.repositories.fingerprint_repo import get_fingerprint_repo

            fp_repo = get_fingerprint_repo()
            await fp_repo.update_image_path(status.fingerprint_id, status.object_name)

        _write_log("worker_enrollment_upload", worker_id, data)
        logger.info(
            "Enrollment upload status from worker '%s': fp=%s status=%s",
            worker_id,
            status.fingerprint_id,
            status.status,
        )
    except Exception as exc:
        logger.error("Error processing enrollment upload status from '%s': %s", worker_id, exc)


async def _handle_worker_enrolled(worker_id: str, message: aiomqtt.Message) -> None:
    """Persist worker enrollment, ask source worker to upload image, then broadcast sync."""
    try:
        data = json.loads(message.payload.decode())
        user = data.get("user", {})
        fp = data.get("fingerprint", {})
        model = data.get("model", {})

        username = (user.get("employee_id") or "").strip()
        fullname = (user.get("full_name") or "").strip()
        finger_index = int(fp.get("finger_index", 0))
        vector = fp.get("embedding") or []
        quality_score = float(fp.get("quality_score", 0) or 0)
        image_available = bool(fp.get("image_available"))

        if not username or not vector:
            logger.warning("Skip worker enrollment: missing employee_id or embedding")
            return

        from app.repositories.user_repo import get_user_repo
        from app.repositories.fingerprint_repo import get_fingerprint_repo
        from app.repositories.storage_repo import get_storage_repo

        user_repo = get_user_repo()
        fp_repo = get_fingerprint_repo()
        storage = get_storage_repo()

        existing_user = await user_repo.find_by_employee_id(username)
        if existing_user:
            user_id = existing_user["user_id"]
        else:
            user_id = str(uuid.uuid4())
            await user_repo.create(user_id, username, fullname or username)

        fp_id_raw = fp.get("fp_id")
        if fp_id_raw is not None:
            fingerprint_id = f"{worker_id}-{fp_id_raw}"
        else:
            fingerprint_id = "fp_" + str(uuid.uuid4())[:8]

        model_name = model.get("name") or "worker-sync"
        await fp_repo.save(
            fingerprint_id=fingerprint_id,
            user_id=user_id,
            finger_index=finger_index,
            embedding=vector,
            model_version=model_name,
            image_path="",
            quality_score=quality_score,
        )

        if image_available:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            object_name = f"{username}_f{finger_index}_{ts}.tif"
            upload_url = storage.get_presigned_put_url(object_name)
            await _dispatch_enrollment_upload_command(
                worker_id,
                EnrollmentUploadCommand(
                    fp_id=int(fp.get("fp_id", 0) or 0),
                    fingerprint_id=fingerprint_id,
                    object_name=object_name,
                    upload_url=upload_url,
                    content_type="image/tiff",
                ),
            )

        sync_payload = {
            **data,
            "user": {
                **user,
                "user_id": user_id,
                "id": user_id,
                "employee_id": username,
                "full_name": fullname or username,
            },
            "fingerprint": {
                **fp,
                "fingerprint_id": fingerprint_id,
                "finger_index": finger_index,
            },
        }
        await _broadcast_sync_to_other_workers(worker_id, sync_payload)

        _write_log("worker_enrolled", worker_id, {
            "employee_id": username,
            "fingerprint_id": fingerprint_id,
            "finger_index": finger_index,
            "vector_dim": len(vector),
            "quality_score": quality_score,
            "image_available": image_available,
        })
        logger.info(
            "Synced worker enrollment: worker=%s user=%s fp=%s",
            worker_id, username, fingerprint_id,
        )
    except Exception as exc:
        logger.error("Error processing worker enrollment from '%s': %s", worker_id, exc)


async def _handle_edge_register(edge_id: str, message: aiomqtt.Message) -> None:
    try:
        data = json.loads(message.payload.decode())
        task_id = data.get("task_id", "")
        employee_id = data.get("employee_id", data.get("username", ""))
        full_name = data.get("full_name", data.get("fullname", ""))
        finger_index = int(data.get("finger_index", data.get("finger_type", 1)))
        department = data.get("department", "")
        image_base64 = data.get("image_base64", "")
        image_filename = data.get("image_filename", "")

        logger.info("📥 Registration from edge '%s': employee_id=%s, name=%s", edge_id, employee_id, full_name)

        if not employee_id or not full_name or not image_base64:
            logger.error("Missing required fields in registration request")
            return

        _write_log("edge_register", edge_id, {
            "task_id": task_id, "employee_id": employee_id,
            "full_name": full_name, "finger_index": finger_index,
        })

        from app.services.registration_service import register_fingerprint
        from app.mqtt.broker import get_mqtt_broker

        broker = get_mqtt_broker()
        mqtt_client = broker.client
        if not mqtt_client:
            logger.error("MQTT client not available for registration")
            return

        result = await register_fingerprint(
            client=mqtt_client,
            employee_id=employee_id, full_name=full_name,
            image_base64=image_base64, finger_index=finger_index,
            department=department,
            image_filename=image_filename,
        )

        from app.services.registration_service import get_pending_registrations
        pending = get_pending_registrations()
        if result.get("task_id") in pending:
            pending[result["task_id"]]["edge_id"] = edge_id
            pending[result["task_id"]]["edge_task_id"] = task_id

        logger.info("Registration dispatched: task=%s, worker=%s", result.get("task_id"), result.get("worker_id"))
    except Exception as exc:
        logger.error("Error processing registration from edge '%s': %s", edge_id, exc)
        import traceback
        traceback.print_exc()


async def _handle_edge_verify(edge_id: str, message: aiomqtt.Message) -> None:
    """Handle verification request from edge device."""
    try:
        data = json.loads(message.payload.decode())
        task_id = data.get("task_id", "")
        image_base64 = data.get("image_base64", "")
        image_filename = data.get("image_filename", "")

        logger.info("📥 Verify request from edge '%s': task=%s", edge_id, task_id)

        if not image_base64:
            logger.error("Missing image_base64 in verify request")
            return

        _write_log("edge_verify", edge_id, {"task_id": task_id})

        # TODO: Call verification service
        # from app.services.verification_service import start_verification
        # from app.mqtt.broker import get_mqtt_broker
        # broker = get_mqtt_broker()
        # mqtt_client = broker.client
        # if not mqtt_client:
        #     logger.error("MQTT client not available for verification")
        #     return
        # result = await start_verification(
        #     client=mqtt_client,
        #     task_id=task_id,
        #     image_base64=image_base64,
        #     image_filename=image_filename,
        #     edge_id=edge_id,
        # )

        # TODO: Track edge_id so final result can be sent back
        # from app.services.verification_service import get_pending_verifications
        # pending = get_pending_verifications()
        # if task_id in pending:
        #     pending[task_id]["edge_id"] = edge_id
        #     pending[task_id]["edge_task_id"] = task_id

        logger.info("[VERIFY] TODO: _handle_edge_verify not implemented yet")

    except Exception as exc:
        logger.error("Error processing verify from edge '%s': %s", edge_id, exc)
