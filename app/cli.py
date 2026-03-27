from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime
from threading import Lock
from typing import Optional

import paho.mqtt.client as mqtt
from minio import Minio
from minio.error import S3Error

from app.core.config import get_settings

# ── ANSI Colors ──────────────────────────────────────────────
class C:
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    RED     = "\033[91m"
    GREEN   = "\033[92m"
    YELLOW  = "\033[93m"
    BLUE    = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN    = "\033[96m"
    WHITE   = "\033[97m"


# ── State ────────────────────────────────────────────────────
_lock = Lock()
_workers: dict[str, dict] = {}       # worker_id → {status, last_seen, uptime, ...}
_message_log: list[tuple] = []       # (timestamp, event_type, worker_id, data)
_max_log = 100
_connected = False
_stats = {
    "messages_received": 0,
    "heartbeats": 0,
    "worker_messages": 0,
    "task_results": 0,
    "lwt_events": 0,
}

LOG_DIR = os.path.join(os.getcwd(), "logs")
LOG_FILE = os.path.join(LOG_DIR, "worker_events.log")
os.makedirs(LOG_DIR, exist_ok=True)


def _write_log(event_type: str, worker_id: str, data: dict) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = {"timestamp": timestamp, "event_type": event_type, "worker_id": worker_id, **data}
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _add_log(event_type: str, worker_id: str, data: dict) -> None:
    with _lock:
        _message_log.append((time.time(), event_type, worker_id, data))
        if len(_message_log) > _max_log:
            _message_log.pop(0)


# ── Helpers ──────────────────────────────────────────────────
def clear_screen():
    os.system("clear" if os.name != "nt" else "cls")


def fmt_time(ts):
    if ts is None:
        return "—"
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S")


def fmt_ago(ts):
    if ts is None:
        return "—"
    diff = int(time.time() - ts)
    if diff < 60:
        return f"{diff}s ago"
    elif diff < 3600:
        return f"{diff // 60}m {diff % 60}s ago"
    return f"{diff // 3600}h ago"


# ── MQTT Callbacks ───────────────────────────────────────────
def _on_connect(client, userdata, flags, *args):
    # Compatible with both paho v1 (rc,) and v2 (reason_code, properties)
    global _connected
    rc = args[0]
    if rc == 0:
        _connected = True
        client.subscribe("worker/+/heartbeat", qos=1)
        client.subscribe("worker/+/status", qos=1)
        client.subscribe("worker/+/message", qos=1)
        client.subscribe("worker/+/model/status", qos=1)
        client.subscribe("result/+", qos=1)
    else:
        _connected = False


def _on_disconnect(client, userdata, *args):
    global _connected
    _connected = False


def _on_message(client, userdata, message: mqtt.MQTTMessage):
    global _stats
    _stats["messages_received"] += 1

    topic = message.topic
    parts = topic.split("/")

    try:
        data = json.loads(message.payload.decode())
    except Exception:
        data = {"raw": message.payload.decode()[:200] if message.payload else ""}

    # worker/+/heartbeat
    if len(parts) >= 3 and parts[0] == "worker" and parts[2] == "heartbeat":
        worker_id = parts[1]
        _stats["heartbeats"] += 1
        with _lock:
            loaded_models = data.get("loaded_models", {})
            _workers[worker_id] = {
                "status": data.get("status", "unknown"),
                "last_seen": time.time(),
                "uptime": data.get("uptime_seconds"),
                "current_task": data.get("current_task_id"),
                "gpu_used": data.get("gpu_memory_used_mb"),
                "gpu_total": data.get("gpu_memory_total_mb"),
                "loaded_models": loaded_models,
            }
        _add_log("heartbeat", worker_id, {"status": data.get("status")})
        _write_log("heartbeat", worker_id, {"status": data.get("status")})

    # worker/+/status (LWT)
    elif len(parts) >= 3 and parts[0] == "worker" and parts[2] == "status":
        worker_id = parts[1]
        _stats["lwt_events"] += 1
        with _lock:
            if worker_id in _workers:
                _workers[worker_id]["status"] = "offline"
                _workers[worker_id]["last_seen"] = time.time()
        _add_log("lwt", worker_id, {"status": "offline"})
        _write_log("lwt_offline", worker_id, {})

    # worker/+/message
    elif len(parts) >= 3 and parts[0] == "worker" and parts[2] == "message":
        worker_id = parts[1]
        _stats["worker_messages"] += 1
        content = data.get("content", "")
        _add_log("message", worker_id, {"content": content})
        _write_log("message", worker_id, {"content": content, "message_id": data.get("message_id")})

    # result/+
    elif len(parts) >= 2 and parts[0] == "result":
        task_id = parts[1]
        worker_id = data.get("worker_id", "unknown")
        _stats["task_results"] += 1
        _add_log("result", worker_id, {"task_id": task_id, "status": data.get("status")})
        _write_log("task_result", worker_id, {"task_id": task_id, "status": data.get("status")})


# ── MQTT Client Setup ───────────────────────────────────────
_mqtt_client: Optional[mqtt.Client] = None


def _get_client():
    global _mqtt_client
    if _mqtt_client is None:
        settings = get_settings()
        # Support both paho-mqtt v1.x and v2.x
        try:
            _mqtt_client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id="orchestrator-cli-{}".format(os.getpid()),
                protocol=mqtt.MQTTv311,
            )
        except (AttributeError, TypeError):
            _mqtt_client = mqtt.Client(
                client_id="orchestrator-cli-{}".format(os.getpid()),
                protocol=mqtt.MQTTv311,
            )
        if settings.MQTT_USERNAME:
            _mqtt_client.username_pw_set(settings.MQTT_USERNAME, settings.MQTT_PASSWORD)
        _mqtt_client.on_connect = _on_connect
        _mqtt_client.on_disconnect = _on_disconnect
        _mqtt_client.on_message = _on_message
    return _mqtt_client


def _connect():
    global _connected
    settings = get_settings()
    client = _get_client()
    try:
        client.connect(settings.MQTT_BROKER_HOST, settings.MQTT_BROKER_PORT, settings.MQTT_KEEPALIVE)
        client.loop_start()
        time.sleep(2)
    except Exception as exc:
        print(f"  {C.RED}✗ Connection error: {exc}{C.RESET}")


def _disconnect():
    client = _get_client()
    client.loop_stop()
    client.disconnect()


# ── Banner ───────────────────────────────────────────────────
def print_banner():
    print(f"""
{C.CYAN}{C.BOLD}╔══════════════════════════════════════════════════╗
║     🎛️  FINGERPRINT ORCHESTRATOR — CLI           ║
╚══════════════════════════════════════════════════╝{C.RESET}
""")


# ── Menu ─────────────────────────────────────────────────────
def print_menu():
    settings = get_settings()
    if _connected:
        status = f"{C.GREEN}● CONNECTED{C.RESET}"
    else:
        status = f"{C.RED}● DISCONNECTED{C.RESET}"

    worker_count = len([w for w in _workers.values() if w.get("status") in ("idle", "online", "busy")])
    print(f"  {C.DIM}Orchestrator{C.RESET}  │  {status}  │  {C.DIM}Workers online:{C.RESET} {C.BOLD}{worker_count}{C.RESET}")
    print(f"  {C.DIM}Broker:{C.RESET} {settings.MQTT_BROKER_HOST}:{settings.MQTT_BROKER_PORT}")
    print()
    print(f"  {C.YELLOW}{'─' * 48}{C.RESET}")
    print(f"  {C.BOLD}[1]{C.RESET}  🖥️   Connected Workers")
    print(f"  {C.BOLD}[2]{C.RESET}  📋  Event Log")
    print(f"  {C.BOLD}[3]{C.RESET}  📈  Statistics")
    print(f"  {C.BOLD}[4]{C.RESET}  ⚙️   Current Configuration")
    print(f"  {C.BOLD}[5]{C.RESET}  🔄  Reconnect MQTT")
    print(f"  {C.BOLD}[6]{C.RESET}  🧹  Clear Screen")
    print(f"  {C.BOLD}[7]{C.RESET}  🧠  MinIO Models")
    print(f"  {C.BOLD}[8]{C.RESET}  🚀  Deploy Model to Worker")
    print(f"  {C.BOLD}[0]{C.RESET}  🚪  Exit")
    print(f"  {C.YELLOW}{'─' * 48}{C.RESET}")


# ── [1] Connected Workers ───────────────────────────────────
def show_workers():
    print(f"\n  {C.CYAN}{C.BOLD}═══ CONNECTED WORKERS ═══{C.RESET}\n")

    if not _workers:
        print(f"  {C.DIM}No workers detected yet. Waiting for heartbeats...{C.RESET}")
        return

    print(f"  {C.DIM}{'Worker ID':<20} {'Status':<10} {'Last Seen':<12} {'Models'}{C.RESET}")
    print(f"  {C.DIM}{'─' * 75}{C.RESET}")

    for wid, info in _workers.items():
        status = info.get("status", "unknown")
        if status in ("idle", "online"):
            s_color = C.GREEN
        elif status == "busy":
            s_color = C.YELLOW
        else:
            s_color = C.RED

        last = fmt_ago(info.get("last_seen"))

        # Format loaded models
        loaded = info.get("loaded_models", {})
        if loaded:
            models_str = ", ".join(
                f"{C.MAGENTA}{v}{C.RESET}" for v in loaded.values()
            )
        else:
            models_str = f"{C.DIM}(none){C.RESET}"

        print(f"  {C.WHITE}{wid:<20}{C.RESET} {s_color}{status:<10}{C.RESET} {last:<12} {models_str}")

    print()


# ── [2] Event Log ────────────────────────────────────────────
def show_event_log():
    print(f"\n  {C.CYAN}{C.BOLD}═══ EVENT LOG ═══{C.RESET}\n")

    # 1. In-memory recent events
    with _lock:
        logs = list(_message_log)

    if logs:
        print(f"  {C.BOLD}▸ Recent Events (in-memory){C.RESET}")
        print(f"  {C.DIM}{'Time':<10} {'Type':<12} {'Worker':<20} {'Details'}{C.RESET}")
        print(f"  {C.DIM}{'─' * 70}{C.RESET}")

        type_colors = {"heartbeat": C.DIM, "message": C.CYAN, "result": C.GREEN, "lwt": C.RED}

        for ts, event_type, worker_id, data in logs[-15:]:
            t = datetime.fromtimestamp(ts).strftime("%H:%M:%S")
            color = type_colors.get(event_type, C.WHITE)
            detail = ""
            if event_type == "message":
                detail = data.get("content", "")[:40]
            elif event_type == "result":
                detail = f"task={data.get('task_id', '')[:12]}  status={data.get('status', '')}"
            elif event_type == "lwt":
                detail = "OFFLINE (LWT)"
            elif event_type == "heartbeat":
                detail = data.get("status", "")

            print(f"  {t:<10} {color}{event_type:<12}{C.RESET} {worker_id:<20} {detail}")

        print(f"  {C.DIM}({min(15, len(logs))}/{len(logs)} events){C.RESET}")
    else:
        print(f"  {C.DIM}No in-memory events yet.{C.RESET}")

    # 2. Log file (persisted)
    print()
    if os.path.exists(LOG_FILE):
        print(f"  {C.BOLD}▸ Log File ({LOG_FILE}){C.RESET}")
        try:
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()

            if lines:
                for line in lines[-10:]:
                    try:
                        entry = json.loads(line.strip())
                        ts = entry.get("timestamp", "")
                        etype = entry.get("event_type", "")
                        wid = entry.get("worker_id", "")
                        content = entry.get("content", entry.get("status", ""))
                        print(f"  {C.DIM}{ts}{C.RESET}  {etype:<15} {wid:<20} {content}")
                    except Exception:
                        print(f"  {line.strip()}")
                print(f"  {C.DIM}(last 10/{len(lines)} entries — full log: {LOG_FILE}){C.RESET}")
            else:
                print(f"  {C.DIM}Log file is empty.{C.RESET}")
        except Exception as exc:
            print(f"  {C.RED}Error reading log: {exc}{C.RESET}")
    else:
        print(f"  {C.DIM}No log file yet.{C.RESET}")


# ── [3] Statistics ───────────────────────────────────────────
def show_stats():
    print(f"\n  {C.CYAN}{C.BOLD}═══ STATISTICS ═══{C.RESET}\n")
    print(f"  Total messages received : {C.BOLD}{_stats['messages_received']}{C.RESET}")
    print(f"  Heartbeats             : {_stats['heartbeats']}")
    print(f"  Worker messages        : {C.CYAN}{_stats['worker_messages']}{C.RESET}")
    print(f"  Task results           : {C.GREEN}{_stats['task_results']}{C.RESET}")
    print(f"  LWT events             : {C.RED}{_stats['lwt_events']}{C.RESET}")
    print(f"  Workers tracked        : {len(_workers)}")
    print()


# ── [6] Configuration ───────────────────────────────────────
def show_config():
    settings = get_settings()
    print(f"\n  {C.CYAN}{C.BOLD}═══ CURRENT CONFIGURATION ═══{C.RESET}\n")
    print(f"  MQTT_BROKER_HOST   = {settings.MQTT_BROKER_HOST}")
    print(f"  MQTT_BROKER_PORT   = {settings.MQTT_BROKER_PORT}")
    print(f"  MQTT_USERNAME      = {settings.MQTT_USERNAME or '(empty)'}")
    print(f"  MQTT_CLIENT_ID     = {settings.MQTT_CLIENT_ID}")
    print(f"  MQTT_KEEPALIVE     = {settings.MQTT_KEEPALIVE}s")
    print()
    print(f"  {C.DIM}Subscribe topics:{C.RESET}")
    print(f"    → worker/+/heartbeat")
    print(f"    → worker/+/status")
    print(f"    → worker/+/message")
    print(f"    → result/+")
    print()


# ── [9] MinIO Models ────────────────────────────────────────
def _get_minio_client() -> Minio:
    settings = get_settings()
    return Minio(
        endpoint=settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY,
        secure=settings.MINIO_SECURE,
    )


def _fmt_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


def show_minio_models():
    settings = get_settings()
    print(f"\n  {C.CYAN}{C.BOLD}═══ MinIO MODELS ═══{C.RESET}")
    print(f"  {C.DIM}Endpoint: {settings.MINIO_ENDPOINT}  │  Bucket: {settings.MINIO_BUCKET_MODELS}{C.RESET}\n")

    try:
        client = _get_minio_client()
    except Exception as exc:
        print(f"  {C.RED}✗ Cannot connect to MinIO: {exc}{C.RESET}")
        return

    model_types = ["embedding", "matching", "pad"]
    total_models = 0

    for mtype in model_types:
        # List objects under this type prefix
        try:
            objects = list(client.list_objects(
                settings.MINIO_BUCKET_MODELS,
                prefix=f"{mtype}/",
                recursive=True,
            ))
        except S3Error as exc:
            print(f"  {C.RED}✗ Error listing {mtype}: {exc}{C.RESET}")
            continue

        # Filter out .keep files
        files = [obj for obj in objects if not obj.object_name.endswith(".keep")]

        # Color per type
        type_colors = {"embedding": C.MAGENTA, "matching": C.BLUE, "pad": C.YELLOW}
        tc = type_colors.get(mtype, C.WHITE)

        print(f"  {tc}{C.BOLD}┌─ {mtype.upper()}{C.RESET}  {C.DIM}({len(files)} file{'s' if len(files) != 1 else ''}){C.RESET}")

        if not files:
            print(f"  {tc}│{C.RESET}  {C.DIM}(empty — upload models to {mtype}/<name>_v<ver>/model.onnx){C.RESET}")
            print(f"  {tc}└{'─' * 50}{C.RESET}")
            print()
            continue

        # Group by version folder: embedding/embedding_v1/model.onnx → embedding_v1
        versions: dict[str, list] = {}
        for obj in files:
            parts = obj.object_name.split("/")
            ver = parts[1] if len(parts) >= 2 else "(root)"
            versions.setdefault(ver, []).append(obj)

        for ver, ver_files in versions.items():
            print(f"  {tc}│{C.RESET}")
            print(f"  {tc}├── {C.BOLD}{ver}{C.RESET}")

            for obj in ver_files:
                fname = obj.object_name.split("/")[-1]
                size = _fmt_size(obj.size) if obj.size else "—"
                modified = obj.last_modified.strftime("%Y-%m-%d %H:%M") if obj.last_modified else "—"
                print(f"  {tc}│{C.RESET}     📦 {C.WHITE}{fname}{C.RESET}  {C.DIM}({size}  •  {modified}){C.RESET}")
                total_models += 1

        print(f"  {tc}└{'─' * 50}{C.RESET}")
        print()

    print(f"  {C.DIM}Total model files: {total_models}{C.RESET}")
    print(f"  {C.DIM}Upload via MinIO Console: http://{settings.MINIO_ENDPOINT.replace(':9000', ':9001')}{C.RESET}")
    print()


# ── [10] Deploy Model to Worker ─────────────────────────────
def deploy_model_to_worker():
    settings = get_settings()
    print(f"\n  {C.CYAN}{C.BOLD}═══ DEPLOY MODEL TO WORKER ═══{C.RESET}\n")

    if not _connected:
        print(f"  {C.RED}✗ Not connected to MQTT!{C.RESET}")
        return

    # 1. Show available workers
    online_workers = [
        (wid, info) for wid, info in _workers.items()
        if info.get("status") in ("idle", "online", "busy")
    ]
    if not online_workers:
        print(f"  {C.RED}✗ No online workers available.{C.RESET}")
        return

    print(f"  {C.DIM}Available workers:{C.RESET}")
    for i, (wid, info) in enumerate(online_workers, 1):
        status = info.get("status", "unknown")
        loaded = info.get("loaded_models", {})
        models_str = ", ".join(loaded.values()) if loaded else "(none)"
        print(f"    {C.BOLD}[{i}]{C.RESET} {wid} ({status}) ─ models: {models_str}")

    print()
    try:
        idx = int(input(f"  {C.YELLOW}▸ Select worker [1-{len(online_workers)}]: {C.RESET}").strip()) - 1
        if idx < 0 or idx >= len(online_workers):
            print(f"  {C.RED}✗ Invalid selection{C.RESET}")
            return
    except (ValueError, EOFError):
        print(f"  {C.RED}✗ Invalid input{C.RESET}")
        return

    target_worker_id = online_workers[idx][0]

    # 2. Connect to MinIO and list model types
    try:
        minio_client = _get_minio_client()
    except Exception as exc:
        print(f"  {C.RED}✗ Cannot connect to MinIO: {exc}{C.RESET}")
        return

    model_types = ["embedding", "matching", "pad"]
    print(f"\n  {C.DIM}Model types:{C.RESET}")
    for i, mt in enumerate(model_types, 1):
        print(f"    {C.BOLD}[{i}]{C.RESET} {mt}")

    try:
        mt_idx = int(input(f"  {C.YELLOW}▸ Select type [1-{len(model_types)}]: {C.RESET}").strip()) - 1
        if mt_idx < 0 or mt_idx >= len(model_types):
            print(f"  {C.RED}✗ Invalid selection{C.RESET}")
            return
    except (ValueError, EOFError):
        print(f"  {C.RED}✗ Invalid input{C.RESET}")
        return

    selected_type = model_types[mt_idx]

    # 3. List available models for this type
    try:
        objects = list(minio_client.list_objects(
            settings.MINIO_BUCKET_MODELS,
            prefix=f"{selected_type}/",
            recursive=True,
        ))
        # Filter .keep and group by version folder
        model_files = [
            obj for obj in objects
            if not obj.object_name.endswith(".keep")
            and obj.object_name.endswith(".onnx")
        ]
    except Exception as exc:
        print(f"  {C.RED}✗ Error listing models: {exc}{C.RESET}")
        return

    if not model_files:
        print(f"  {C.RED}✗ No .onnx models found in {selected_type}/ on MinIO{C.RESET}")
        print(f"  {C.DIM}Upload models via MinIO Console: http://{settings.MINIO_ENDPOINT.replace(':9000', ':9001')}{C.RESET}")
        return

    print(f"\n  {C.DIM}Available models ({selected_type}):{C.RESET}")
    for i, obj in enumerate(model_files, 1):
        size = _fmt_size(obj.size) if obj.size else "—"
        # Extract model name from path: embedding/embedding_v1/model.onnx → embedding_v1
        parts = obj.object_name.split("/")
        display_name = parts[1] if len(parts) >= 2 else obj.object_name
        print(f"    {C.BOLD}[{i}]{C.RESET} {display_name}  {C.DIM}({size}){C.RESET}")

    try:
        m_idx = int(input(f"  {C.YELLOW}▸ Select model [1-{len(model_files)}]: {C.RESET}").strip()) - 1
        if m_idx < 0 or m_idx >= len(model_files):
            print(f"  {C.RED}✗ Invalid selection{C.RESET}")
            return
    except (ValueError, EOFError):
        print(f"  {C.RED}✗ Invalid input{C.RESET}")
        return

    selected_model = model_files[m_idx]
    s3_path = selected_model.object_name
    path_parts = s3_path.split("/")
    model_name = path_parts[1] if len(path_parts) >= 2 else s3_path
    # Extract version: embedding_v1 → v1
    version = model_name.split("_")[-1] if "_" in model_name else "unknown"

    # 4. Generate presigned URL (using public endpoint if configured)
    try:
        from datetime import timedelta
        # If workers are remote, use public endpoint for signing
        if settings.MINIO_PUBLIC_ENDPOINT:
            public_client = Minio(
                endpoint=settings.MINIO_PUBLIC_ENDPOINT,
                access_key=settings.MINIO_ACCESS_KEY,
                secret_key=settings.MINIO_SECRET_KEY,
                secure=settings.MINIO_SECURE,
            )
            download_url = public_client.presigned_get_object(
                settings.MINIO_BUCKET_MODELS,
                s3_path,
                expires=timedelta(hours=2),
            )
        else:
            download_url = minio_client.presigned_get_object(
                settings.MINIO_BUCKET_MODELS,
                s3_path,
                expires=timedelta(hours=2),
            )
    except Exception as exc:
        print(f"  {C.RED}✗ Failed to create presigned URL: {exc}{C.RESET}")
        return

    # 5. Publish model update command via MQTT
    payload = json.dumps({
        "model_type": selected_type,
        "model_name": model_name,
        "version": version,
        "download_url": download_url,
        "s3_path": s3_path,
    })

    topic = f"task/{target_worker_id}/model/update"
    client = _get_client()
    result = client.publish(topic, payload, qos=1)

    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        print(f"\n  {C.GREEN}✓ Deploy command sent!{C.RESET}")
        print(f"    Worker  : {target_worker_id}")
        print(f"    Model   : {selected_type}/{model_name}")
        print(f"    Topic   : {topic}")
        print(f"  {C.DIM}Worker is downloading the model...{C.RESET}")
    else:
        print(f"  {C.RED}✗ Failed to send deploy command{C.RESET}")


# ── [7] Reconnect ───────────────────────────────────────────
def reconnect():
    print(f"\n  {C.CYAN}{C.BOLD}═══ RECONNECT ═══{C.RESET}\n")

    if _connected:
        print(f"  {C.YELLOW}Disconnecting...{C.RESET}")
        _disconnect()
        time.sleep(1)

    print(f"  {C.YELLOW}Reconnecting...{C.RESET}")
    _connect()
    if _connected:
        print(f"  {C.GREEN}✓ Reconnected successfully!{C.RESET}")
    else:
        print(f"  {C.RED}✗ Reconnection failed{C.RESET}")


# ── Main CLI Loop ───────────────────────────────────────────
def run_cli():
    settings = get_settings()

    clear_screen()
    print(f"\n  {C.CYAN}{C.BOLD}🎛️  Orchestrator CLI starting...{C.RESET}")
    print(f"  {C.DIM}Broker: {settings.MQTT_BROKER_HOST}:{settings.MQTT_BROKER_PORT}{C.RESET}")
    print()

    print(f"  {C.YELLOW}Connecting to MQTT broker...{C.RESET}")
    _connect()

    if _connected:
        print(f"  {C.GREEN}✓ MQTT connected!{C.RESET}")
    else:
        print(f"  {C.RED}✗ Could not connect to MQTT{C.RESET}")
        print(f"  {C.DIM}You can try reconnect from the menu{C.RESET}")

    print()
    input(f"  {C.DIM}Press Enter to open main menu...{C.RESET}")

    clear_screen()
    print_banner()

    actions = {
        "1": show_workers,
        "2": show_event_log,
        "3": show_stats,
        "4": show_config,
        "5": reconnect,
        "6": lambda: (clear_screen(), print_banner()),
        "7": show_minio_models,
        "8": deploy_model_to_worker,
    }

    while True:
        print_menu()
        try:
            choice = input(f"\n  {C.YELLOW}{C.BOLD}▸ Select [0-8]: {C.RESET}").strip()
        except (KeyboardInterrupt, EOFError):
            choice = "0"

        if choice == "0":
            print(f"\n  {C.DIM}Exiting...{C.RESET}")
            break

        action = actions.get(choice)
        if action:
            action()
            input(f"\n  {C.DIM}Press Enter to continue...{C.RESET}")
            clear_screen()
            print_banner()
        else:
            print(f"  {C.RED}Invalid choice!{C.RESET}")

    _disconnect()
    print(f"  {C.GREEN}👋 Orchestrator CLI stopped.{C.RESET}\n")


if __name__ == "__main__":
    run_cli()
