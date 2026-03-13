from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime
from threading import Lock
from typing import Optional

import paho.mqtt.client as mqtt

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
            _workers[worker_id] = {
                "status": data.get("status", "unknown"),
                "last_seen": time.time(),
                "uptime": data.get("uptime_seconds"),
                "current_task": data.get("current_task_id"),
                "gpu_used": data.get("gpu_memory_used_mb"),
                "gpu_total": data.get("gpu_memory_total_mb"),
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
    print(f"  {C.BOLD}[2]{C.RESET}  📋  Recent Event Log")
    print(f"  {C.BOLD}[3]{C.RESET}  ✉️   Send Message to Worker")
    print(f"  {C.BOLD}[4]{C.RESET}  📈  Statistics")
    print(f"  {C.BOLD}[5]{C.RESET}  📄  View Log File")
    print(f"  {C.BOLD}[6]{C.RESET}  ⚙️   Current Configuration")
    print(f"  {C.BOLD}[7]{C.RESET}  🔄  Reconnect MQTT")
    print(f"  {C.BOLD}[8]{C.RESET}  🧹  Clear Screen")
    print(f"  {C.BOLD}[0]{C.RESET}  🚪  Exit")
    print(f"  {C.YELLOW}{'─' * 48}{C.RESET}")


# ── [1] Connected Workers ───────────────────────────────────
def show_workers():
    print(f"\n  {C.CYAN}{C.BOLD}═══ CONNECTED WORKERS ═══{C.RESET}\n")

    if not _workers:
        print(f"  {C.DIM}No workers detected yet. Waiting for heartbeats...{C.RESET}")
        return

    print(f"  {C.DIM}{'Worker ID':<20} {'Status':<12} {'Last Seen':<14} {'Uptime':<12} {'Task'}{C.RESET}")
    print(f"  {C.DIM}{'─' * 70}{C.RESET}")

    for wid, info in _workers.items():
        status = info.get("status", "unknown")
        if status in ("idle", "online"):
            s_color = C.GREEN
        elif status == "busy":
            s_color = C.YELLOW
        else:
            s_color = C.RED

        uptime = info.get("uptime")
        uptime_str = f"{int(uptime)}s" if uptime else "—"
        task = info.get("current_task") or "—"
        last = fmt_ago(info.get("last_seen"))

        print(f"  {C.WHITE}{wid:<20}{C.RESET} {s_color}{status:<12}{C.RESET} {last:<14} {uptime_str:<12} {task}")

    print()


# ── [2] Event Log ────────────────────────────────────────────
def show_event_log():
    print(f"\n  {C.CYAN}{C.BOLD}═══ RECENT EVENT LOG ═══{C.RESET}\n")

    with _lock:
        logs = list(_message_log)

    if not logs:
        print(f"  {C.DIM}No events yet.{C.RESET}")
        return

    print(f"  {C.DIM}{'Time':<10} {'Type':<12} {'Worker':<20} {'Details'}{C.RESET}")
    print(f"  {C.DIM}{'─' * 70}{C.RESET}")

    type_colors = {"heartbeat": C.DIM, "message": C.CYAN, "result": C.GREEN, "lwt": C.RED}

    for ts, event_type, worker_id, data in logs[-20:]:
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

    print(f"\n  {C.DIM}Showing {min(20, len(logs))}/{len(logs)} events{C.RESET}")


# ── [3] Send Message to Worker ──────────────────────────────
def send_message_to_worker():
    print(f"\n  {C.CYAN}{C.BOLD}═══ SEND MESSAGE TO WORKER ═══{C.RESET}\n")

    if not _connected:
        print(f"  {C.RED}✗ Not connected to MQTT!{C.RESET}")
        return

    # Show available workers
    if _workers:
        print(f"  {C.DIM}Available workers:{C.RESET}")
        for wid, info in _workers.items():
            status = info.get("status", "unknown")
            print(f"    • {wid} ({status})")
        print()

    worker_id = input(f"  {C.YELLOW}▸ Worker ID: {C.RESET}").strip()
    if not worker_id:
        print(f"  {C.RED}✗ Worker ID cannot be empty{C.RESET}")
        return

    message_text = input(f"  {C.YELLOW}▸ Message: {C.RESET}").strip()
    if not message_text:
        print(f"  {C.RED}✗ Message cannot be empty{C.RESET}")
        return

    payload = json.dumps({
        "sender": "orchestrator",
        "content": message_text,
        "message_id": str(uuid.uuid4()),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    })

    topic = f"task/{worker_id}/message"
    client = _get_client()
    result = client.publish(topic, payload, qos=1)
    if result.rc == mqtt.MQTT_ERR_SUCCESS:
        print(f"  {C.GREEN}✓ Message sent → {topic}{C.RESET}")
        _write_log("sent_message", worker_id, {"content": message_text})
    else:
        print(f"  {C.RED}✗ Send failed{C.RESET}")


# ── [4] Statistics ───────────────────────────────────────────
def show_stats():
    print(f"\n  {C.CYAN}{C.BOLD}═══ STATISTICS ═══{C.RESET}\n")
    print(f"  Total messages received : {C.BOLD}{_stats['messages_received']}{C.RESET}")
    print(f"  Heartbeats             : {_stats['heartbeats']}")
    print(f"  Worker messages        : {C.CYAN}{_stats['worker_messages']}{C.RESET}")
    print(f"  Task results           : {C.GREEN}{_stats['task_results']}{C.RESET}")
    print(f"  LWT events             : {C.RED}{_stats['lwt_events']}{C.RESET}")
    print(f"  Workers tracked        : {len(_workers)}")
    print()


# ── [5] View Log File ───────────────────────────────────────
def view_log_file():
    print(f"\n  {C.CYAN}{C.BOLD}═══ LOG FILE ({LOG_FILE}) ═══{C.RESET}\n")

    if not os.path.exists(LOG_FILE):
        print(f"  {C.DIM}Log file not found yet.{C.RESET}")
        return

    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()

        if not lines:
            print(f"  {C.DIM}Log file is empty.{C.RESET}")
            return

        # Show last 20 lines
        for line in lines[-20:]:
            try:
                entry = json.loads(line.strip())
                ts = entry.get("timestamp", "")
                etype = entry.get("event_type", "")
                wid = entry.get("worker_id", "")
                content = entry.get("content", entry.get("status", ""))
                print(f"  {C.DIM}{ts}{C.RESET}  {etype:<15} {wid:<20} {content}")
            except Exception:
                print(f"  {line.strip()}")

        print(f"\n  {C.DIM}Showing last {min(20, len(lines))}/{len(lines)} entries{C.RESET}")
        print(f"  {C.DIM}Full log: {LOG_FILE}{C.RESET}")

    except Exception as exc:
        print(f"  {C.RED}Error reading log: {exc}{C.RESET}")


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
        "3": send_message_to_worker,
        "4": show_stats,
        "5": view_log_file,
        "6": show_config,
        "7": reconnect,
        "8": lambda: (clear_screen(), print_banner()),
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
