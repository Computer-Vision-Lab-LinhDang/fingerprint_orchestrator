# Fingerprint Orchestrator

Central server that manages GPU workers via **MQTT** for fingerprint recognition tasks.
Includes a **FastAPI web server** and an **interactive CLI** for monitoring.

---

## Prerequisites

- Python 3.10+
- Mosquitto MQTT broker installed and running

## Installation

```bash
cd fingerprint_orchestrator

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Configuration

```bash
# Copy example env file
cp .env.example .env
```

Edit `.env` with your values:

```env
MQTT_BROKER_HOST=localhost         # IP of the Mosquitto broker
MQTT_BROKER_PORT=1883
MQTT_USERNAME=                     # Leave empty if allow_anonymous
MQTT_PASSWORD=                     # Leave empty if allow_anonymous
MQTT_CLIENT_ID=orchestrator-main
WORKER_HEARTBEAT_TIMEOUT=30        # Seconds before marking worker offline
```

| Variable                   | Description                                                             |
| -------------------------- | ----------------------------------------------------------------------- |
| `MQTT_BROKER_HOST`         | Mosquitto broker IP. Use `localhost` if broker runs on the same machine |
| `MQTT_BROKER_PORT`         | Default MQTT port: `1883`                                               |
| `MQTT_USERNAME`            | Leave empty if broker has `allow_anonymous true`                        |
| `MQTT_PASSWORD`            | Leave empty if broker has `allow_anonymous true`                        |
| `MQTT_CLIENT_ID`           | Unique client ID for the orchestrator                                   |
| `WORKER_HEARTBEAT_TIMEOUT` | Seconds without heartbeat before a worker is marked offline             |

## Mosquitto Broker Setup

```bash
# Install Mosquitto
sudo apt install mosquitto mosquitto-clients

# Allow external connections (for Jetson Nano workers)
echo 'listener 1883 0.0.0.0
allow_anonymous true' | sudo tee /etc/mosquitto/conf.d/external.conf

# Restart broker
sudo systemctl restart mosquitto

# Verify it's running
sudo systemctl status mosquitto
```

## Running

### Option 1: CLI Dashboard (recommended for development)

```bash
source venv/bin/activate
python -m app.cli
```

### Option 2: FastAPI Web Server

```bash
source venv/bin/activate
python -m app.main
```

Then visit `http://localhost:8000/health` or `http://localhost:8000/workers`.

## CLI Menu

```
╔══════════════════════════════════════════════════╗
║     🎛️  FINGERPRINT ORCHESTRATOR — CLI           ║
╚══════════════════════════════════════════════════╝

  Orchestrator  │  ● CONNECTED  │  Workers online: 1

  [1]  🖥️   Connected Workers
  [2]  📋  Recent Event Log
  [3]  ✉️   Send Message to Worker
  [4]  📈  Statistics
  [5]  📄  View Log File
  [6]  ⚙️   Current Configuration
  [7]  🔄  Reconnect MQTT
  [8]  🧹  Clear Screen
  [0]  🚪  Exit
```

| Option                         | Description                                                                            |
| ------------------------------ | -------------------------------------------------------------------------------------- |
| **[1] Connected Workers**      | Shows all detected workers with status (idle/busy/offline), last seen time, and uptime |
| **[2] Recent Event Log**       | Live event log: heartbeats, messages, task results, LWT events                         |
| **[3] Send Message to Worker** | Send a text message to a specific worker by ID                                         |
| **[4] Statistics**             | Total messages received, heartbeats, worker messages, task results, LWT events         |
| **[5] View Log File**          | Read the last 20 entries from `logs/worker_events.log` (persistent JSON log)           |
| **[6] Current Configuration**  | Display all current `.env` settings                                                    |
| **[7] Reconnect MQTT**         | Disconnect and reconnect to the MQTT broker                                            |

## MQTT Topics

| Direction | Topic                      | Description                         |
| --------- | -------------------------- | ----------------------------------- |
| Subscribe | `worker/+/heartbeat`       | Worker heartbeat (every 10s)        |
| Subscribe | `worker/+/status`          | Worker LWT (auto offline detection) |
| Subscribe | `worker/+/message`         | Messages from workers               |
| Subscribe | `result/+`                 | Task results from workers           |
| Publish   | `task/{worker_id}/embed`   | Send embed task to worker           |
| Publish   | `task/{worker_id}/match`   | Send match task to worker           |
| Publish   | `task/{worker_id}/message` | Send message to worker              |

## Log File

All worker events are logged to `logs/worker_events.log` in JSON format:

```json
{"timestamp": "2026-03-13 16:10:05", "event_type": "heartbeat", "worker_id": "jetson-nano-01", "status": "idle"}
{"timestamp": "2026-03-13 16:10:12", "event_type": "message", "worker_id": "jetson-nano-01", "content": "hello laptop"}
```

## Project Structure

```
fingerprint_orchestrator/
├── app/
│   ├── main.py              # FastAPI entry point + MQTT listener
│   ├── cli.py               # Interactive CLI dashboard
│   ├── core/config.py       # Settings from .env
│   ├── schemas/             # Pydantic models
│   ├── mqtt/
│   │   ├── broker.py        # MQTT connection manager (aiomqtt)
│   │   └── handlers.py      # Message handlers + file logger
│   └── managers/
│       └── worker_manager.py  # Worker status tracking
├── logs/                    # Auto-created log directory
├── .env
├── .env.example
├── requirements.txt
└── README.md
```
