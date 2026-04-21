# Fingerprint Orchestrator

Central server that manages Jetson workers via MQTT, stores master data in PostgreSQL/pgvector, and serves the dashboard/API via FastAPI.

## Runtime dependencies

- Python 3.10+
- `uv`
- Mosquitto MQTT broker
- PostgreSQL + pgvector
- MinIO

If you already have these containers running:

```bash
docker ps
```

and you see:

- PostgreSQL/pgvector on `localhost:5433`
- MinIO on `localhost:9000`

then the remaining host dependency is usually just Mosquitto.

## Installation with `uv sync`

```bash
cd fingerprint_orchestrator
cp .env.example .env
./scripts/setup_orchestrator_env.sh
```

If the script is not executable yet:

```bash
chmod +x scripts/setup_orchestrator_env.sh
```

For daily updates after `git pull`:

```bash
cd fingerprint_orchestrator
RECREATE_VENV=0 ./scripts/setup_orchestrator_env.sh
```

This refreshes the installed package from the current source tree without recreating `venv`.

## Manual `uv` workflow

```bash
cd fingerprint_orchestrator
uv venv --python python3 venv
source venv/bin/activate
uv sync --active --no-editable \
  --refresh-package fingerprint-orchestrator \
  --reinstall-package fingerprint-orchestrator
```

## Configuration

Copy `.env.example` to `.env` and set the important values:

```env
MQTT_BROKER_HOST=localhost
MQTT_BROKER_PORT=1883

DB_HOST=localhost
DB_PORT=5433
DB_USER=fingerprint
DB_PASSWORD=fingerprint123
DB_NAME=fingerprint_db

MINIO_ENDPOINT=localhost:9000
MINIO_PUBLIC_ENDPOINT=100.100.108.30:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
MINIO_BUCKET_MODELS=fingerprint-models
MINIO_BUCKET_IMAGES=fingerprint-images
MINIO_SECURE=false
```

Use `MINIO_PUBLIC_ENDPOINT` as the address Jetson workers can reach, for example your Tailscale/LAN IP.

## Run

API server:

```bash
source venv/bin/activate
fingerprint-orchestrator-api
```

CLI:

```bash
source venv/bin/activate
fingerprint-orchestrator-cli
```

You can still run the old entrypoints if needed:

```bash
python -m app.main
python -m app.cli
```

## MQTT topics used by orchestrator

Subscribe:

- `worker/+/heartbeat`
- `worker/+/status`
- `worker/+/message`
- `worker/+/enrolled`
- `worker/+/enrollment/upload/status`
- `worker/+/model/status`
- `result/+`
- `edge/+/register`
- `edge/+/verify`

Publish:

- `task/{worker_id}/embed`
- `task/{worker_id}/match`
- `task/{worker_id}/message`
- `task/{worker_id}/model/update`
- `task/{worker_id}/sync`
- `task/{worker_id}/sync/check`
- `task/{worker_id}/enrollment/upload`

## Notes

- Worker enrollment images are now uploaded by the source worker through presigned MinIO PUT URLs.
- When a worker reconnects, orchestrator asks it to flush pending offline registrations via `task/{worker_id}/sync/check`.
- The dashboard/API schema initialization creates required PostgreSQL tables and the `vector` extension automatically at startup.
