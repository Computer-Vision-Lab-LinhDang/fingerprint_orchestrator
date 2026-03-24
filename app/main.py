from __future__ import annotations

import asyncio
import logging
import sys
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from app.core.config import get_settings
from app.mqtt.broker import get_mqtt_broker
from app.mqtt.handlers import on_message
from app.services.worker_service import get_worker_service
from app.routes.health import router as health_router

logger = logging.getLogger(__name__)

# ── Logging config ───────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# ── Lifespan ─────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logger.info("🚀 Starting %s ...", settings.APP_NAME)

    # TODO: Initialize MinIO buckets
    # from app.repositories.storage_repo import get_storage_repo
    # try:
    #     storage = get_storage_repo()
    #     storage.ensure_buckets()
    #     logger.info("✅ MinIO ready")
    # except Exception as exc:
    #     logger.error("❌ MinIO error: %s", exc)

    # TODO: Connect to Database (PostgreSQL + pgvector)
    # from app.db.database import get_database
    # db = get_database()
    # try:
    #     await db.connect()
    #     await db.init_schema()
    #     logger.info("✅ Database ready")
    # except Exception as exc:
    #     logger.error("❌ Database error: %s", exc)

    # ── Start MQTT listener (background task) ────────────────
    broker = get_mqtt_broker()
    broker.add_subscription("worker/+/heartbeat", qos=1)
    broker.add_subscription("worker/+/status", qos=1)
    broker.add_subscription("worker/+/message", qos=1)
    broker.add_subscription("result/+", qos=1)
    broker.set_message_handler(on_message)

    mqtt_task = asyncio.create_task(_run_mqtt(app, broker))
    logger.info("✅ MQTT listener running")

    yield

    # ── Shutdown ─────────────────────────────────────────────
    logger.info("🛑 Shutting down %s ...", settings.APP_NAME)
    mqtt_task.cancel()
    try:
        await mqtt_task
    except asyncio.CancelledError:
        pass
    # TODO: await db.disconnect()
    logger.info("👋 Shutdown complete.")


async def _run_mqtt(app: FastAPI, broker):
    while True:
        try:
            async with broker.create_client() as client:
                app.state.mqtt_client = client
                await broker.subscribe_all(client)
                logger.info("MQTT connected and subscribed successfully.")
                await broker.process_messages(client)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("MQTT disconnected: %s. Reconnecting in 5s...", exc)
            app.state.mqtt_client = None
            await asyncio.sleep(5)


# ── FastAPI App ──────────────────────────────────────────────
app = FastAPI(
    title="Fingerprint Orchestrator",
    description="Fingerprint recognition orchestrator — manages GPU workers via MQTT",
    version="1.0.0",
    lifespan=lifespan,
)

app.state.mqtt_client = None

# ── Routes ───────────────────────────────────────────────────
app.include_router(health_router)

# TODO: Include API routers when ready
# from app.routes.register import router as register_router
# from app.routes.verify import router as verify_router
# app.include_router(register_router, prefix="/api/v1")
# app.include_router(verify_router, prefix="/api/v1")


# ── Run ──────────────────────────────────────────────────────
if __name__ == "__main__":
    cli_mode = "--cli" in sys.argv

    if cli_mode:
        from app.cli import run_cli
        run_cli()
    else:
        settings = get_settings()
        try:
            uvicorn.run(
                "app.main:app",
                host=settings.APP_HOST,
                port=settings.APP_PORT,
                reload=settings.DEBUG,
            )
        except KeyboardInterrupt:
            print("\n👋 Bye!")
