# TODO: Registration service — not yet implemented
#
# Full pipeline when ready:
#   Step 1: Upload fingerprint image to MinIO
#   Step 2: Select idle GPU worker
#   Step 3: Send embed task via MQTT
#   Step 4: Wait for embedding vector result
#   Step 5: Save vector to PostgreSQL + pgvector
#
# Dependencies needed:
#   - StorageRepository (app/repositories/storage_repo.py)
#   - FingerprintRepository (app/repositories/fingerprint_repo.py)
#   - MQTTPublisher (app/mqtt/publisher.py)
#   - WorkerService (app/services/worker_service.py)

import logging

logger = logging.getLogger(__name__)


async def register_fingerprint(**kwargs) -> dict:
    """
    Placeholder — prints pipeline steps.
    TODO: Implement when MinIO, DB, and GPU workers are ready.
    """
    logger.info("[REGISTER] Step 1: Upload image to MinIO        → TODO")
    logger.info("[REGISTER] Step 2: Select idle GPU worker        → TODO")
    logger.info("[REGISTER] Step 3: Send embed task via MQTT      → TODO")
    logger.info("[REGISTER] Step 4: Wait for embedding result     → TODO")
    logger.info("[REGISTER] Step 5: Save vector to DB (pgvector)  → TODO")

    return {
        "fingerprint_id": "not-implemented",
        "user_id": kwargs.get("user_id", ""),
        "finger_id": kwargs.get("finger_id", ""),
        "processing_time_ms": 0,
    }
