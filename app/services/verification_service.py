# TODO: Verification service — not yet implemented
#
# Full pipeline when ready:
#   Step 1: Upload fingerprint image to MinIO
#   Step 2: Select idle GPU worker → send embed task
#   Step 3: Wait for embedding vector result
#   Step 4: Query DB with pgvector cosine similarity
#   Step 5: Return matching results
#
# Dependencies needed:
#   - StorageRepository (app/repositories/storage_repo.py)
#   - FingerprintRepository (app/repositories/fingerprint_repo.py)
#   - MQTTPublisher (app/mqtt/publisher.py)
#   - WorkerService (app/services/worker_service.py)

import logging

logger = logging.getLogger(__name__)


async def verify_fingerprint(**kwargs) -> dict:
    """
    Placeholder — prints pipeline steps.
    TODO: Implement when MinIO, DB, and GPU workers are ready.
    """
    logger.info("[VERIFY] Step 1: Upload image to MinIO              → TODO")
    logger.info("[VERIFY] Step 2: Select idle worker → send embed    → TODO")
    logger.info("[VERIFY] Step 3: Wait for embedding result          → TODO")
    logger.info("[VERIFY] Step 4: Query DB (pgvector similarity)     → TODO")
    logger.info("[VERIFY] Step 5: Return matching results            → TODO")

    return {
        "matched": False,
        "matches": [],
        "processing_time_ms": 0,
    }
