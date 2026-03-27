"""
Verification service — full pipeline for fingerprint verification.

Flow:
  1. Edge sends image via MQTT (edge/{id}/verify)
  2. Upload image to MinIO
  3. Select idle worker → send embed task
  4. Worker returns embedding vector
  5. Fetch all fingerprint embeddings from DB
  6. Select idle worker → send match task (query_vector + candidate_vectors)
  7. Worker returns match results
  8. Send results back to edge device
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

_pending_verifications: dict = {}


def get_pending_verifications() -> dict:
    return _pending_verifications


async def start_verification(
    client,
    task_id: str,
    image_base64: str,
    image_filename: str = "",
    content_type: str = "image/tiff",
    edge_id: str = "",
) -> dict:
    """
    Start fingerprint verification pipeline.
    Called by MQTT handler when edge sends verify request.
    """
    # TODO Step 1: Upload image to MinIO
    # storage = get_storage_repo()
    # object_name = f"verify_{task_id}.tif"
    # storage.upload_image(filename=object_name, image_base64=image_base64, content_type=content_type)

    # TODO Step 2: Create presigned URL for worker
    # If MINIO_PUBLIC_ENDPOINT is set, use public client for remote workers
    # image_url = storage.get_presigned_url(object_name)

    # TODO Step 3: Select idle worker and send embed task
    # worker_svc = get_worker_service()
    # publisher = get_publisher()
    # worker_id = worker_svc.select_idle_worker(task_type="embed")
    # embed_payload = TaskPayload(task_id=task_id, task_type=TaskType.EMBED, image_url=image_url)
    # await publisher.send_embed_task(client, worker_id, embed_payload)

    # TODO Step 4: Track pending verification (will be continued in handle_verify_embed_result)
    # _pending_verifications[task_id] = {
    #     "task_id": task_id,
    #     "edge_id": edge_id,
    #     "image_path": object_name,
    #     "status": "embedding",
    # }

    logger.info("[VERIFY] TODO: Pipeline not implemented yet — task_id=%s", task_id)
    return {"task_id": task_id, "status": "not_implemented"}


async def handle_verify_embed_result(task_id: str, result: dict) -> Optional[dict]:
    """
    Called when worker returns embedding result for a verify task.
    Next step: fetch DB vectors and send match task.
    """
    # TODO Step 5: Get the verification context
    # verification = _pending_verifications.get(task_id)
    # if not verification:
    #     return None

    # TODO Step 6: Extract embedding vector from result
    # result_data = result.get("result", {})
    # query_vector = result_data.get("vector", [])
    # if not query_vector:
    #     _pending_verifications.pop(task_id, None)
    #     return {"task_id": task_id, "status": "failed", "error": "Empty embedding vector"}

    # TODO Step 7: Fetch all fingerprint embeddings from DB
    # fp_repo = get_fingerprint_repo()
    # All fingerprints with their embeddings:
    # db = get_database()
    # async with db.acquire() as conn:
    #     rows = await conn.fetch("""
    #         SELECT f.fingerprint_id, f.user_id, f.embedding::text,
    #                u.username, u.name as fullname
    #         FROM fingerprints f
    #         JOIN users u ON u.user_id = f.user_id
    #     """)
    # candidate_vectors = [parse_vector(row["embedding"]) for row in rows]
    # candidate_metadata = [{"fingerprint_id": row["fingerprint_id"], ...} for row in rows]

    # TODO Step 8: Select idle worker and send match task
    # match_task_id = str(uuid.uuid4())
    # match_payload = MatchPayload(
    #     task_id=match_task_id,
    #     query_vector=query_vector,
    #     candidate_vectors=candidate_vectors,
    #     top_k=5,
    #     threshold=0.7,
    # )
    # worker_id = worker_svc.select_idle_worker(task_type="match")
    # await publisher.send_match_task(client, worker_id, match_payload)

    # TODO Step 9: Update pending verification with match task info
    # verification["match_task_id"] = match_task_id
    # verification["status"] = "matching"
    # verification["candidate_metadata"] = candidate_metadata
    # _pending_verifications[match_task_id] = verification  # re-key by match task

    logger.info("[VERIFY] TODO: handle_verify_embed_result not implemented — task_id=%s", task_id)
    return None


async def handle_verify_match_result(task_id: str, result: dict) -> Optional[dict]:
    """
    Called when worker returns match result.
    Final step: send results back to edge device.
    """
    # TODO Step 10: Get the verification context
    # verification = _pending_verifications.pop(task_id, None)
    # if not verification:
    #     return None

    # TODO Step 11: Parse match results
    # result_data = result.get("result", {})
    # raw_matches = result_data.get("matches", [])
    # candidate_metadata = verification.get("candidate_metadata", [])

    # TODO Step 12: Combine match scores with user metadata
    # matches = []
    # for match in raw_matches:
    #     idx = match.get("index")
    #     similarity = match.get("similarity", 0)
    #     if idx is not None and idx < len(candidate_metadata):
    #         meta = candidate_metadata[idx]
    #         matches.append({
    #             "fingerprint_id": meta["fingerprint_id"],
    #             "username": meta["username"],
    #             "fullname": meta["fullname"],
    #             "similarity": similarity,
    #         })

    # TODO Step 13: Return final result (handler will send to edge)
    # return {
    #     "task_id": verification["task_id"],
    #     "status": "completed",
    #     "matched": len(matches) > 0,
    #     "matches": matches,
    #     "processing_time_ms": result.get("processing_time_ms", 0),
    # }

    logger.info("[VERIFY] TODO: handle_verify_match_result not implemented — task_id=%s", task_id)
    return None
