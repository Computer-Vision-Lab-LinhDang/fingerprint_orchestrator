"""Sync endpoint: provide full data dump for worker synchronization."""

from __future__ import annotations

import logging

from fastapi import APIRouter

from app.repositories.user_repo import get_user_repo
from app.repositories.fingerprint_repo import get_fingerprint_repo

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["Sync"])


@router.get("/sync/full")
async def sync_full():
    """Return all users and fingerprints (with embeddings) for worker sync.

    Workers call this endpoint to pull the complete dataset from the
    Orchestrator and rebuild their local SQLite + FAISS index.
    """
    user_repo = get_user_repo()
    fp_repo = get_fingerprint_repo()

    users = await user_repo.list_all()
    fingerprints = await fp_repo.list_all_with_embeddings()

    return {
        "users": [
            {
                "user_id": u["user_id"],
                "employee_id": u["employee_id"],
                "full_name": u["full_name"],
                "department": u.get("department", ""),
                "role": u.get("role", "user"),
                "is_active": u.get("is_active", True),
            }
            for u in users
        ],
        "fingerprints": [
            {
                "fingerprint_id": fp["fingerprint_id"],
                "user_id": fp["user_id"],
                "finger_index": fp["finger_index"],
                "embedding": fp.get("embedding", []),
                "model_version": fp.get("model_version", "default"),
                "quality_score": fp.get("quality_score", 0),
                "image_path": fp.get("image_path", ""),
                "image_hash": fp.get("image_hash", ""),
            }
            for fp in fingerprints
        ],
    }
