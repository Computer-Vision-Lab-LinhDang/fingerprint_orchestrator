# TODO: Enable when API routes are ready

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request

from app.schemas.requests import VerifyRequest
from app.schemas.responses import VerifyResponse, MatchItem
from app.services.verification_service import verify_fingerprint
from app.core.exceptions import (
    NoWorkerAvailableError,
    TaskTimeoutError,
    TaskFailedError,
    StorageError,
    DatabaseError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/verify", tags=["Verification"])


@router.post("/", response_model=VerifyResponse)
async def verify(request: Request, body: VerifyRequest):
    """
    Verify a fingerprint — search for matching users.
    Flow: Upload image → Embed via GPU worker → Query vector DB → Return results.
    """
    mqtt_client = request.app.state.mqtt_client

    if mqtt_client is None:
        raise HTTPException(
            status_code=503,
            detail="MQTT not connected. System not ready.",
        )

    try:
        result = await verify_fingerprint(
            mqtt_client=mqtt_client,
            image_base64=body.image_base64,
            top_k=body.top_k,
            threshold=body.threshold,
        )

        matches = [MatchItem(**m) for m in result.get("matches", [])]

        return VerifyResponse(
            success=True,
            matched=result.get("matched", False),
            message=(
                f"Found {len(matches)} matching result(s)."
                if matches
                else "No matching results found."
            ),
            matches=matches,
        )

    except NoWorkerAvailableError:
        raise HTTPException(
            status_code=503,
            detail="No GPU worker available. Please try again later.",
        )
    except TaskTimeoutError as exc:
        raise HTTPException(
            status_code=504,
            detail=f"Timeout: {exc}",
        )
    except TaskFailedError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Task failed: {exc}",
        )
    except (StorageError, DatabaseError) as exc:
        logger.error("Storage/DB error during verification: %s", exc)
        raise HTTPException(
            status_code=500,
            detail=f"System error: {exc}",
        )
