# TODO: Enable when API routes are ready

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request

from app.schemas.requests import RegisterRequest
from app.schemas.responses import RegisterResponse
from app.services.registration_service import register_fingerprint
from app.core.exceptions import (
    NoWorkerAvailableError,
    TaskTimeoutError,
    TaskFailedError,
    StorageError,
    DatabaseError,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/register", tags=["Registration"])


@router.post("/", response_model=RegisterResponse)
async def register(request: Request, body: RegisterRequest):
    """
    Register a new fingerprint for a user.
    Flow: Upload image → Embed via GPU worker → Save vector to DB.
    """
    mqtt_client = request.app.state.mqtt_client

    if mqtt_client is None:
        raise HTTPException(
            status_code=503,
            detail="MQTT not connected. System not ready.",
        )

    try:
        result = await register_fingerprint(
            mqtt_client=mqtt_client,
            employee_id=body.employee_id,
            full_name=body.full_name,
            image_base64=body.image_base64,
            finger_index=body.finger_index,
            department=body.department,
            image_encrypted=body.image_encrypted,
        )
        return RegisterResponse(
            success=True,
            message="Fingerprint registered successfully.",
            user_id=result.get("employee_id", ""),
            fingerprint_id=result.get("fingerprint_id"),
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
        logger.error("Storage/DB error during registration: %s", exc)
        raise HTTPException(
            status_code=500,
            detail=f"System error: {exc}",
        )
