# TODO: MQTT Publisher — enable when task dispatch is ready

from __future__ import annotations

import json
import logging
from typing import Optional

import aiomqtt

from app.core.config import get_settings
from app.schemas.payload import TaskPayload, MatchPayload, TaskType

logger = logging.getLogger(__name__)


class MQTTPublisher:
    def __init__(self) -> None:
        self._settings = get_settings()

    def _build_topic(self, worker_id: str, task_type: TaskType) -> str:
        return f"task/{worker_id}/{task_type.value}"

    # ── Send Embed task ──────────────────────────────────────
    async def send_embed_task(
        self,
        client: aiomqtt.Client,
        worker_id: str,
        payload: TaskPayload,
    ) -> str:
        """Send embedding task to a specific worker. Returns task_id."""
        topic = self._build_topic(worker_id, TaskType.EMBED)
        message = payload.model_dump_json()

        await client.publish(topic, payload=message, qos=1)
        logger.info(
            "Sent embed task '%s' to worker '%s' on topic '%s'",
            payload.task_id, worker_id, topic,
        )
        return payload.task_id

    # ── Send Match task ──────────────────────────────────────
    async def send_match_task(
        self,
        client: aiomqtt.Client,
        worker_id: str,
        payload: MatchPayload,
    ) -> str:
        """Send matching task to a specific worker. Returns task_id."""
        topic = self._build_topic(worker_id, TaskType.MATCH)
        message = payload.model_dump_json()

        await client.publish(topic, payload=message, qos=1)
        logger.info(
            "Sent match task '%s' to worker '%s' on topic '%s'",
            payload.task_id, worker_id, topic,
        )
        return payload.task_id


# ── Singleton ────────────────────────────────────────────────
_publisher: Optional[MQTTPublisher] = None


def get_publisher() -> MQTTPublisher:
    global _publisher
    if _publisher is None:
        _publisher = MQTTPublisher()
    return _publisher
