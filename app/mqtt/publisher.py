from __future__ import annotations

import logging
from typing import Optional

import aiomqtt

from app.core.config import get_settings
from app.schemas.mqtt_payloads import TaskPayload, MatchPayload, TaskType

logger = logging.getLogger(__name__)


class MQTTPublisher:
    def __init__(self) -> None:
        self._settings = get_settings()

    def _build_topic(self, worker_id: str, task_type: TaskType) -> str:
        return f"task/{worker_id}/{task_type.value}"

    async def send_embed_task(self, client: aiomqtt.Client, worker_id: str, payload: TaskPayload) -> str:
        topic = self._build_topic(worker_id, TaskType.EMBED)
        await client.publish(topic, payload=payload.model_dump_json(), qos=1)
        logger.info("Sent embed task '%s' → worker '%s'", payload.task_id, worker_id)
        return payload.task_id

    async def send_match_task(self, client: aiomqtt.Client, worker_id: str, payload: MatchPayload) -> str:
        topic = self._build_topic(worker_id, TaskType.MATCH)
        await client.publish(topic, payload=payload.model_dump_json(), qos=1)
        logger.info("Sent match task '%s' → worker '%s'", payload.task_id, worker_id)
        return payload.task_id


_publisher: Optional[MQTTPublisher] = None


def get_publisher() -> MQTTPublisher:
    global _publisher
    if _publisher is None:
        _publisher = MQTTPublisher()
    return _publisher
