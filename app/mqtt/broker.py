from __future__ import annotations

import logging
from typing import Optional, Callable, Awaitable

import aiomqtt

from app.core.config import get_settings

logger = logging.getLogger(__name__)

MessageCallback = Callable[[aiomqtt.Message], Awaitable[None]]


class MQTTBroker:
    def __init__(self) -> None:
        self._settings = get_settings()
        self._client: Optional[aiomqtt.Client] = None
        self._connected: bool = False
        self._subscriptions: list[tuple[str, int]] = []
        self._message_handler: Optional[MessageCallback] = None

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def client(self) -> Optional[aiomqtt.Client]:
        return self._client

    # ── Connect ──────────────────────────────────────────────
    def create_client(self) -> aiomqtt.Client:
        return aiomqtt.Client(
            hostname=self._settings.MQTT_BROKER_HOST,
            port=self._settings.MQTT_BROKER_PORT,
            username=self._settings.MQTT_USERNAME or None,
            password=self._settings.MQTT_PASSWORD or None,
            identifier=self._settings.MQTT_CLIENT_ID,
            keepalive=self._settings.MQTT_KEEPALIVE,
        )

    # ── Subscribe ────────────────────────────────────────────
    def add_subscription(self, topic: str, qos: int = 1) -> None:
        self._subscriptions.append((topic, qos))

    async def subscribe_all(self, client: aiomqtt.Client) -> None:
        for topic, qos in self._subscriptions:
            await client.subscribe(topic, qos=qos)
            logger.info("Subscribed: %s (QoS=%d)", topic, qos)
        self._connected = True

    # ── Message handler ──────────────────────────────────────
    def set_message_handler(self, handler: MessageCallback) -> None:
        self._message_handler = handler

    async def process_messages(self, client: aiomqtt.Client) -> None:
        async for message in client.messages:
            if self._message_handler:
                try:
                    await self._message_handler(message)
                except Exception as exc:
                    logger.error("Error processing message from '%s': %s", message.topic, exc)

    # ── Publish ──────────────────────────────────────────────
    async def publish(
        self,
        client: aiomqtt.Client,
        topic: str,
        payload: str | bytes,
        qos: int = 1,
        retain: bool = False,
    ) -> None:
        await client.publish(topic, payload=payload, qos=qos, retain=retain)
        logger.debug("Published to '%s': %s bytes", topic, len(payload) if payload else 0)


# ── Singleton ────────────────────────────────────────────────
_broker: Optional[MQTTBroker] = None


def get_mqtt_broker() -> MQTTBroker:
    global _broker
    if _broker is None:
        _broker = MQTTBroker()
    return _broker
