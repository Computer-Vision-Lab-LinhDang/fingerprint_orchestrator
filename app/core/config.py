from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── FastAPI ──────────────────────────────────────────────
    APP_NAME: str = "Fingerprint Orchestrator"
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000
    DEBUG: bool = False

    # ── MQTT / Mosquitto ─────────────────────────────────────
    MQTT_BROKER_HOST: str = "localhost"
    MQTT_BROKER_PORT: int = 1883
    MQTT_USERNAME: str = ""
    MQTT_PASSWORD: str = ""
    MQTT_CLIENT_ID: str = "orchestrator-main"
    MQTT_KEEPALIVE: int = 60

    # ── MQTT Topics ──────────────────────────────────────────
    MQTT_TOPIC_HEARTBEAT: str = "worker/+/heartbeat"
    MQTT_TOPIC_LWT_PREFIX: str = "worker/{worker_id}/status"
    MQTT_TOPIC_TASK_PREFIX: str = "task/{worker_id}/{task_type}"
    MQTT_TOPIC_RESULT: str = "result/+"

    # ── Worker Management ────────────────────────────────────
    WORKER_HEARTBEAT_TIMEOUT: int = 30

    # TODO: MinIO settings (uncomment when ready)
    # MINIO_ENDPOINT: str = "192.168.1.100:9000"
    # MINIO_ACCESS_KEY: str = "minioadmin"
    # MINIO_SECRET_KEY: str = "minioadmin"
    # MINIO_BUCKET_MODELS: str = "fingerprint-models"
    # MINIO_BUCKET_IMAGES: str = "fingerprint-images"
    # MINIO_SECURE: bool = False

    # TODO: Database settings (uncomment when ready)
    # DB_HOST: str = "localhost"
    # DB_PORT: int = 5432
    # DB_USER: str = "postgres"
    # DB_PASSWORD: str = "postgres"
    # DB_NAME: str = "fingerprint_db"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
