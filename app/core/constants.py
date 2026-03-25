"""
Centralized constants — MQTT topics, defaults, etc.
"""

# ── MQTT Topic Patterns ─────────────────────────────────────
TOPIC_WORKER_HEARTBEAT = "worker/{worker_id}/heartbeat"
TOPIC_WORKER_STATUS = "worker/{worker_id}/status"
TOPIC_WORKER_MESSAGE = "worker/{worker_id}/message"
TOPIC_MODEL_UPDATE = "task/{worker_id}/model/update"
TOPIC_MODEL_STATUS = "worker/{worker_id}/model/status"
TOPIC_TASK = "task/{worker_id}/{task_type}"
TOPIC_RESULT = "result/{task_id}"

# Subscribe patterns (with wildcards)
SUB_HEARTBEAT = "worker/+/heartbeat"
SUB_STATUS = "worker/+/status"
SUB_MESSAGE = "worker/+/message"
SUB_MODEL_STATUS = "worker/+/model/status"
SUB_RESULT = "result/+"

# ── Defaults ────────────────────────────────────────────────
DEFAULT_HEARTBEAT_TIMEOUT = 30
DEFAULT_TASK_TIMEOUT = 60
DEFAULT_TOP_K = 5
DEFAULT_SIMILARITY_THRESHOLD = 0.7
