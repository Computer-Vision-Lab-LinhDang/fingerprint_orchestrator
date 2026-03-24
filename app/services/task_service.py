from __future__ import annotations

import logging
from typing import Optional

from app.schemas.mqtt_payloads import TaskResult

logger = logging.getLogger(__name__)

# Pending results — store results until workflow retrieves them
_pending_results: dict[str, TaskResult] = {}


def store_result(task_id: str, result: TaskResult) -> None:
    """Store a task result for later retrieval."""
    _pending_results[task_id] = result
    logger.debug("Stored result for task '%s'", task_id)


def get_task_result(task_id: str) -> Optional[TaskResult]:
    """Get a task result without removing it."""
    return _pending_results.get(task_id)


def pop_task_result(task_id: str) -> Optional[TaskResult]:
    """Get and remove a task result."""
    return _pending_results.pop(task_id, None)
