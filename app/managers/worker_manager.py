from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from app.core.config import get_settings
from app.core.exceptions import NoWorkerAvailableError
from app.schemas.payload import WorkerStatus

logger = logging.getLogger(__name__)


@dataclass
class WorkerInfo:
    worker_id: str
    status: WorkerStatus = WorkerStatus.OFFLINE
    last_heartbeat: float = 0.0
    gpu_memory_used_mb: Optional[float] = None
    gpu_memory_total_mb: Optional[float] = None
    current_task_id: Optional[str] = None
    task_count: int = 0


class WorkerManager:
    def __init__(self) -> None:
        self._workers: dict[str, WorkerInfo] = {}
        self._settings = get_settings()
        self._heartbeat_timeout = self._settings.WORKER_HEARTBEAT_TIMEOUT

    # ── Properties ───────────────────────────────────────────
    @property
    def workers(self) -> dict[str, WorkerInfo]:
        return self._workers

    @property
    def active_count(self) -> int:
        self._check_timeouts()
        return sum(
            1 for w in self._workers.values()
            if w.status in (WorkerStatus.IDLE, WorkerStatus.BUSY, WorkerStatus.ONLINE)
        )

    # ── Heartbeat ────────────────────────────────────────────
    def update_heartbeat(
        self,
        worker_id: str,
        status: WorkerStatus,
        gpu_memory_used: Optional[float] = None,
        gpu_memory_total: Optional[float] = None,
        current_task_id: Optional[str] = None,
    ) -> None:
        now = time.time()

        if worker_id not in self._workers:
            self._workers[worker_id] = WorkerInfo(worker_id=worker_id)
            logger.info("New worker registered: %s", worker_id)

        worker = self._workers[worker_id]
        worker.status = status
        worker.last_heartbeat = now
        worker.gpu_memory_used_mb = gpu_memory_used
        worker.gpu_memory_total_mb = gpu_memory_total
        worker.current_task_id = current_task_id

    # ── Offline ──────────────────────────────────────────────
    def mark_offline(self, worker_id: str) -> None:
        if worker_id in self._workers:
            self._workers[worker_id].status = WorkerStatus.OFFLINE
            self._workers[worker_id].current_task_id = None
            logger.warning("Worker '%s' marked OFFLINE.", worker_id)

    def mark_busy(self, worker_id: str, task_id: str) -> None:
        if worker_id in self._workers:
            self._workers[worker_id].status = WorkerStatus.BUSY
            self._workers[worker_id].current_task_id = task_id
            self._workers[worker_id].task_count += 1

    # ── Select worker ───────────────────────────────────────
    def select_idle_worker(self, task_type: str = "unknown") -> str:
        self._check_timeouts()

        idle_workers = [
            w for w in self._workers.values()
            if w.status in (WorkerStatus.IDLE, WorkerStatus.ONLINE)
        ]

        if not idle_workers:
            raise NoWorkerAvailableError(task_type)

        # Prefer worker with fewest tasks (simple round-robin)
        idle_workers.sort(key=lambda w: w.task_count)
        selected = idle_workers[0]

        logger.info(
            "Selected worker '%s' for task '%s' (completed %d tasks)",
            selected.worker_id, task_type, selected.task_count,
        )
        return selected.worker_id

    # ── Timeout check ────────────────────────────────────────
    def _check_timeouts(self) -> None:
        now = time.time()
        for worker in self._workers.values():
            if worker.status != WorkerStatus.OFFLINE:
                elapsed = now - worker.last_heartbeat
                if elapsed > self._heartbeat_timeout:
                    logger.warning(
                        "Worker '%s' timed out (%.0fs without heartbeat).",
                        worker.worker_id, elapsed,
                    )
                    worker.status = WorkerStatus.OFFLINE
                    worker.current_task_id = None

    # ── Summary ──────────────────────────────────────────────
    def get_summary(self) -> list[dict]:
        self._check_timeouts()
        return [
            {
                "worker_id": w.worker_id,
                "status": w.status.value,
                "last_heartbeat": w.last_heartbeat,
                "gpu_memory_used_mb": w.gpu_memory_used_mb,
                "gpu_memory_total_mb": w.gpu_memory_total_mb,
                "current_task_id": w.current_task_id,
                "task_count": w.task_count,
            }
            for w in self._workers.values()
        ]


# ── Singleton ────────────────────────────────────────────────
_manager: Optional[WorkerManager] = None


def get_worker_manager() -> WorkerManager:
    global _manager
    if _manager is None:
        _manager = WorkerManager()
    return _manager
