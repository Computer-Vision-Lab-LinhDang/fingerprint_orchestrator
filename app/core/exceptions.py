class OrchestratorError(Exception):
    pass


# ── Worker ───────────────────────────────────────────────────
class NoWorkerAvailableError(OrchestratorError):
    def __init__(self, task_type: str = "unknown"):
        self.task_type = task_type
        super().__init__(
            f"No idle worker available for task '{task_type}'. "
            "Please try again later."
        )


class WorkerTimeoutError(OrchestratorError):
    def __init__(self, worker_id: str, timeout: int):
        self.worker_id = worker_id
        self.timeout = timeout
        super().__init__(
            f"Worker '{worker_id}' did not respond within {timeout}s."
        )


# ── Task ─────────────────────────────────────────────────────
class TaskTimeoutError(OrchestratorError):
    def __init__(self, task_id: str, timeout: int):
        self.task_id = task_id
        self.timeout = timeout
        super().__init__(
            f"Task '{task_id}' timed out after {timeout}s."
        )


class TaskFailedError(OrchestratorError):
    def __init__(self, task_id: str, reason: str = ""):
        self.task_id = task_id
        self.reason = reason
        super().__init__(
            f"Task '{task_id}' failed: {reason}"
        )


# ── Storage / DB ─────────────────────────────────────────────
class StorageError(OrchestratorError):
    pass


class DatabaseError(OrchestratorError):
    pass
