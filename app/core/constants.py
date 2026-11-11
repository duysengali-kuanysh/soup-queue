from enum import StrEnum


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_STATUSES = {JobStatus.SUCCEEDED.value, JobStatus.FAILED.value, JobStatus.CANCELLED.value}
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_LEASE_DURATION_SECONDS = 30
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 10
