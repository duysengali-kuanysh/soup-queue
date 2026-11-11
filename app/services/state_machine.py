from app.core.constants import JobStatus
from app.core.exceptions import InvalidJobStateError

ALLOWED_TRANSITIONS = {
    JobStatus.QUEUED.value: {JobStatus.RUNNING.value, JobStatus.CANCELLED.value},
    JobStatus.RUNNING.value: {
        JobStatus.SUCCEEDED.value,
        JobStatus.FAILED.value,
        JobStatus.CANCELLED.value,
        JobStatus.QUEUED.value,
    },
    JobStatus.SUCCEEDED.value: set(),
    JobStatus.FAILED.value: set(),
    JobStatus.CANCELLED.value: set(),
}


def transition(job, new_status: str) -> None:
    allowed = ALLOWED_TRANSITIONS.get(job.status, set())
    if new_status not in allowed:
        raise InvalidJobStateError(f"Transition {job.status!r} -> {new_status!r} is not allowed")
    job.status = new_status
