import logging
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.core.constants import JobStatus
from app.repositories.job_repository import JobRepository
from app.services.state_machine import transition

logger = logging.getLogger(__name__)


async def recover_expired_jobs(session: AsyncSession, *, batch_size: int | None = None) -> int:
    """Requeue expired leases in a transaction; concurrent recoverers use SKIP LOCKED."""
    repo = JobRepository(session)
    expired = await repo.expired_for_update(batch_size or settings.recovery_batch_size)
    recovered = 0
    for job in expired:
        previous_worker = job.worker_id
        if job.cancel_requested:
            transition(job, JobStatus.CANCELLED.value)
            job.error = "Cancellation completed after worker lease expired"
            job.finished_at = func.now()
        elif job.attempt < job.max_attempts:
            transition(job, JobStatus.QUEUED.value)
            job.error = "Worker lease expired; job queued for retry"
            job.started_at = None
            job.finished_at = None
        else:
            transition(job, JobStatus.FAILED.value)
            job.error = "Worker lease expired and retry limit was exhausted"
            job.finished_at = func.now()
        job.worker_id = None
        job.lease_expires_at = None
        recovered += 1
        logger.warning(
            "expired_job_recovered",
            extra={"worker_id": previous_worker, "job_id": str(job.id), "event": "expired_job_recovered"},
        )
    await session.flush()
    return recovered
