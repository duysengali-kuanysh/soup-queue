from uuid import UUID
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import JobStatus
from app.core.exceptions import JobNotFoundError
from app.repositories.job_repository import JobRepository
from app.services.state_machine import transition


class CancellationService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.jobs = JobRepository(session)

    async def cancel(self, job_id: UUID):
        async with self.session.begin():
            job = await self.jobs.get_locked(job_id)
            if job is None:
                raise JobNotFoundError(f"Job {job_id} was not found")
            if job.status == JobStatus.QUEUED.value:
                transition(job, JobStatus.CANCELLED.value)
                job.cancel_requested = True
                job.error = None
                job.finished_at = func.now()
            elif job.status == JobStatus.RUNNING.value:
                # Completion takes the same row lock. Whichever transaction commits first wins.
                job.cancel_requested = True
            # Terminal jobs are idempotent no-ops.
            await self.session.flush()
            await self.session.refresh(job)
        return job
