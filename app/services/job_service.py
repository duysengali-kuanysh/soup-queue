import uuid
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import JobStatus
from app.core.exceptions import JobNotFoundError
from app.models.job import Job
from app.repositories.job_repository import JobRepository
from app.services.cancellation import CancellationService


class JobService:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.repository = JobRepository(session)
        self.cancellation = CancellationService(session)

    async def create_job(self, *, idempotency_key: str, payload: dict, max_attempts: int) -> tuple[Job, bool]:
        try:
            async with self.session.begin():
                existing = await self.repository.get_by_idempotency_key(idempotency_key)
                if existing is not None:
                    return existing, False
                job = Job(
                    id=uuid.uuid4(),
                    idempotency_key=idempotency_key,
                    payload=payload,
                    status=JobStatus.QUEUED.value,
                    max_attempts=max_attempts,
                )
                await self.repository.add(job)
            return job, True
        except IntegrityError:
            # A concurrent request may have inserted the same key after our read.
            await self.session.rollback()
            existing = await self.repository.get_by_idempotency_key(idempotency_key)
            if existing is not None:
                return existing, False
            raise

    async def get_job(self, job_id):
        job = await self.repository.get(job_id)
        if job is None:
            raise JobNotFoundError(f"Job {job_id} was not found")
        return job

    async def list_jobs(self, *, offset: int, limit: int, status: str | None = None):
        return await self.repository.list_jobs(offset=offset, limit=limit, status=status)

    async def cancel_job(self, job_id):
        return await self.cancellation.cancel(job_id)
