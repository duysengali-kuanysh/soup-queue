from datetime import timedelta
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import JobStatus
from app.models.job import Job


class JobRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add(self, job: Job) -> None:
        self.session.add(job)
        await self.session.flush()

    async def get(self, job_id: UUID) -> Job | None:
        return await self.session.get(Job, job_id)

    async def get_locked(self, job_id: UUID, *, active_owner: tuple[str, int] | None = None) -> Job | None:
        stmt = select(Job).where(Job.id == job_id).with_for_update()
        if active_owner is not None:
            worker_id, token = active_owner
            stmt = stmt.where(
                Job.status == JobStatus.RUNNING.value,
                Job.worker_id == worker_id,
                Job.fencing_token == token,
                Job.lease_expires_at > func.now(),
            )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_by_idempotency_key(self, key: str) -> Job | None:
        return (await self.session.execute(select(Job).where(Job.idempotency_key == key))).scalar_one_or_none()

    async def list_jobs(self, *, offset: int, limit: int, status: str | None = None) -> tuple[list[Job], int]:
        stmt = select(Job)
        count_stmt = select(func.count()).select_from(Job)
        if status:
            stmt = stmt.where(Job.status == status)
            count_stmt = count_stmt.where(Job.status == status)
        total = int(await self.session.scalar(count_stmt) or 0)
        rows = await self.session.execute(stmt.order_by(Job.created_at.desc(), Job.id).offset(offset).limit(limit))
        return list(rows.scalars().all()), total

    async def claim_next(self, worker_id: str, lease_seconds: int) -> Job | None:
        """Claim one queued row; PostgreSQL SKIP LOCKED prevents double claim."""
        stmt = (
            select(Job)
            .where(Job.status == JobStatus.QUEUED.value)
            .order_by(Job.created_at, Job.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        job = (await self.session.execute(stmt)).scalar_one_or_none()
        if job is None:
            return None
        job.status = JobStatus.RUNNING.value
        job.worker_id = worker_id
        job.attempt += 1
        job.fencing_token += 1
        job.lease_expires_at = func.now() + timedelta(seconds=lease_seconds)
        job.started_at = func.now()
        job.finished_at = None
        job.error = None
        await self.session.flush()
        await self.session.refresh(job)
        return job

    async def renew_lease(self, job_id: UUID, worker_id: str, token: int, lease_seconds: int) -> bool:
        result = await self.session.execute(
            update(Job)
            .where(
                Job.id == job_id,
                Job.status == JobStatus.RUNNING.value,
                Job.worker_id == worker_id,
                Job.fencing_token == token,
                Job.lease_expires_at > func.now(),
            )
            .values(lease_expires_at=func.now() + timedelta(seconds=lease_seconds), updated_at=func.now())
        )
        return result.rowcount == 1

    async def expired_for_update(self, batch_size: int) -> list[Job]:
        stmt = (
            select(Job)
            .where(
                Job.status == JobStatus.RUNNING.value,
                Job.lease_expires_at.is_not(None),
                Job.lease_expires_at <= func.now(),
            )
            .order_by(Job.lease_expires_at)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        return list((await self.session.execute(stmt)).scalars().all())

    async def cancellation_requested(self, job_id: UUID, worker_id: str, token: int) -> bool:
        value = await self.session.scalar(
            select(Job.cancel_requested).where(
                Job.id == job_id,
                Job.status == JobStatus.RUNNING.value,
                Job.worker_id == worker_id,
                Job.fencing_token == token,
            )
        )
        return value is True
