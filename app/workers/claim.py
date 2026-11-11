from sqlalchemy.ext.asyncio import AsyncSession
from app.repositories.job_repository import JobRepository


async def claim_job(session: AsyncSession, worker_id: str, lease_seconds: int):
    repository = JobRepository(session)
    return await repository.claim_next(worker_id, lease_seconds)
