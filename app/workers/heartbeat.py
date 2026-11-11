import asyncio
import logging
from uuid import UUID

from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from app.config import settings
from app.repositories.job_repository import JobRepository
from app.repositories.worker_repository import WorkerRepository

logger = logging.getLogger(__name__)


async def heartbeat_loop(
    *, session_factory: async_sessionmaker[AsyncSession], worker_id: str,
    job_id: UUID, fencing_token: int, ownership_lost: asyncio.Event,
    stop_event: asyncio.Event,
) -> None:
    """Renew using database time; a failed conditional update means ownership is gone."""
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=settings.heartbeat_interval_seconds)
            return
        except TimeoutError:
            pass
        try:
            async with session_factory() as session:
                async with session.begin():
                    renewed = await JobRepository(session).renew_lease(
                        job_id, worker_id, fencing_token, settings.lease_duration_seconds
                    )
                    await WorkerRepository(session).heartbeat(worker_id)
            if not renewed:
                ownership_lost.set()
                logger.warning("lease_renewal_rejected", extra={"worker_id": worker_id, "job_id": str(job_id), "event": "lease_renewal_rejected"})
                return
        except Exception:
            # A transient DB error is not proof of death. Retry, but execution is still fenced at commit.
            logger.exception("heartbeat_failed", extra={"worker_id": worker_id, "job_id": str(job_id), "event": "heartbeat_failed"})
