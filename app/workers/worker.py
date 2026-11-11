import argparse
import asyncio
import logging
import os
import signal
import socket
import uuid
from datetime import datetime, timezone

from sqlalchemy import select, func

from app.config import settings
from app.core.constants import JobStatus
from app.core.exceptions import JobCancelled, StaleWorkerError, WorkerStopping
from app.database import SessionLocal, dispose_database
from app.logging_config import configure_logging
from app.models.job import Job
from app.repositories.execution_repository import ExecutionRepository
from app.repositories.job_repository import JobRepository
from app.repositories.worker_repository import WorkerRepository
from app.services.state_machine import transition
from app.workers.executor import execute_training
from app.workers.heartbeat import heartbeat_loop
from app.workers.recovery import recover_expired_jobs

logger = logging.getLogger(__name__)


def _worker_id() -> str:
    return settings.worker_id or os.getenv("WORKER_ID") or f"{socket.gethostname()}-{uuid.uuid4().hex[:10]}"


async def _should_stop(job_id, worker_id: str, token: int, ownership_lost: asyncio.Event, shutdown: asyncio.Event) -> bool:
    if shutdown.is_set():
        raise WorkerStopping("Worker is shutting down; leave lease for recovery")
    if ownership_lost.is_set():
        return True
    async with SessionLocal() as session:
        row = (await session.execute(
            select(Job.status, Job.worker_id, Job.fencing_token, Job.cancel_requested)
            .where(Job.id == job_id)
        )).one_or_none()
    if row is None or row.status != JobStatus.RUNNING.value or row.worker_id != worker_id or row.fencing_token != token:
        ownership_lost.set()
        return True
    return bool(row.cancel_requested)


async def _finish_cancelled(job_id, worker_id: str, token: int) -> None:
    async with SessionLocal() as session:
        async with session.begin():
            job = await JobRepository(session).get_locked(job_id, active_owner=(worker_id, token))
            if job is None:
                raise StaleWorkerError("Worker no longer owns this job")
            if job.cancel_requested:
                transition(job, JobStatus.CANCELLED.value)
                job.worker_id = None
                job.lease_expires_at = None
                job.error = None
                job.finished_at = func.now()


async def _finish_failure(job_id, worker_id: str, token: int, error: Exception) -> None:
    async with SessionLocal() as session:
        async with session.begin():
            job = await JobRepository(session).get_locked(job_id, active_owner=(worker_id, token))
            if job is None:
                raise StaleWorkerError("Worker no longer owns this job")
            job.worker_id = None
            job.lease_expires_at = None
            if job.cancel_requested:
                transition(job, JobStatus.CANCELLED.value)
                job.error = None
                job.finished_at = func.now()
            elif job.attempt < job.max_attempts:
                transition(job, JobStatus.QUEUED.value)
                job.error = str(error)[:4000]
                job.started_at = None
                job.finished_at = None
            else:
                transition(job, JobStatus.FAILED.value)
                job.error = str(error)[:4000]
                job.finished_at = func.now()


async def _commit_success(job_id, worker_id: str, token: int, result: dict, session_factory=SessionLocal) -> str:
    """Commit the durable effect and succeeded state atomically under the job row lock."""
    async with session_factory() as session:
        async with session.begin():
            jobs = JobRepository(session)
            job = await jobs.get_locked(job_id, active_owner=(worker_id, token))
            if job is None:
                raise StaleWorkerError("Stale worker cannot commit a result")
            if job.cancel_requested:
                transition(job, JobStatus.CANCELLED.value)
                job.worker_id = None
                job.lease_expires_at = None
                job.error = None
                job.finished_at = func.now()
                return "cancelled"
            existing = await ExecutionRepository(session).get_by_job_id(job_id)
            if existing is not None:
                # Normally effect and succeeded are atomic, so this is an invariant guard.
                raise RuntimeError("Execution effect already exists for a non-terminal job")
            await ExecutionRepository(session).add_effect(
                job_id=job_id,
                effect_type="training_completed",
                result=result,
                fencing_token=token,
            )
            job.result = result
            job.error = None
            transition(job, JobStatus.SUCCEEDED.value)
            job.worker_id = None
            job.lease_expires_at = None
            job.finished_at = func.now()
            return "succeeded"


class Worker:
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.shutdown = asyncio.Event()
        self.last_recovery = 0.0

    async def run(self) -> None:
        async with SessionLocal() as session:
            async with session.begin():
                await WorkerRepository(session).register(self.worker_id)
        logger.info("worker_started", extra={"worker_id": self.worker_id, "event": "worker_started"})
        try:
            while not self.shutdown.is_set():
                now = asyncio.get_running_loop().time()
                if now - self.last_recovery >= settings.recovery_interval_seconds:
                    async with SessionLocal() as session:
                        async with session.begin():
                            count = await recover_expired_jobs(session)
                    if count:
                        logger.info("recovery_batch_completed", extra={"worker_id": self.worker_id, "event": "recovery_batch_completed"})
                    self.last_recovery = now

                async with SessionLocal() as session:
                    async with session.begin():
                        job = await JobRepository(session).claim_next(self.worker_id, settings.lease_duration_seconds)
                if job is None:
                    try:
                        await asyncio.wait_for(self.shutdown.wait(), timeout=settings.worker_poll_interval_seconds)
                    except TimeoutError:
                        pass
                    continue
                await self._process(job)
        finally:
            async with SessionLocal() as session:
                async with session.begin():
                    await WorkerRepository(session).set_status(self.worker_id, "offline")
            logger.info("worker_stopped", extra={"worker_id": self.worker_id, "event": "worker_stopped"})

    async def _process(self, job: Job) -> None:
        token = job.fencing_token
        ownership_lost = asyncio.Event()
        heartbeat_stop = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            heartbeat_loop(
                session_factory=SessionLocal,
                worker_id=self.worker_id,
                job_id=job.id,
                fencing_token=token,
                ownership_lost=ownership_lost,
                stop_event=heartbeat_stop,
            )
        )
        logger.info("job_claimed", extra={"worker_id": self.worker_id, "job_id": str(job.id), "event": "job_claimed"})
        try:
            async def should_stop():
                return await _should_stop(job.id, self.worker_id, token, ownership_lost, self.shutdown)

            result = await asyncio.wait_for(
                execute_training(job.payload, attempt=job.attempt, should_stop=should_stop),
                timeout=settings.job_timeout_seconds,
            )
            final_status = await _commit_success(job.id, self.worker_id, token, result)
            logger.info("job_%s", final_status, extra={"worker_id": self.worker_id, "job_id": str(job.id), "event": f"job_{final_status}"})
        except WorkerStopping:
            # Graceful shutdown: do not acknowledge; let lease expiration/recovery retry the job.
            logger.info("job_released_for_recovery", extra={"worker_id": self.worker_id, "job_id": str(job.id), "event": "job_released_for_recovery"})
        except JobCancelled:
            try:
                await _finish_cancelled(job.id, self.worker_id, token)
            except StaleWorkerError:
                logger.warning("cancel_ack_rejected_stale_worker", extra={"worker_id": self.worker_id, "job_id": str(job.id), "event": "cancel_ack_rejected_stale_worker"})
        except StaleWorkerError:
            logger.warning("stale_worker_result_discarded", extra={"worker_id": self.worker_id, "job_id": str(job.id), "event": "stale_worker_result_discarded"})
        except Exception as exc:
            logger.exception("job_execution_failed", extra={"worker_id": self.worker_id, "job_id": str(job.id), "event": "job_execution_failed", "error_message": str(exc)})
            try:
                await _finish_failure(job.id, self.worker_id, token, exc)
            except StaleWorkerError:
                logger.warning("stale_worker_failure_discarded", extra={"worker_id": self.worker_id, "job_id": str(job.id), "event": "stale_worker_failure_discarded"})
        finally:
            heartbeat_stop.set()
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass


def _install_signal_handlers(worker: Worker) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, worker.shutdown.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: worker.shutdown.set())


async def async_main() -> None:
    configure_logging(settings.log_level)
    worker = Worker(_worker_id())
    _install_signal_handlers(worker)
    try:
        await worker.run()
    finally:
        await dispose_database()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
