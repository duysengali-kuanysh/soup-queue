import asyncio
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.models.job import Job
from app.repositories.job_repository import JobRepository
from app.repositories.worker_repository import WorkerRepository

pytestmark = pytest.mark.skipif(
    not os.getenv("TEST_DATABASE_URL", "").startswith("postgresql"),
    reason="PostgreSQL row locks and SKIP LOCKED are required for this test",
)


@pytest.mark.asyncio
async def test_parallel_workers_never_claim_the_same_job(test_engine):
    factory = async_sessionmaker(test_engine, expire_on_commit=False, autoflush=False)
    worker_ids = [f"claim-worker-{i}" for i in range(8)]
    async with factory() as session:
        async with session.begin():
            for worker_id in worker_ids:
                await WorkerRepository(session).register(worker_id)
            for index in range(64):
                session.add(Job(
                    id=uuid.uuid4(),
                    idempotency_key=f"concurrency-{index}",
                    payload={"steps": 1},
                    status="queued",
                    max_attempts=3,
                ))

    claimed_ids: list[str] = []
    async def consume(worker_id: str):
        while True:
            async with factory() as session:
                async with session.begin():
                    job = await JobRepository(session).claim_next(worker_id, lease_seconds=30)
                    job_id = str(job.id) if job else None
            if job_id is None:
                return
            claimed_ids.append(job_id)

    await asyncio.gather(*(consume(worker_id) for worker_id in worker_ids))
    assert len(claimed_ids) == 64
    assert len(set(claimed_ids)) == 64


@pytest.mark.asyncio
async def test_fencing_token_mismatch_cannot_renew_lease(test_engine):
    factory = async_sessionmaker(test_engine, expire_on_commit=False, autoflush=False)
    job_id = uuid.uuid4()
    async with factory() as session:
        async with session.begin():
            await WorkerRepository(session).register("fence-worker")
            session.add(Job(
                id=job_id,
                idempotency_key="fencing-token-test",
                payload={},
                status="running",
                worker_id="fence-worker",
                attempt=2,
                max_attempts=3,
                fencing_token=2,
                lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=60),
            ))
    async with factory() as session:
        async with session.begin():
            accepted = await JobRepository(session).renew_lease(job_id, "fence-worker", token=1, lease_seconds=30)
    assert accepted is False


@pytest.mark.asyncio
async def test_concurrent_cancel_and_completion_have_one_consistent_winner(test_engine):
    from datetime import datetime, timedelta, timezone

    from app.core.constants import JobStatus
    from app.models.execution import Execution
    from app.models.worker import Worker
    from app.services.cancellation import CancellationService
    from app.workers.worker import _commit_success

    factory = async_sessionmaker(test_engine, expire_on_commit=False, autoflush=False)
    job_id = uuid.uuid4()
    async with factory() as session:
        async with session.begin():
            await WorkerRepository(session).register("race-worker")
            session.add(Job(
                id=job_id,
                idempotency_key="postgres-cancel-complete-race",
                payload={},
                status="running",
                worker_id="race-worker",
                attempt=1,
                max_attempts=3,
                fencing_token=1,
                lease_expires_at=datetime.now(timezone.utc) + timedelta(minutes=2),
            ))

    async def cancel():
        async with factory() as session:
            return await CancellationService(session).cancel(job_id)

    async def complete():
        return await _commit_success(job_id, "race-worker", 1, {"ok": True}, session_factory=factory)

    await asyncio.gather(cancel(), complete())
    async with factory() as session:
        job = await session.get(Job, job_id)
        effect = await session.scalar(select(Execution).where(Execution.job_id == job_id))
        assert job.status in {JobStatus.CANCELLED.value, JobStatus.SUCCEEDED.value}
        if job.status == JobStatus.CANCELLED.value:
            assert effect is None
        else:
            assert effect is not None
            assert effect.fencing_token == 1


@pytest.mark.asyncio
async def test_concurrent_idempotency_replays_create_exactly_one_job(test_engine):
    from app.services.job_service import JobService

    factory = async_sessionmaker(test_engine, expire_on_commit=False, autoflush=False)

    async def create_one(index: int):
        async with factory() as session:
            return await JobService(session).create_job(
                idempotency_key="one-key-many-concurrent-requests",
                payload={"request_index": index},
                max_attempts=3,
            )

    results = await asyncio.gather(*(create_one(index) for index in range(24)))
    job_ids = {str(job.id) for job, _ in results}
    created_count = sum(1 for _, created in results if created)
    async with factory() as session:
        row_count = await session.scalar(
            select(func.count()).select_from(Job).where(
                Job.idempotency_key == "one-key-many-concurrent-requests"
            )
        )
    assert len(job_ids) == 1
    assert created_count == 1
    assert row_count == 1
