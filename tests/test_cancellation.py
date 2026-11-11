import uuid
from datetime import datetime, timedelta, timezone
import pytest
from sqlalchemy import select
pytest.importorskip("aiosqlite", reason="SQLite async test driver is a development dependency")
from app.core.exceptions import JobNotFoundError
from app.models.job import Job
from app.models.worker import Worker
from app.models.execution import Execution
from app.core.exceptions import StaleWorkerError
from app.workers.worker import _commit_success
from app.services.cancellation import CancellationService


@pytest.mark.asyncio
async def test_queued_job_cancellation_is_immediate(session):
    job = Job(id=uuid.uuid4(), idempotency_key="cancel-queued", payload={}, status="queued", max_attempts=3)
    session.add(job)
    await session.commit()
    result = await CancellationService(session).cancel(job.id)
    assert result.status == "cancelled"
    assert result.cancel_requested is True


@pytest.mark.asyncio
async def test_running_job_gets_cooperative_cancel_flag(session):
    session.add(Worker(id="w1", hostname="test-host", status="online"))
    await session.flush()
    job = Job(id=uuid.uuid4(), idempotency_key="cancel-running", payload={}, status="running", worker_id="w1", attempt=1, max_attempts=3, fencing_token=1)
    session.add(job)
    await session.commit()
    result = await CancellationService(session).cancel(job.id)
    assert result.status == "running"
    assert result.cancel_requested is True


@pytest.mark.asyncio
async def test_cancel_missing_job_raises(session):
    with pytest.raises(JobNotFoundError):
        await CancellationService(session).cancel(uuid.uuid4())


@pytest.mark.asyncio
async def test_cancel_transaction_winning_before_completion_prevents_effect(session_factory):
    job_id = uuid.uuid4()
    async with session_factory() as session:
        session.add(Worker(id="race-worker", hostname="test-host", status="online"))
        await session.flush()
        job = Job(
            id=job_id, idempotency_key="cancel-before-commit", payload={}, status="running",
            worker_id="race-worker", attempt=1, max_attempts=3, fencing_token=1,
            lease_expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
        )
        session.add(job)
        await session.commit()

    async with session_factory() as session:
        await CancellationService(session).cancel(job_id)

    final_status = await _commit_success(job_id, "race-worker", 1, {"ok": True}, session_factory=session_factory)
    assert final_status == "cancelled"
    async with session_factory() as session:
        stored = await session.get(Job, job_id)
        effect = await session.scalar(select(Execution).where(Execution.job_id == job_id))
        assert stored.status == "cancelled"
        assert effect is None


@pytest.mark.asyncio
async def test_completion_winning_before_cancel_remains_succeeded(session_factory):
    job_id = uuid.uuid4()
    async with session_factory() as session:
        session.add(Worker(id="complete-worker", hostname="test-host", status="online"))
        await session.flush()
        session.add(Job(
            id=job_id, idempotency_key="complete-before-cancel", payload={}, status="running",
            worker_id="complete-worker", attempt=1, max_attempts=3, fencing_token=1,
            lease_expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
        ))
        await session.commit()

    final_status = await _commit_success(job_id, "complete-worker", 1, {"ok": True}, session_factory=session_factory)
    assert final_status == "succeeded"
    async with session_factory() as session:
        stored = await CancellationService(session).cancel(job_id)
        effect = await session.scalar(select(Execution).where(Execution.job_id == job_id))
        assert stored.status == "succeeded"
        assert effect is not None


@pytest.mark.asyncio
async def test_stale_fencing_token_cannot_commit(session_factory):
    job_id = uuid.uuid4()
    async with session_factory() as session:
        session.add(Worker(id="new-worker", hostname="test-host", status="online"))
        await session.flush()
        session.add(Job(
            id=job_id, idempotency_key="stale-token", payload={}, status="running",
            worker_id="new-worker", attempt=2, max_attempts=3, fencing_token=2,
            lease_expires_at=datetime.now(timezone.utc) + timedelta(minutes=1),
        ))
        await session.commit()

    with pytest.raises(StaleWorkerError):
        await _commit_success(job_id, "new-worker", 1, {"stale": True}, session_factory=session_factory)
