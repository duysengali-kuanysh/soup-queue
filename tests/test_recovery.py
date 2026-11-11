import uuid
from datetime import datetime, timedelta, timezone
import pytest
pytest.importorskip("aiosqlite", reason="SQLite async test driver is a development dependency")
from app.models.job import Job
from app.models.worker import Worker
from app.workers.recovery import recover_expired_jobs


@pytest.mark.asyncio
async def test_expired_job_is_requeued_when_attempts_remain(session):
    session.add(Worker(id="dead-worker", hostname="test-host", status="offline"))
    await session.flush()
    job = Job(id=uuid.uuid4(), idempotency_key="expired-retry", payload={}, status="running", worker_id="dead-worker", attempt=1, max_attempts=3, fencing_token=1, lease_expires_at=datetime.now(timezone.utc)-timedelta(minutes=1))
    session.add(job)
    await session.commit()
    count = await recover_expired_jobs(session, batch_size=10)
    await session.commit()
    assert count == 1
    assert job.status == "queued"
    assert job.worker_id is None
    assert job.lease_expires_at is None


@pytest.mark.asyncio
async def test_expired_job_exhausted_attempts_fails(session):
    session.add(Worker(id="dead-worker", hostname="test-host", status="offline"))
    await session.flush()
    job = Job(id=uuid.uuid4(), idempotency_key="expired-final", payload={}, status="running", worker_id="dead-worker", attempt=3, max_attempts=3, fencing_token=3, lease_expires_at=datetime.now(timezone.utc)-timedelta(minutes=1))
    session.add(job)
    await session.commit()
    await recover_expired_jobs(session, batch_size=10)
    await session.commit()
    assert job.status == "failed"


@pytest.mark.asyncio
async def test_expired_cancel_requested_job_becomes_cancelled(session):
    session.add(Worker(id="dead-cancel-worker", hostname="test-host", status="offline"))
    await session.flush()
    job = Job(
        id=uuid.uuid4(),
        idempotency_key="expired-cancel-requested",
        payload={},
        status="running",
        worker_id="dead-cancel-worker",
        attempt=1,
        max_attempts=3,
        fencing_token=1,
        cancel_requested=True,
        lease_expires_at=datetime.now(timezone.utc) - timedelta(minutes=1),
    )
    session.add(job)
    await session.commit()
    await recover_expired_jobs(session, batch_size=10)
    await session.commit()
    assert job.status == "cancelled"
    assert job.worker_id is None
