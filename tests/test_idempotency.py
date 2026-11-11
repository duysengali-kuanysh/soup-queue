import pytest
pytest.importorskip("aiosqlite", reason="SQLite async test driver is a development dependency")
from app.models.job import Job
from app.services.job_service import JobService


@pytest.mark.asyncio
async def test_same_idempotency_key_returns_same_job(session):
    service = JobService(session)
    first, created_first = await service.create_job(idempotency_key="same-key", payload={"steps": 1}, max_attempts=3)
    second, created_second = await service.create_job(idempotency_key="same-key", payload={"steps": 99}, max_attempts=1)
    assert created_first is True
    assert created_second is False
    assert first.id == second.id
    assert second.payload == {"steps": 1}


@pytest.mark.asyncio
async def test_database_unique_constraint_is_present(session):
    constraints = await session.run_sync(lambda sync: {c.name for c in Job.__table__.constraints})
    assert "uq_jobs_idempotency_key" in constraints




