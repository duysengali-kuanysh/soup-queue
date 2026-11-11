import uuid
import pytest

pytest.importorskip("aiosqlite", reason="SQLite async test driver is a development dependency")

from sqlalchemy.exc import IntegrityError
from app.models.job import Job
from app.repositories.execution_repository import ExecutionRepository


@pytest.mark.asyncio
async def test_execution_effect_is_unique_per_job(session):
    job = Job(
        id=uuid.uuid4(),
        idempotency_key="effect-once",
        payload={},
        status="succeeded",
        max_attempts=1,
    )

    session.add(job)
    await session.commit()

    job_id = job.id

    repo = ExecutionRepository(session)

    await repo.add_effect(
        job_id=job_id,
        effect_type="training_completed",
        result={"ok": True},
        fencing_token=1,
    )
    await session.commit()

    with pytest.raises(IntegrityError):
        await repo.add_effect(
            job_id=job_id,
            effect_type="training_completed",
            result={"ok": False},
            fencing_token=2,
        )

    await session.rollback()

    effect = await repo.get_by_job_id(job_id)
    assert effect is not None
    assert effect.result == {"ok": True}
