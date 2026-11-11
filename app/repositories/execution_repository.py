from uuid import UUID
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.execution import Execution


class ExecutionRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add_effect(self, *, job_id: UUID, effect_type: str, result: dict, fencing_token: int) -> Execution:
        effect = Execution(job_id=job_id, effect_type=effect_type, result=result, fencing_token=fencing_token)
        self.session.add(effect)
        # The UNIQUE(job_id) constraint is the final durable duplicate-effect barrier.
        await self.session.flush()
        return effect

    async def get_by_job_id(self, job_id: UUID) -> Execution | None:
        return (await self.session.execute(select(Execution).where(Execution.job_id == job_id))).scalar_one_or_none()
