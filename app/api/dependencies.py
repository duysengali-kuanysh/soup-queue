from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session
from app.services.job_service import JobService


async def get_job_service(session: AsyncSession = Depends(get_session)) -> JobService:
    return JobService(session)
