from uuid import UUID

from fastapi import APIRouter, Depends, Header, Query, Response

from app.api.dependencies import get_job_service
from app.api.schemas import JobCreate, JobListResponse, JobResponse
from app.config import settings
from app.services.job_service import JobService

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("", response_model=JobResponse, summary="Create a training job")
async def create_job(
    body: JobCreate,
    response: Response,
    idempotency_key: str = Header(..., alias="Idempotency-Key", min_length=1, max_length=255),
    service: JobService = Depends(get_job_service),
):
    job, created = await service.create_job(
        idempotency_key=idempotency_key,
        payload=body.payload,
        max_attempts=body.max_attempts if body.max_attempts is not None else settings.default_max_attempts,
    )
    response.status_code = 201 if created else 200
    return job


@router.get("/{job_id}", response_model=JobResponse, summary="Get a training job")
async def get_job(job_id: UUID, service: JobService = Depends(get_job_service)):
    return await service.get_job(job_id)


@router.get("", response_model=JobListResponse, summary="List jobs")
async def list_jobs(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    status: str | None = Query(default=None, pattern="^(queued|running|succeeded|failed|cancelled)$"),
    service: JobService = Depends(get_job_service),
):
    items, total = await service.list_jobs(offset=offset, limit=limit, status=status)
    return JobListResponse(items=items, total=total, limit=limit, offset=offset)


@router.delete("/{job_id}", response_model=JobResponse, summary="Cancel a training job")
async def cancel_job(job_id: UUID, service: JobService = Depends(get_job_service)):
    return await service.cancel_job(job_id)
