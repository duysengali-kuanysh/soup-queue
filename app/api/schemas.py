from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class JobCreate(BaseModel):
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int | None = Field(default=None, ge=1, le=20)


class JobResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    idempotency_key: str
    payload: dict[str, Any]
    status: Literal["queued", "running", "succeeded", "failed", "cancelled"]
    worker_id: str | None
    attempt: int
    max_attempts: int
    fencing_token: int
    lease_expires_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    cancel_requested: bool
    result: dict[str, Any] | None
    error: str | None
    created_at: datetime
    updated_at: datetime


class JobStatusResponse(BaseModel):
    id: UUID
    status: str
    attempt: int
    max_attempts: int
    cancel_requested: bool


class JobListResponse(BaseModel):
    items: list[JobResponse]
    total: int
    limit: int
    offset: int


class ErrorResponse(BaseModel):
    detail: str


class PaginationParams(BaseModel):
    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0)
