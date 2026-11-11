from functools import lru_cache
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Environment-backed configuration shared by API and workers."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "Soup Training Job Queue"
    environment: str = "development"
    database_url: str = "postgresql+asyncpg://soup:soup@postgres:5432/soup_queue"
    worker_id: str | None = None
    worker_count: int = Field(default=4, ge=1, le=128)
    lease_duration_seconds: int = Field(default=30, ge=5, le=3600)
    heartbeat_interval_seconds: int = Field(default=10, ge=1, le=600)
    default_max_attempts: int = Field(default=3, ge=1, le=20)
    job_timeout_seconds: int = Field(default=300, ge=1, le=86400)
    worker_poll_interval_seconds: float = Field(default=0.5, gt=0, le=60)
    recovery_interval_seconds: float = Field(default=2.0, gt=0, le=300)
    recovery_batch_size: int = Field(default=100, ge=1, le=10000)
    db_pool_size: int = Field(default=10, ge=1, le=100)
    db_max_overflow: int = Field(default=20, ge=0, le=200)
    db_pool_timeout_seconds: int = Field(default=30, ge=1, le=300)
    shutdown_timeout_seconds: int = Field(default=15, ge=1, le=300)
    log_level: str = "INFO"

    @model_validator(mode="after")
    def validate_timing(self):
        if self.heartbeat_interval_seconds >= self.lease_duration_seconds:
            raise ValueError("heartbeat_interval_seconds must be smaller than lease_duration_seconds")
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
