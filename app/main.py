from contextlib import asynccontextmanager
import logging

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.jobs import router as jobs_router
from app.config import settings
from app.core.exceptions import InvalidJobStateError, JobNotFoundError
from app.core.metrics import collect_metrics
from app.database import dispose_database, get_session
from app.logging_config import configure_logging

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging(settings.log_level)
    logger.info("api_starting", extra={"event": "api_starting"})
    yield
    await dispose_database()
    logger.info("api_stopped", extra={"event": "api_stopped"})


def create_app() -> FastAPI:
    app = FastAPI(title=settings.app_name, version="1.0.0", lifespan=lifespan)
    app.include_router(jobs_router)

    @app.get("/healthz", tags=["health"])
    async def healthz(session: AsyncSession = Depends(get_session)):
        await session.execute(text("SELECT 1"))
        return {"status": "ok", "database": "ok"}

    @app.get("/metrics", tags=["operations"])
    async def metrics(session: AsyncSession = Depends(get_session)):
        try:
            return await collect_metrics(session)
        except Exception:
            logger.exception("metrics_collection_failed", extra={"event": "metrics_collection_failed"})
            return JSONResponse(status_code=503, content={"detail": "Metrics temporarily unavailable"})

    @app.exception_handler(JobNotFoundError)
    async def job_not_found_handler(request: Request, exc: JobNotFoundError):
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(InvalidJobStateError)
    async def invalid_state_handler(request: Request, exc: InvalidJobStateError):
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    return app


app = create_app()
