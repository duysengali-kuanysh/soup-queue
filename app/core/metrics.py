from collections import Counter
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.constants import JobStatus
from app.models.job import Job


async def collect_metrics(session: AsyncSession) -> dict[str, Any]:
    """Read durable queue metrics from PostgreSQL; metrics failures never affect execution."""
    status_rows = await session.execute(select(Job.status, func.count()).group_by(Job.status))
    counts = Counter({str(status): int(count) for status, count in status_rows.all()})
    total = sum(counts.values())
    retried = await session.scalar(select(func.count()).select_from(Job).where(Job.attempt > 1)) or 0
    expired = await session.scalar(
        select(func.count()).select_from(Job).where(
            Job.status == JobStatus.RUNNING.value,
            Job.lease_expires_at.is_not(None),
            Job.lease_expires_at <= func.now(),
        )
    ) or 0
    queue_size = counts[JobStatus.QUEUED.value]

    rows = (await session.execute(
        select(Job.started_at, Job.finished_at).where(Job.started_at.is_not(None), Job.finished_at.is_not(None))
    )).all()
    durations = []
    for started, finished in rows:
        if started and finished:
            try:
                durations.append(max(0.0, (finished - started).total_seconds()))
            except TypeError:
                # Some SQLite test configurations return naive datetimes.
                durations.append(max(0.0, (finished.replace(tzinfo=None) - started.replace(tzinfo=None)).total_seconds()))

    return {
        "total_jobs": total,
        "successful_jobs": counts[JobStatus.SUCCEEDED.value],
        "failed_jobs": counts[JobStatus.FAILED.value],
        "cancelled_jobs": counts[JobStatus.CANCELLED.value],
        "queued_jobs": queue_size,
        "running_jobs": counts[JobStatus.RUNNING.value],
        "retried_jobs": int(retried),
        "expired_leases": int(expired),
        "average_execution_seconds": round(sum(durations) / len(durations), 4) if durations else 0.0,
        "queue_size": queue_size,
    }
