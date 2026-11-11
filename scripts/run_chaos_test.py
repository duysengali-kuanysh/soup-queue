"""Destructive chaos harness. Run only against a disposable PostgreSQL database."""
import argparse
import asyncio
import json
import os
import random
import signal
import subprocess
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path

import httpx
from sqlalchemy import func, select

from app.database import SessionLocal, dispose_database
from app.models.execution import Execution
from app.models.job import Job


def spawn_worker(index: int, log_dir: Path):
    worker_id = f"chaos-worker-{index}-{uuid.uuid4().hex[:8]}"
    env = os.environ.copy()
    env["WORKER_ID"] = worker_id
    log_path = log_dir / f"{worker_id}.log"
    with log_path.open("w", encoding="utf-8") as log_file:
        process = subprocess.Popen(
            [sys.executable, "-m", "app.workers.worker"],
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
    return process, worker_id, str(log_path)


async def run(args) -> dict:
    started_wall = time.time()
    started_mono = time.monotonic()
    original_by_key: dict[str, str] = {}
    duplicate_checks: list[bool] = []
    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    workers = []
    all_worker_logs = []
    kill_events = 0
    cancel_ids: list[str] = []
    unique_ids: list[uuid.UUID] = []

    async with httpx.AsyncClient(base_url=args.api_url, timeout=60) as client:
        semaphore = asyncio.Semaphore(args.submit_concurrency)

        async def submit_one(index: int):
            key = f"chaos-job-{index}"
            payload = {"steps": 4, "step_delay_seconds": 0.05, "samples": 1000 + index}
            async with semaphore:
                response = await client.post(
                    "/jobs",
                    headers={"Idempotency-Key": key},
                    json={"payload": payload, "max_attempts": 4},
                )
                response.raise_for_status()
                return key, response.json()

        create_results = await asyncio.gather(*(submit_one(i) for i in range(args.jobs)))
        for key, job in create_results:
            original_by_key[key] = job["id"]
        unique_ids = [uuid.UUID(job_id) for job_id in original_by_key.values()]

        # Additional requests intentionally reuse existing keys. They must return the
        # original job instead of inserting a second row.
        duplicate_keys = [f"chaos-job-{random.randrange(args.jobs)}" for _ in range(args.duplicate_requests)]

        async def repeat_key(key: str):
            response = await client.post(
                "/jobs",
                headers={"Idempotency-Key": key},
                json={"payload": {"steps": 99}, "max_attempts": 1},
            )
            response.raise_for_status()
            return response.json()["id"] == original_by_key[key]

        duplicate_checks = await asyncio.gather(*(repeat_key(key) for key in duplicate_keys))

        workers = [spawn_worker(i, log_dir) for i in range(args.workers)]
        all_worker_logs.extend(entry[2] for entry in workers)
        await asyncio.sleep(0.2)  # allow some jobs to enter execution before cancellation races
        cancel_count = max(1, args.jobs // args.cancel_every)
        cancel_ids = random.sample(list(original_by_key.values()), k=min(cancel_count, args.jobs))

        async def request_cancel(job_id: str):
            response = await client.delete(f"/jobs/{job_id}")
            response.raise_for_status()

        await asyncio.gather(*(request_cancel(job_id) for job_id in cancel_ids))

        deadline = time.monotonic() + args.timeout_seconds
        try:
            while time.monotonic() < deadline:
                # Also replace unexpected exits so the harness maintains the requested pool size.
                for index, (process, _, _) in enumerate(workers):
                    if process.poll() is not None:
                        workers[index] = spawn_worker(index, log_dir)
                        all_worker_logs.append(workers[index][2])
                alive_indices = [i for i, (process, _, _) in enumerate(workers) if process.poll() is None]
                if alive_indices:
                    victim_index = random.choice(alive_indices)
                    process, worker_id, _ = workers[victim_index]
                    process.send_signal(signal.SIGKILL)
                    process.wait(timeout=5)
                    kill_events += 1
                    workers[victim_index] = spawn_worker(victim_index, log_dir)
                    all_worker_logs.append(workers[victim_index][2])

                async with SessionLocal() as session:
                    rows = (await session.execute(
                        select(Job.status, func.count())
                        .where(Job.id.in_(unique_ids))
                        .group_by(Job.status)
                    )).all()
                counts = {status: int(count) for status, count in rows}
                terminal = sum(counts.get(status, 0) for status in ("succeeded", "failed", "cancelled"))
                if terminal == args.jobs:
                    break
                await asyncio.sleep(args.kill_interval_seconds)
            else:
                raise TimeoutError("Chaos run timed out before every submitted job reached a terminal state")
        finally:
            for process, _, _ in workers:
                if process.poll() is None:
                    process.terminate()
            for process, _, _ in workers:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)

    async with SessionLocal() as session:
        all_jobs = (await session.execute(select(Job).where(Job.id.in_(unique_ids)))).scalars().all()
        effects = (await session.execute(select(Execution).where(Execution.job_id.in_(unique_ids)))).scalars().all()
        duplicate_effect_groups = await session.scalar(
            select(func.count()).select_from(
                select(Execution.job_id)
                .where(Execution.job_id.in_(unique_ids))
                .group_by(Execution.job_id)
                .having(func.count() > 1)
                .subquery()
            )
        ) or 0
        stuck_running = [str(job.id) for job in all_jobs if job.status == "running"]
        queued = [str(job.id) for job in all_jobs if job.status == "queued"]
        attempts_over_limit = [str(job.id) for job in all_jobs if job.attempt > job.max_attempts]
        final_status_counts = {
            status: sum(1 for job in all_jobs if job.status == status)
            for status in ("queued", "running", "succeeded", "failed", "cancelled")
        }
        effect_counts = defaultdict(int)
        for effect in effects:
            effect_counts[str(effect.job_id)] += 1
        succeeded_without_effect = [str(job.id) for job in all_jobs if job.status == "succeeded" and effect_counts[str(job.id)] != 1]
        non_succeeded_with_effect = [str(job.id) for job in all_jobs if job.status != "succeeded" and effect_counts[str(job.id)] > 0]
        retried_jobs = sum(1 for job in all_jobs if job.attempt > 1)

    worker_log_files = all_worker_logs
    output = {
        "started_at_utc_epoch": started_wall,
        "duration_seconds": round(time.monotonic() - started_mono, 3),
        "unique_job_requests": args.jobs,
        "duplicate_idempotency_requests": args.duplicate_requests,
        "database_job_count_for_run": len(all_jobs),
        "worker_processes": args.workers,
        "worker_kill_events": kill_events,
        "worker_log_files": worker_log_files,
        "side_effect_count": len(effects),
        "retried_job_count": retried_jobs,
        "succeeded_without_effect_job_ids": succeeded_without_effect,
        "non_succeeded_with_effect_job_ids": non_succeeded_with_effect,
        "duplicate_effect_groups": int(duplicate_effect_groups),
        "stuck_running_job_ids": stuck_running,
        "queued_job_ids_at_end": queued,
        "attempts_over_limit_job_ids": attempts_over_limit,
        "duplicate_idempotency_checks_passed": sum(duplicate_checks),
        "cancel_request_count": len(cancel_ids),
        "final_status_counts": final_status_counts,
        "invariants": {
            "no_job_lost": len(all_jobs) == args.jobs,
            "no_duplicate_committed_effect": duplicate_effect_groups == 0,
            "side_effect_status_consistent": not succeeded_without_effect and not non_succeeded_with_effect,
            "no_stuck_running_job": not stuck_running,
            "retry_limit_respected": not attempts_over_limit,
            "idempotency_replays_return_original_job": all(duplicate_checks),
            "cancel_requests_processed_or_won_by_completion": all(
                next(job.status for job in all_jobs if str(job.id) == job_id) in ("succeeded", "failed", "cancelled")
                for job_id in cancel_ids
            ),
            "all_jobs_terminal": not queued and not stuck_running and len(all_jobs) == args.jobs,
        },
    }
    output["passed"] = all(output["invariants"].values())
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(json.dumps(output, indent=2))
    await dispose_database()
    return output


def parse_args():
    parser = argparse.ArgumentParser(description="Run the 10k-job / 8-worker PostgreSQL chaos test")
    parser.add_argument("--api-url", default="http://127.0.0.1:8000")
    parser.add_argument("--jobs", type=int, default=10000, help="Number of unique job rows to create")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--duplicate-requests", type=int, default=1000)
    parser.add_argument("--submit-concurrency", type=int, default=100)
    parser.add_argument("--cancel-every", type=int, default=37)
    parser.add_argument("--kill-interval-seconds", type=float, default=2.0)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--log-dir", default="docs/chaos-logs")
    parser.add_argument("--output", default="docs/chaos-output.json")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result = asyncio.run(run(args))
    raise SystemExit(0 if result["passed"] else 1)
