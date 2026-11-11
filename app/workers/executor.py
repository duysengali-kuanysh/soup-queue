import asyncio
from typing import Awaitable, Callable

from app.core.exceptions import JobCancelled, WorkerStopping

StopCheck = Callable[[], Awaitable[bool]]


async def execute_training(payload: dict, *, attempt: int, should_stop: StopCheck) -> dict:
    """Deterministic training-job simulation; replace with a sandboxed trainer in production."""
    steps = int(payload.get("steps", 5))
    delay = float(payload.get("step_delay_seconds", 0.05))
    if not 1 <= steps <= 10000:
        raise ValueError("payload.steps must be between 1 and 10000")
    if not 0 <= delay <= 30:
        raise ValueError("payload.step_delay_seconds must be between 0 and 30")

    fail_until_attempt = int(payload.get("fail_until_attempt", 0))
    for _ in range(steps):
        if await should_stop():
            raise JobCancelled("Cancellation requested or worker ownership lost")
        await asyncio.sleep(delay)

    if attempt <= fail_until_attempt:
        raise RuntimeError(f"Simulated training failure on attempt {attempt}")
    if payload.get("simulate_failure") is True:
        raise RuntimeError("Simulated training failure requested by payload")

    return {
        "task": "training_simulation",
        "steps_completed": steps,
        "samples_processed": int(payload.get("samples", steps * 100)),
        "metrics": {"loss": round(1.0 / (attempt + steps), 6), "accuracy": round(min(0.99, 0.5 + steps * 0.01), 4)},
    }
