# Soup Training Job Queue

A PostgreSQL-backed training job queue with a FastAPI control plane and independent worker processes. PostgreSQL is both the durable queue and the coordination layer; Redis and Celery are intentionally not used.

> **Production verdict: DON'T SHIP yet.** The repository implements the core correctness mechanisms, but the 10k-job chaos run, PostgreSQL concurrency tests, operational load tests, security review, and 100 RPS benchmark have not been run in this environment.

## Architecture

```text
Client -> FastAPI -> JobService -> PostgreSQL jobs table
                                  ^          |
                                  |          +-- worker-1
                                  |          +-- worker-2
                                  |          +-- worker-3
                                  |          +-- worker-4
                                  |                 |
                                  +---- executions <- executor
```

The API and workers are separate processes. Workers atomically claim jobs with `SELECT ... FOR UPDATE SKIP LOCKED`, update a lease using database time, and increment a fencing token. Heartbeats renew leases; recovery requeues expired jobs or fails them when attempts are exhausted. Completion commits the unique database side effect and `succeeded` state in one transaction.

## Technology stack

- Python 3.12+
- FastAPI, Pydantic 2, SQLAlchemy 2 async
- PostgreSQL 16+, asyncpg
- Alembic migrations
- Docker / Docker Compose
- pytest, pytest-asyncio, httpx

## Folder structure

- `app/api`: HTTP routes, schemas, and dependencies.
- `app/models`: SQLAlchemy tables for jobs, executions, and workers.
- `app/repositories`: database access and atomic queue operations.
- `app/services`: business logic, cancellation, and the state machine.
- `app/workers`: claim, execute, heartbeat, recovery, and process loop.
- `app/core`: exceptions, constants, and durable metrics.
- `migrations`: Alembic schema history.
- `tests`: unit, API, database, concurrency, and opt-in chaos tests.
- `scripts`: readiness and chaos-test utilities.
- `docs`: design, failure modes, and honest test status.

## Installation

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Set `DATABASE_URL` in `.env` to a reachable PostgreSQL database. The default Compose URL is configured for the bundled service.

## Docker Compose startup

```bash
docker compose up --build
```

The API container waits for PostgreSQL, applies Alembic migrations, then starts Uvicorn. Four independent worker containers wait for the API health check before starting. API: `http://localhost:8000`; interactive docs: `http://localhost:8000/ docs`; health: `GET /healthz`.

To stop the stack:

```bash
docker compose down
```

For a clean test database only, `docker compose down -v` removes the PostgreSQL volume and all persisted data.

## API endpoints

### Create a job

```bash
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: experiment-001' \
  -d '{"payload":{"steps":8,"step_delay_seconds":0.1,"samples":5000},"max_attempts":3}'
```

A new job returns `201 Created`; repeating the same idempotency key returns the original job with `200 OK`, even if the repeated payload differs. The key is globally unique in the database.

### Get a job

```bash
curl http://localhost:8000/jobs/<job-uuid>
```

### List jobs

```bash
curl 'http://localhost:8000/jobs?limit=20&offset=0&status=queued'
```

`limit` is 1–100; `offset` is zero or greater. Valid status filters: `queued`, `running`, `succeeded`, `failed`, `cancelled`.

### Cancel a job

```bash
curl -X DELETE http://localhost:8000/jobs/<job-uuid>
```

Queued jobs are cancelled immediately. Running jobs receive a cooperative cancellation request; the executor checks the flag and completion rechecks it under a row lock.

### Metrics

`GET /metrics` returns counts for total, successful, failed, cancelled, queued, running, retried jobs, expired leases, and average execution time. Metrics are computed from PostgreSQL, not process-local counters.

## Job lifecycle

Allowed transitions:

```text
queued -> running -> succeeded
   |         |----> failed
   |         |----> cancelled
   |         |----> queued (retry / recovery)
   +-------------> cancelled
```

Terminal states cannot be reopened. Attempt increments on claim, not on every failure acknowledgement. Each claim increments the fencing token.

## Worker recovery and fencing

A worker owns a job only while the row is `running`, its `worker_id` matches, the lease is unexpired, and its fencing token matches. Lease renewal and final writes use conditional database updates/row locks. An expired lease is not proof that a process is dead, so a resumed old worker is treated as stale and cannot commit.

## Idempotency and delivery guarantee

`Idempotency-Key` is protected by a PostgreSQL unique constraint. Execution attempts are **at-least-once**: after a crash, work may be run again. The committed PostgreSQL effect is **at-most-once per job** because `executions.job_id` is unique and the effect plus `succeeded` state share one transaction. This does not guarantee exactly-once execution or exactly-once arbitrary external effects. External systems need their own idempotency/outbox protocol.

## Running tests

Install development requirements (included in `requirements.txt`) and run:

```bash
pytest -q
```

SQLite tests require `aiosqlite` and cover state transitions, API behavior, idempotency, cancellation, recovery, and the uniqueness guard. SQLite does not emulate PostgreSQL row locks; concurrency correctness must be tested against PostgreSQL:

```bash
TEST_DATABASE_URL='postgresql+asyncpg://user:password@localhost:5432/soup_test' pytest -q tests/test_concurrency.py
```

The PostgreSQL test database must already exist and be disposable. The tests create and drop the model schema.

## Running the chaos test

Run only against a disposable PostgreSQL database. Start the API and migrations first, but do not use the four Compose workers at the same time. Then run:

```bash
python scripts/run_chaos_test.py --jobs 10000 --workers 8
```

The harness creates 10,000 unique job rows, sends 1,000 additional requests that replay existing idempotency keys, sends concurrent cancellations, launches eight worker processes, and kills/restarts a random worker approximately every two seconds. It writes raw structured output to `docs/chaos-output.json` and per-process stdout/stderr logs under `docs/chaos-logs/`. It exits non-zero if an invariant fails. For the Compose database, first stop the four normal workers, then run the harness from the repository root with the host-side database URL:

```bash
docker compose stop worker-1 worker-2 worker-3 worker-4
DATABASE_URL='postgresql+asyncpg://soup:soup@127.0.0.1:5432/soup_queue' python scripts/run_chaos_test.py --jobs 10000 --workers 8
```

The API and harness must use the same disposable database. Do not run this against production or valuable development data.

## Known limitations

- The executor is a deterministic simulation, not an isolated training runtime. Do not execute untrusted model code in this process.
- No external side-effect adapter or transactional outbox is implemented.
- The queue has no priority, scheduled jobs, per-tenant quotas, backpressure policy, or job payload size enforcement.
- The metrics endpoint is a database snapshot, not a Prometheus exporter.
- The optional chaos harness is destructive and has not been run in this environment.
- SQLite is used only for fast logic/API tests; PostgreSQL is required to validate locking behavior.
- Database failover, backups, schema rollback, secret management, network policy, and production observability need deployment-specific work.

## Production readiness verdict

**DON'T SHIP.** Do not claim 100 RPS capacity without a representative load test. First priority: run PostgreSQL integration and 10k-job/8-worker chaos tests, fix every invariant failure, then benchmark API writes, claim throughput, database connection saturation, and recovery latency under realistic load. A production rollout also needs a sandboxed trainer, external-effect strategy, security review, monitoring/alerting, and operational runbooks.
