# Design overview

The API writes jobs to PostgreSQL; independent worker processes claim them from the same table. A worker opens a short transaction, selects the oldest `queued` row with `FOR UPDATE SKIP LOCKED`, changes it to `running`, increments `attempt` and the monotonically increasing `fencing_token`, and assigns a database-time lease. The row lock plus `SKIP LOCKED` prevents two live transactions from claiming the same row. The database is the source of truth; no Redis/Celery broker is involved.

Workers renew leases using a conditional update that must match job ID, worker ID, token, running state, and an unexpired lease. Recovery workers lock expired rows with `SKIP LOCKED`; they cancel, requeue, or fail them according to the cancel flag and attempt limit. A worker that wakes late is stale: completion, failure, and heartbeat writes are conditional on the current owner and fencing token, so an old process cannot overwrite a newer attempt.

Queued cancellation changes the state immediately. Running cancellation sets `cancel_requested`; the executor polls it cooperatively. Cancellation and completion lock the same job row and commit through one transaction, so the transaction that wins the lock/commit order determines the final state. Completion writes the unique `executions.job_id` side effect and changes the job to `succeeded` in the same PostgreSQL transaction. This gives at-least-once execution attempts but at-most-once committed database effect per job. It does **not** give exactly-once execution, and cannot make arbitrary external calls exactly once. External effects need their own idempotency key/outbox/inbox protocol or a transactional integration.

PostgreSQL is used because transactional row locks, `SKIP LOCKED`, unique constraints, JSONB, and conditional updates provide the coordination primitives required by this queue. Leases recover from dead processes; fencing tokens reject stale owners. Remaining risks include long-running/non-cooperative training code, database outages, clock behavior in external systems, migrations and operational limits, and external side effects outside the database transaction. The included executor is only a simulation, not a secure training sandbox.

## Implementation boundaries

The current implementation provides:

- PostgreSQL-backed durable job storage.
- Independent worker processes.
- Atomic job claiming with row-level locking.
- Lease renewal and expired-job recovery.
- Fencing-token protection against stale workers.
- Cooperative cancellation.
- Idempotent job creation.
- At-most-once committed PostgreSQL execution effects.

The following are outside the current implementation scope:

- Secure isolation for arbitrary user-supplied training code.
- Exactly-once external API or service effects.
- Multi-tenant authorization and quotas.
- Production-grade autoscaling and backpressure.
- High-availability database failover.
- Prometheus-based monitoring and alerting.

These boundaries are intentional and should be considered before production deployment.
