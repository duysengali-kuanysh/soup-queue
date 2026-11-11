# Failure modes and mitigations

| Failure | How it happens / risk | Protection | Verification |
|---|---|---|---|
| Double claim | Two workers select one queued row; duplicate training attempts | PostgreSQL `FOR UPDATE SKIP LOCKED` in a transaction; conditional state change | PostgreSQL contention test and 8-worker chaos run |
| Zombie worker | A paused process resumes after its lease expires and another worker reclaims the job | Lease ownership checks plus monotonically increasing fencing token on every write/commit | Expire a lease, reclaim, then assert old token is rejected |
| Stale result | An old process tries to overwrite a newer attempt | Completion/failure/heartbeat updates require matching worker ID, running state, unexpired lease, and token | `StaleWorkerError` path and PostgreSQL race test |
| Cancel/complete race | DELETE races the completion transaction | Both lock the job row; running cancel sets a flag; completion rechecks it under lock | Cancellation tests plus concurrent API/worker test |
| Duplicate retry | A failure acknowledgement is repeated or a lease expires after retry | State/owner/token guard; attempt increments only at claim; `max_attempts` checked in retry and recovery | Recovery and max-attempt tests |
| Transaction isolation | Partial effect could commit without succeeded state | Side effect insert and job success are one transaction; row-level lock serializes state transition | Side-effect uniqueness test; transaction rollback test |
| Clock skew | Worker host clocks disagree about lease expiry | Lease comparisons and renewal use PostgreSQL `NOW()` rather than worker wall clocks | Integration test with deliberately skewed worker clock |
| Crash after side effect | Process dies between effect and status update | Both are in one DB transaction, so both commit or both roll back | Kill process during transaction in chaos/integration testing |
| Lease expiration | Slow job does not heartbeat or network pauses | Heartbeat renews lease; recovery requeues or fails; old token is fenced | Recovery tests and chaos run |
| Database connection loss | Worker cannot renew or acknowledge | Heartbeat retries transient errors; any final write is guarded; lease recovery provides eventual retry | Fault-injection test by dropping DB connections |

A transient heartbeat DB failure is not treated as proof that a worker is dead; the worker keeps trying. This can allow wasted computation, but it cannot authorize a stale commit because the database validates ownership at commit time. Cancellation is cooperative: a trainer that never polls its stop callback may continue consuming resources, but the final commit gate still prevents it from committing success after cancellation has won.

## Verification status

The following scenarios have been exercised in the current test environment:

- PostgreSQL concurrent job claiming.
- Stale fencing-token rejection.
- Concurrent cancellation and completion.
- Idempotency under concurrent requests.
- Recovery and retry behavior in the 10,000-job chaos scenario.
- Unique committed execution effects.
- Worker process termination and restart during chaos testing.

The following areas still require dedicated fault-injection or production-level verification:

- Deliberate database connection loss during active execution.
- Worker host clock skew.
- Database failover and recovery.
- Long-running non-cooperative executor behavior.
- External side-effect failure and recovery.
- Sustained throughput and connection saturation.

The chaos run reported success, but cancellation accounting remains unresolved: 270 cancellation requests were recorded while 396 jobs ended in the cancelled state. See `TEST_RESULTS.md`.
