# Test Results

**Last updated:** 2026-11-11

## 1. Default test suite

Command:
docker compose exec api python -m pytest -q

Result:
- 16 passed
- 5 skipped
- Duration: 1.01 seconds

The skipped tests include the opt-in chaos test and four PostgreSQL concurrency tests.

## 2. PostgreSQL concurrency tests

Executed against the isolated `soup_test` database.

Command:
docker compose exec -e TEST_DATABASE_URL=postgresql+asyncpg://soup:soup@postgres:5432/soup_test api python -m pytest -q tests/test_concurrency.py

Result:
- 4 passed
- Duration: 1.43 seconds

Scenarios:
- Multiple workers claim jobs without duplicate claims.
- Stale fencing tokens are rejected.
- Concurrent cancellation and completion.
- Concurrent idempotency requests create one job.

## 3. Chaos test

Executed against the isolated `soup_chaos` database.

Configuration:
- 10,000 unique job requests
- 1,000 duplicate idempotency requests
- 8 worker processes
- 3 worker kill events
- 270 cancellation requests

Observed results:
- Database jobs: 10,000
- Successful jobs: 9,604
- Cancelled jobs: 396
- Failed jobs: 0
- Retried jobs: 139
- Committed side effects: 9,604
- Duplicate committed effects: 0
- Stuck running jobs: 0
- Queued jobs at end: 0
- Running jobs at end: 0
- Idempotency replay checks passed: 1,000

The chaos harness reported `passed: true`.

### Cancellation reconciliation caveat

The harness recorded 270 cancellation requests, while the final database contained 396 cancelled jobs.

A database inspection confirmed that all 396 cancelled jobs had `cancel_requested = true`. The additional 126 cancellations have not been fully explained.

Cancellation accounting therefore requires further investigation.

## 4. Database isolation

- `soup_test`: PostgreSQL concurrency tests.
- `soup_chaos`: destructive chaos test.
- `soup_queue`: normal application database, kept separate.

## 5. Remaining verification

- 100 RPS benchmark.
- Worker throughput and database connection saturation.
- Recovery latency under sustained load.
- Security review.
- External side-effect and execution sandbox validation.
- Investigation of cancellation count discrepancy.

## 6. Summary

The default test suite passed, all four PostgreSQL concurrency tests passed, and the chaos harness reported success for the 10,000-job scenario.

These results do not establish production readiness or a specific throughput capacity.

**Production verdict: DON'T SHIP yet.**
