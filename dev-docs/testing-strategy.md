# Testing Strategy

ByteMQ's tests must prove behavior under failure, not only happy paths.

## Testing Philosophy

The most important ByteMQ bugs are not syntax bugs. They are distributed-system
correctness bugs:

- A job is lost after enqueue succeeds.
- A job is stuck forever after a worker crashes.
- A worker completes a job after its lease expired.
- A retry happens immediately instead of respecting backoff.
- Two workers both believe they own the same job.
- Shutdown interrupts state reporting in an unsafe way.

Tests should make these failures hard to introduce.

## Test Layers

### Domain Tests

Domain tests should cover pure state rules:

- Valid state transitions.
- Invalid state transitions.
- Attempt counting.
- Retry exhaustion.
- Timeout decisions.
- Dead-letter rules.

These tests should be fast and not require PostgreSQL.

### Store Tests

Store tests should use PostgreSQL and verify durable behavior:

- Enqueue persists jobs.
- Idempotency keys prevent duplicate job creation.
- Eligible jobs can be leased.
- Two concurrent lease requests do not receive the same job.
- Lease renewal requires the active lease owner.
- Completion requires the active lease owner.
- Expired leases become recoverable.
- Retry scheduling sets the correct future time.
- Dead-lettered jobs retain payload and error details.

Store tests are where row locking and transaction behavior must be proven.

### Scheduler Tests

Scheduler tests should verify:

- Pending jobs become eligible.
- Future `run_after` jobs are skipped.
- Expired leases are recovered.
- Retry-ready jobs are made available.
- The scheduler exits cleanly on context cancellation.

The scheduler should be testable without sleeping for real time where possible.

### Worker Tests

Worker tests should verify:

- A registered handler can complete a job.
- Handler error marks an attempt failed.
- Retry policy is applied.
- Heartbeats run while the handler is active.
- Heartbeats stop after completion or failure.
- Timeout cancels the handler context.
- Graceful shutdown stops polling and waits for active work according to policy.

### Integration Tests

Integration tests should verify the complete path:

```text
API enqueue -> PostgreSQL -> scheduler lease -> worker execution -> completion
```

Additional integration paths:

- Worker failure -> retry -> success.
- Worker crash simulation -> lease expiry -> another worker executes.
- Duplicate enqueue with idempotency key -> same job returned.
- Exhausted retries -> dead-letter.

## Chaos Tests

Chaos tests are a later phase, but the project should be designed for them.

Future scenarios:

- Kill a worker while processing.
- Kill the scheduler while retries are due.
- Restart PostgreSQL during polling.
- Run many workers concurrently.
- Introduce slow handlers.
- Create a retry storm.
- Simulate clock skew by relying on database time.

## Determinism

Avoid tests that depend on arbitrary sleeps. Prefer:

- Fake clocks for domain logic.
- Short but controlled lease durations in integration tests.
- Polling with deadlines in tests that need asynchronous behavior.
- Database time for lease expiry behavior.

If a test must wait, it should wait with a clear timeout and explain why.

## Execution-Core Integration Tests

The PostgreSQL execution-core tests cover the job ownership protocol:

- Leasing only due queued jobs.
- Returning `ErrNoJobAvailable` when no job can be leased.
- Rejecting start, heartbeat, complete, and fail operations from the wrong worker
  or lease.
- Moving leased jobs to running and incrementing attempts on start.
- Extending active leases through heartbeat.
- Clearing lease fields on completion.
- Scheduling retries when attempts remain.
- Dead-lettering when attempts are exhausted.
- Recovering expired leased jobs back to queued.
- Recovering expired running jobs into retry or dead-letter states.

These tests should run against a disposable PostgreSQL database through
`BYTEMQ_TEST_DATABASE_URL`. The default suite still verifies that integration
tests compile and skip clearly when the variable is absent.

## Runtime Unit Tests

Runtime packages should stay database-independent:

- `internal/app` tests use a fake `JobStore` to prove enqueue defaults, ID
  generation, and lookup delegation.
- `internal/api` tests use `httptest` and a fake app/store path to prove JSON
  request and response behavior.
- `internal/worker` tests use a fake `JobStore` to prove lease, start,
  heartbeat, complete, and fail calls.
- `internal/scheduler` tests use a fake `JobStore` to prove expired-lease
  recovery requests.

These tests should not import `internal/store/postgres`.

## Minimum Done Bar

A reliability feature is not done until tests cover:

- The success path.
- The expected failure path.
- The recovery path.
- The invalid ownership or invalid state path when relevant.
