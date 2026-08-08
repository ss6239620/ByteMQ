# Job Lifecycle

The job lifecycle is the backbone of ByteMQ. Every component exists to move jobs
through this lifecycle safely.

## Basic Lifecycle

The v1 lifecycle is:

```text
queued -> leased -> running -> completed
                    |
                    v
                  failed -> retry_scheduled -> queued
                    |
                    v
               dead_lettered
```

The exact implementation may combine or rename some states, but the behavior
must remain explicit.

## Main States

### Queued

The job is durable in PostgreSQL and eligible to run when its `run_after` time
arrives.

Queued jobs are not owned by any worker.

### Leased

A worker has temporary ownership of the job. The lease includes:

- Job ID.
- Worker ID.
- Lease ID.
- Lease expiry time.

Only the active lease owner can heartbeat, complete, or fail the job.

### Running

The worker is actively executing the handler. This may be represented as part of
the leased state in storage, but it is useful conceptually.

### Completed

The job finished successfully. This is a terminal state.

Completed jobs should not be leased again.

### Failed

An attempt failed. Failed is often an intermediate decision point:

- If attempts remain, schedule a retry.
- If attempts are exhausted, dead-letter the job.

Each failure should preserve error details.

### Retry Scheduled

The job is waiting for a future retry time. It should not be leased until
`run_after` arrives.

### Dead-Lettered

The job exhausted retries or hit a permanent failure policy. This is a terminal
state unless a future explicit requeue operation is designed.

Dead-lettered jobs must preserve enough information for debugging:

- Payload reference or payload.
- Final error.
- Attempt history.
- Worker IDs.
- Timing.
- Job events.

## Important Transitions

### Enqueue

```text
request -> validate -> insert job -> return job id
```

Once enqueue succeeds, the job must be durable.

### Lease

```text
queued job -> active lease owned by worker
```

Leasing must be atomic. Two workers must not successfully lease the same job at
the same time.

### Heartbeat

```text
active lease -> extended lease expiry
```

Heartbeat proves the worker is still alive enough to keep ownership.

### Complete

```text
active lease -> completed
```

Completion must verify active lease ownership. A stale worker must not complete
a job after losing its lease.

### Fail

```text
active lease -> failed attempt -> retry or dead-letter
```

Failure must record the attempt before deciding what happens next.

### Recover Expired Lease

```text
expired lease -> queued or retry_scheduled
```

If a worker disappears, the job must not stay leased forever.

## State Transition Rules

- Terminal states are terminal by default.
- Only active lease owners can complete or fail leased jobs.
- Retry scheduling must respect the retry policy.
- Expired leases must be recoverable.
- Every meaningful transition should create a job event.

## Why This Matters

Most production queue bugs are lifecycle bugs:

- A job is marked running but nobody owns it.
- A job fails but retry metadata is wrong.
- A completed job is accidentally retried.
- A crashed worker leaves a job stuck forever.
- Two workers both run the same job without the system realizing it.

ByteMQ should make these states and transitions boring, visible, and testable.

