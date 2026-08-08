# Leases and Heartbeats

Leases are ByteMQ's most important reliability mechanism.

## What Is a Lease?

A lease is temporary ownership of a job.

```text
worker-17 owns job-123 until 10:30:30 UTC
```

The lease gives a worker permission to execute, heartbeat, complete, or fail a
job. The lease expires if the worker disappears.

## Why Leases Exist

Workers are unreliable:

- The process can crash.
- The machine can restart.
- The network can fail.
- The handler can hang.
- The worker can lose database connectivity.

Without leases, a worker crash could leave a job stuck forever.

With leases, ByteMQ can recover:

```text
worker gets job -> worker crashes -> lease expires -> job becomes eligible again
```

## Lease Ownership Rules

Only the active lease owner can:

- Renew the lease.
- Complete the job.
- Fail the job.

Every ownership-sensitive operation should check:

- Job ID.
- Lease ID.
- Worker ID.
- Lease has not expired.

This prevents stale workers from reporting success after the system has already
reassigned the job.

## Heartbeats

A heartbeat extends the lease while the worker is still processing.

Example:

```text
lease duration:      30 seconds
heartbeat interval: 10 seconds

10:00:00 lease until 10:00:30
10:00:10 heartbeat extends until 10:00:40
10:00:20 heartbeat extends until 10:00:50
```

The heartbeat interval should be shorter than the lease duration. If the worker
cannot heartbeat before the lease expires, the job may be recovered.

## Expiry and Duplicate Execution

Lease expiry does not prove the original worker stopped. It only proves the
worker failed to renew ownership in time.

This creates a possible duplicate execution:

```text
worker A gets job
worker A does external side effect
worker A loses connection before heartbeat
lease expires
worker B gets same job
worker B repeats side effect
```

This is why ByteMQ must be at-least-once and why idempotency is required for
side-effecting jobs.

## Database Time

Lease expiry should use database time when practical. This reduces bugs caused
by clock differences between workers and the database.

Prefer decisions like:

```sql
lease_expires_at < now()
```

inside PostgreSQL over trusting a worker's local clock for ownership decisions.

## Recovery Loop

The scheduler should periodically recover expired leases:

1. Find jobs with expired active leases.
2. Record a lease-expired event.
3. Increment or preserve attempt data according to the chosen policy.
4. Return the job to an eligible state or schedule retry.
5. Allow another worker to lease it.

Recovery must be safe to run more than once.

## Graceful Shutdown

When a worker shuts down, it should:

1. Stop accepting new jobs.
2. Let active jobs finish within a grace period.
3. Continue heartbeating while waiting.
4. Report success or failure for completed handlers.
5. Release or allow expiry for unfinished work according to policy.

The exact v1 behavior should be simple and documented before implementation.

## Key Tests

Lease behavior needs strong tests:

- Two workers cannot lease the same job.
- A worker can heartbeat its own active lease.
- A worker cannot heartbeat another worker's lease.
- A stale lease cannot complete a job.
- Expired leases are recovered.
- Recovered jobs can be leased by another worker.

