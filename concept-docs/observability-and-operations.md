# Observability and Operations

Production job systems are operated under stress. ByteMQ should help operators
understand what happened, what is happening, and what will happen next.

## Operational Questions

ByteMQ should eventually answer:

- Is the queue growing?
- Are workers alive?
- Are jobs waiting too long?
- Are leases expiring?
- Are retries increasing?
- Which jobs are dead-lettered?
- What error is most common?
- Why is this job waiting?
- What will happen to this failed job next?

These questions should shape logs, metrics, APIs, and future dashboard design.

## Structured Logs

Logs should be structured and consistent.

Useful fields:

```text
job_id
queue
job_type
worker_id
lease_id
attempt
state
event
duration_ms
error
retry_at
```

Logs should avoid full payloads by default because payloads may contain secrets
or customer data.

## Metrics

Early metrics should include:

- Jobs enqueued.
- Jobs completed.
- Jobs failed.
- Jobs dead-lettered.
- Jobs currently queued.
- Jobs currently leased.
- Lease expirations.
- Worker heartbeats.
- Retry count.
- Job wait duration.
- Job execution duration.

Prometheus can be added after the core runtime exists, but metric names and
meanings should remain stable once public.

## Job Timeline

A job timeline records important events:

```text
queued
leased by worker-1
heartbeat renewed
attempt failed
retry scheduled
leased by worker-2
completed
```

The timeline is useful for both learning and operations. It shows how the system
actually behaved.

V1 should store enough event data to build this view later.

## Failure Visibility

A failed job should expose:

- Error message.
- Attempt number.
- Worker ID.
- Start and finish times.
- Retry decision.
- Next retry time if any.
- Final dead-letter reason if exhausted.

Future failure intelligence can classify errors and suggest action, but raw
failure visibility comes first.

## Worker Visibility

Operators should be able to understand worker state:

- Worker ID.
- Last heartbeat.
- Active job count.
- Runtime mode.
- Started time.
- Graceful shutdown status later.

Future versions may add resource usage and capabilities.

## Why Is My Job Waiting?

This is a future differentiator for ByteMQ.

The system should be able to say:

```text
This job is waiting because its retry backoff ends in 32 seconds.
This job is waiting because no worker is polling this queue.
This job is waiting because tenant concurrency is exhausted.
```

Do not fake this with guessed UI messages. Scheduling explanations should come
from real scheduler state.

## Alerting Direction

Future alerts:

- Oldest queued job age exceeds threshold.
- Dead-letter rate spikes.
- Lease expiry rate spikes.
- No workers heartbeating.
- Retry storm detected.
- Queue drain time exceeds threshold.

Alerting should be based on metrics that already exist for normal operations.

## Operations Rule

For every important state change, ByteMQ should make it possible to answer:

```text
What happened?
Who or what caused it?
When did it happen?
Will the system retry?
What should a developer or operator inspect next?
```

