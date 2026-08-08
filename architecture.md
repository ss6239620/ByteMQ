# ByteMQ Architecture

ByteMQ is a learning-first, production-shaped distributed job runtime written in
Go. The project starts as a reliable PostgreSQL-backed queue, then grows toward
an explainable distributed execution platform.

The goal is not to clone BullMQ, Celery, or Temporal. ByteMQ should teach and
implement the production mechanics behind background job execution: durable
state, leases, worker heartbeats, retries, idempotency, recovery, scheduling,
backpressure, and operational visibility.

## Product Thesis

Most queues answer this question:

```text
How do I put work into a queue and run it later?
```

ByteMQ should eventually answer a deeper production question:

```text
How do I reliably execute many jobs across unreliable workers while
understanding capacity, retries, failures, fairness, and why each job is waiting?
```

The first version should be intentionally small. A queue that cannot recover
from worker crashes is not production-ready, no matter how many features it has.
ByteMQ v1 therefore focuses on the reliable core before adding advanced
scheduling or dashboards.

## Core Design Goals

- Learn serious systems concepts through readable Go code.
- Use PostgreSQL as the durable source of truth in v1.
- Run as one binary at first, while preserving service boundaries.
- Support at-least-once execution with idempotency support.
- Make job state transitions explicit and testable.
- Recover leased jobs when workers crash or disappear.
- Keep enough history to explain what happened to a job.
- Add advanced scheduling only after the reliable lifecycle is solid.

## Non-Goals for v1

ByteMQ v1 will not include:

- Redis or NATS storage backends.
- Kubernetes autoscaling.
- Multi-region replication.
- GPU scheduling.
- Durable workflows like Temporal.
- A polished dashboard.
- Exactly-once execution guarantees.
- Complex multi-tenant billing or organization management.

These may become future features, but they should not distract from the core
lease, execution, acknowledgment, retry, and recovery protocol.

## Runtime Shape

ByteMQ starts as a single Go binary with multiple modes:

```bash
bytemq dev
bytemq server
bytemq scheduler
bytemq worker
```

`bytemq dev` runs the API server, scheduler, and worker runtime together for
local learning and fast feedback. The other modes run each role separately.

This lets the project begin as a modular monolith while keeping a clear path to
separate services later.

## High-Level Architecture

```text
                         +------------------+
                         | Application Code |
                         | enqueue(job)     |
                         +--------+---------+
                                  |
                                  v
                         +------------------+
                         | API Server       |
                         | validation       |
                         | idempotency      |
                         | read APIs        |
                         +--------+---------+
                                  |
                                  v
                         +------------------+
                         | PostgreSQL Store |
                         | jobs             |
                         | attempts         |
                         | leases           |
                         | workers          |
                         | events           |
                         +--------+---------+
                                  |
                    +-------------+-------------+
                    |                           |
                    v                           v
           +------------------+        +------------------+
           | Scheduler        |        | Worker Runtime   |
           | lease jobs       |        | poll jobs        |
           | recover expired  |        | heartbeat        |
           | schedule retries |        | execute handler  |
           +------------------+        +------------------+
```

In v1, all components can live inside one process during development. The code
should still treat them as separate roles.

## Main Components

### CLI

The CLI starts ByteMQ in different runtime modes. It loads configuration, opens
database connections, runs migrations when explicitly requested, wires services,
and handles shutdown.

The CLI should not contain queue business logic.

### API Server

The API server accepts job enqueue requests and exposes operational read APIs.

Early API responsibilities:

- Create jobs.
- Accept idempotency keys.
- Validate queue name, job type, payload, timeout, and retry policy.
- Fetch job status and job timeline.
- List jobs by state.

The API server should call application services, not database tables directly.

The first HTTP API exposes `POST /v1/jobs` and `GET /v1/jobs/{id}` as thin JSON
adapters over the app service. It does not import PostgreSQL code.

### Application Services

Application services coordinate use cases such as enqueueing a job, fetching job
status, canceling a job later, or retrying a dead-lettered job later.

This layer protects the domain model from transport details and protects API
handlers from storage details.

The first app service exposes enqueue and lookup use cases. It applies a
default retry policy when a caller does not provide one, generates job IDs, and
delegates durable behavior to the `JobStore` contract.

### Domain Model

The domain model defines the concepts and allowed state transitions.

Core v1 concepts:

- Queue
- Job
- Job state
- Attempt
- Lease
- Worker
- Retry policy
- Dead-letter state
- Job event

The domain model should be independent from PostgreSQL, HTTP, and CLI packages.

### PostgreSQL Store

PostgreSQL is the v1 durable state layer.

Store responsibilities:

- Insert jobs transactionally.
- Enforce idempotency keys.
- Select eligible jobs for lease.
- Create and renew leases.
- Mark jobs completed or failed.
- Schedule retries.
- Move exhausted jobs to dead-letter state.
- Recover expired leases.
- Persist job events for debugging.

The store should expose behavior-oriented methods rather than table-shaped
methods. For example, prefer `LeaseNextJob` over exposing raw SQL from callers.

### Scheduler

The scheduler decides which jobs are eligible to run and makes them available to
workers through leases.

In v1, the scheduler can be simple:

- Find pending jobs whose `run_after` is due.
- Respect basic queue ordering.
- Recover jobs whose leases expired.
- Schedule retry attempts when backoff has elapsed.

Later, the scheduler can grow into ByteMQ's main differentiator:

- Priority scheduling.
- Deadline-aware scheduling.
- Tenant fairness.
- Resource-aware scheduling.
- Backpressure.
- Explainable scheduling state.
- Adaptive scheduling based on observed runtime and failures.

### Worker Runtime

The worker runtime executes jobs.

Worker responsibilities:

- Register job handlers by job type.
- Poll for leased work or request a lease.
- Execute handlers with context and timeout.
- Renew heartbeat while work is running.
- Mark jobs completed on success.
- Mark attempts failed on error.
- Let retry policy decide whether a job retries or dead-letters.
- Shut down gracefully without abandoning unclear state.

Workers must assume they can crash at any point. The store and scheduler must be
able to recover from that.

The first worker runtime provides a deterministic `ProcessOne` operation. It
leases one job, starts it, heartbeats while the handler runs, and reports either
completion or failure through the store ownership protocol.

### Observability

Observability starts simple, but it is part of the architecture from day one.

Minimum v1 visibility:

- Structured logs.
- Job state counts.
- Queue depth.
- Job wait time.
- Job execution duration.
- Retry count.
- Failed and dead-lettered counts.
- Lease expiry count.
- Worker heartbeat status.

Later:

- Prometheus metrics.
- OpenTelemetry traces.
- Job timeline UI.
- Failure classification.
- "Why is my job waiting?" explanations.

## Job Lifecycle

The v1 lifecycle:

```text
queued -> leased -> running -> completed
                    |
                    v
                  failed -> retry_scheduled -> queued
                    |
                    v
               dead_lettered
```

More precise state naming may change during implementation, but the behavior
should stay clear:

1. A producer enqueues a job.
2. The job is durable in PostgreSQL before the API returns success.
3. A scheduler or worker selects an eligible job.
4. The job receives a lease with owner and expiry.
5. A worker executes the job and heartbeats the lease.
6. On success, the job becomes completed.
7. On failure, the attempt is recorded.
8. If retries remain, the job is scheduled for a future retry.
9. If retries are exhausted, the job becomes dead-lettered.
10. If a worker disappears, the lease expires and the job becomes eligible
    again.

## Lease Model

Leases are the core production mechanism.

A lease says:

```text
worker X owns job Y until time Z
```

If the worker finishes before expiry, it completes or fails the job. If the
worker crashes, the lease expires and the job can be retried by another worker.

Important rules:

- A worker can only complete a job if it owns the active lease.
- Heartbeats extend the lease before it expires.
- Expired leases are recovered by the scheduler.
- Lease decisions should use database time where possible.
- Lease expiry does not mean the previous worker definitely stopped. This is why
  ByteMQ must remain at-least-once.

## Delivery Semantics

ByteMQ provides at-least-once execution.

This means:

- A job should not be lost after enqueue succeeds.
- A job may run more than once after crashes, timeouts, network failures, or
  ambiguous acknowledgments.
- Users should use idempotency keys and idempotent handlers for side-effecting
  jobs.

ByteMQ should never market v1 as exactly-once. The honest production promise is
durable state, explicit retries, duplicate-aware design, and tooling that helps
users avoid harmful duplicate side effects.

## Idempotency

Idempotency prevents duplicate enqueue or duplicate side effects from causing
incorrect behavior.

In v1, enqueue should support an optional idempotency key. For the same queue
and idempotency key, ByteMQ should return the existing job instead of creating
unbounded duplicates.

Future SDKs can make idempotency easier by encouraging keys such as:

```text
payment:charge:customer-123:invoice-456
report:daily:tenant-9:2026-08-08
```

Handler-level idempotency still belongs to the user's application because
ByteMQ cannot know whether an external payment, email, or file upload already
succeeded.

## Retry and Dead-Letter Strategy

Retries must be explicit. A retry policy should include:

- Maximum attempts.
- Backoff strategy.
- Initial delay.
- Maximum delay.
- Optional jitter later.

A failed attempt records:

- Error message.
- Error type when available.
- Attempt number.
- Worker ID.
- Start and finish times.
- Whether it will retry.

When attempts are exhausted, the job moves to dead-lettered state and keeps its
payload, final error, and timeline for inspection.

## Backpressure Direction

Backpressure is not required in the first implementation, but the architecture
should prepare for it.

ByteMQ should eventually measure:

- Ingestion rate.
- Processing rate.
- Queue growth.
- Estimated drain time.
- Worker capacity.
- Retry storm risk.

Future controls:

- Producer throttling.
- Queue limits.
- Tenant quotas.
- Priority shedding.
- Autoscaling signals.

## Explainability Direction

One of ByteMQ's strongest future differentiators should be explaining scheduler
decisions.

Future examples:

```text
Job 123 is waiting because no worker supports the required capability.
Job 456 is waiting because tenant concurrency is exhausted.
Job 789 is retrying after HTTP 503 with 32 seconds of backoff remaining.
```

This should be designed as real state and decision records, not as decorative UI
text.

## Suggested Phases

### Phase 1: Reliable Core

- Go binary with `dev`, `server`, `scheduler`, and `worker` modes.
- PostgreSQL migrations.
- Enqueue API.
- Job state machine.
- Lease acquisition.
- Worker execution.
- Heartbeat renewal.
- Completion and failure.
- Retry with backoff.
- Dead-letter state.
- Expired lease recovery.
- Basic logs and metrics.
- Integration tests that kill or stop workers.

### Phase 2: Production Reliability

- Idempotency key hardening.
- Timeouts and cancellation.
- Graceful shutdown.
- Job expiration.
- Better retry policies.
- Operational read APIs.
- Prometheus metrics.
- Structured job timeline.

### Phase 3: Scheduling

- Priority.
- Delayed jobs.
- Scheduled jobs.
- Fair scheduling.
- Deadline-aware scheduling.
- Explainable scheduling reasons.

### Phase 4: Multi-Tenancy

- Organizations or tenants.
- Projects.
- API keys.
- Queue quotas.
- Tenant concurrency limits.
- Tenant rate limits.
- Fairness policies.

### Phase 5: Worker Intelligence

- Worker capabilities.
- Worker load reports.
- Job resource hints.
- Resource-aware scheduling.
- Autoscaling signals.

### Phase 6: Operations UI

- Queue overview.
- Job detail and timeline.
- Worker status.
- Failure analysis.
- Retry and dead-letter tools.
- Scheduling explanations.

## Early Success Criteria

ByteMQ v1 is successful when it can prove:

- Enqueued jobs survive process restarts.
- Workers can execute jobs concurrently.
- Worker crash does not permanently strand leased jobs.
- Jobs retry according to policy.
- Exhausted jobs move to dead-letter state.
- Duplicate enqueue with the same idempotency key is controlled.
- Logs and APIs can explain the state of a job.
- Tests cover the core failure paths.

## Architecture Rule of Thumb

If a feature does not make the reliable core easier to understand, easier to
operate, or harder to break, it probably does not belong in v1.
