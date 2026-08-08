# ByteMQ Code Rules

This file is mandatory reading for every human or AI contributor before changing
the project. ByteMQ is a learning-first, production-shaped distributed job
runtime. Code should be simple enough to study and serious enough to evolve into
production infrastructure.

## Project Principles

1. Favor correctness over cleverness.
2. Make distributed-system behavior explicit: leases, retries, timeouts,
   idempotency, ownership, and recovery must be visible in code and tests.
3. Start with a modular monolith, but keep service boundaries clean enough that
   API, scheduler, and worker processes can split later.
4. Prefer small, boring, well-tested components over broad abstractions.
5. Do not claim exactly-once execution. ByteMQ provides at-least-once execution
   and helps users build idempotent jobs.
6. Every production behavior needs an operational explanation: what happened,
   why it happened, and what happens next.

## Required Design Boundaries

ByteMQ starts as one Go binary with multiple runtime modes:

```bash
bytemq dev
bytemq server
bytemq scheduler
bytemq worker
```

The code must preserve these logical boundaries:

- API server: validates requests, authenticates clients, accepts jobs, and
  exposes read APIs.
- Scheduler: decides which jobs are eligible to run and manages leases.
- Worker runtime: polls for work, executes registered handlers, heartbeats
  leases, and reports completion or failure.
- Store: owns durable PostgreSQL persistence and transaction boundaries.
- Domain model: defines jobs, attempts, leases, queues, workers, and state
  transitions without depending on HTTP, CLI, or database drivers.

Do not let HTTP handlers, CLI commands, or worker code directly mutate database
tables. They must go through application services or store interfaces.

## Go Coding Policy

Use idiomatic Go. Keep code direct, explicit, and easy to trace.

### Formatting and Style

- Run `gofmt` on all Go files.
- Use `go test ./...` before claiming work is complete.
- Use `go vet ./...` and `golangci-lint run` when those tools are configured.
- Prefer clear names over short names outside tiny scopes.
- Keep package names short, lowercase, and meaningful.
- Avoid package names like `common`, `utils`, `helpers`, or `misc`.
- Avoid global mutable state.
- Keep exported APIs small. Export only what another package truly needs.
- Add comments for exported types and functions when their behavior is not
  obvious from the name.

### Package Design

Packages should have one reason to exist. A good package boundary can be
explained in one sentence.

Recommended early shape:

```text
cmd/bytemq/              CLI entrypoint
internal/api/            HTTP API server
internal/app/            Application services and use cases
internal/domain/         Core types and state rules
internal/scheduler/      Job selection, leases, recovery
internal/worker/         Worker runtime and handler execution
internal/store/postgres/ PostgreSQL implementation
internal/config/         Configuration loading and validation
internal/observability/  Logs, metrics, traces
migrations/              Database migrations
```

Rules:

- `internal/domain` must not import infrastructure packages.
- `internal/store/postgres` may depend on database libraries, but domain logic
  should not depend on PostgreSQL-specific details.
- `internal/api` should be thin. Put business behavior in `internal/app`.
- `internal/worker` should not know SQL details.
- `cmd/bytemq` should wire dependencies, not contain business logic.

### Error Handling

- Return errors. Do not panic for normal runtime failures.
- Wrap errors with context using `fmt.Errorf("...: %w", err)`.
- Use typed errors or sentinel errors only when callers need to branch on them.
- Log errors at process boundaries, not at every layer.
- Never swallow errors silently.
- Include job ID, queue name, worker ID, attempt number, and lease ID in errors
  when relevant.

### Context and Cancellation

- Functions that perform I/O or can block must accept `context.Context`.
- The first parameter should be `ctx context.Context`.
- Respect cancellation in polling loops, schedulers, workers, and database calls.
- Do not store contexts in structs unless there is a strong lifecycle reason.

### Concurrency

- Every goroutine must have a clear owner and shutdown path.
- Long-running loops must listen for context cancellation.
- Use channels for ownership transfer and signaling, not as hidden queues unless
  the design explicitly calls for it.
- Protect shared state with mutexes or confinement. Do not rely on luck.
- Run race-sensitive code with `go test -race ./...` when practical.
- Heartbeat, lease renewal, job execution, and graceful shutdown must be tested.

### Database and Transactions

- PostgreSQL is the v1 source of truth.
- Keep transaction scopes short and explicit.
- Use row-level locking deliberately. Document queries that rely on
  `FOR UPDATE`, `SKIP LOCKED`, advisory locks, or isolation behavior.
- Never perform slow external work while holding a database transaction.
- Schema changes must go through migrations.
- Use indexes for scheduler-critical queries.
- Use UTC timestamps in the database.
- Use database time for lease expiry decisions when possible, so workers with
  skewed clocks do not corrupt scheduling behavior.

### Job Semantics

- ByteMQ is at-least-once by design.
- Jobs must be safe to retry when users provide idempotency keys.
- State transitions must be explicit and validated.
- A job must never move from terminal states back to active states unless a
  specific requeue operation is designed and tested.
- Failed jobs must retain enough error detail for debugging.
- Dead-lettered jobs must preserve payload, final error, attempts, and timeline.
- Leased jobs must be recoverable after worker crash or heartbeat timeout.

### Time and Scheduling

- Use `time.Duration` for durations in Go code.
- Store absolute timestamps in UTC.
- Do not compare human strings like `"10m"` inside core logic. Parse at config
  or API boundaries.
- Scheduler decisions must be deterministic enough to test.
- Any adaptive behavior must expose the reason for its decision.

### Configuration

- Configuration should come from explicit files, environment variables, or CLI
  flags.
- Validate configuration at startup and fail fast with clear errors.
- Do not hide production-critical defaults. Lease durations, heartbeat intervals,
  retry limits, and poll intervals must be visible.
- Secrets must never be logged.

### Logging, Metrics, and Tracing

- Use structured logging.
- Logs should include stable fields such as `job_id`, `queue`, `worker_id`,
  `attempt`, `lease_id`, and `tenant_id` when available.
- Metrics must focus on operational questions:
  - How many jobs are pending, leased, completed, failed, and dead-lettered?
  - How long do jobs wait before lease?
  - How long do jobs run?
  - How often do retries happen?
  - Are leases expiring?
  - Are workers heartbeating?
- Traces should connect enqueue, scheduling, worker execution, and completion
  when tracing is added.

### Testing Policy

Every meaningful behavior needs tests near the layer where it lives.

Required test categories:

- Unit tests for domain state transitions.
- Store tests for PostgreSQL lease, retry, and recovery queries.
- Scheduler tests for job selection and expired lease recovery.
- Worker tests for heartbeat, success, failure, retry, and graceful shutdown.
- Integration tests for API -> store -> scheduler -> worker flow.
- Chaos-style tests later for worker crash, scheduler restart, database restart,
  duplicate delivery, and retry storms.

Tests should be deterministic. Avoid sleeps when a fake clock or controlled
clock can be used.

### API Policy

- APIs must be versioned once public.
- Request validation belongs at the API boundary.
- Idempotency keys should be accepted on enqueue.
- API errors should be structured and safe to show to clients.
- Do not leak internal database errors directly to clients.

### Security Policy

- Treat job payloads as sensitive by default.
- Do not log full payloads unless explicitly configured for local development.
- Keep API keys, database URLs, and secrets out of logs and committed files.
- Design multi-tenant features so tenant ID is part of every relevant query once
  tenancy is introduced.

### Dependency Policy

- Prefer the Go standard library when it is enough.
- Add third-party dependencies only when they remove meaningful complexity.
- Keep infrastructure dependencies replaceable behind interfaces where useful.
- Do not create interfaces before there are real consumers, except around the
  storage boundary where future backends are an explicit project goal.

### Documentation Policy

- Update docs when changing behavior, architecture, or developer workflow.
- Concept docs explain why the system works.
- Dev docs explain how to work on the system.
- Code comments explain non-obvious local decisions.
- Do not let docs promise behavior that tests do not cover.

## AI Contributor Instructions

AI agents must follow these rules:

1. Read `rules.md`, `architecture.md`, `dev-docs/README.md`, and
   `concept-docs/README.md` before making broad changes.
2. Preserve the learning-first, production-shaped goal.
3. Keep changes small and reviewable.
4. Do not skip tests because the project is "early."
5. Do not add dashboard, Kubernetes, Redis, NATS, workflows, or multi-region
   features before the reliable PostgreSQL core is solid.
6. Do not invent exactly-once guarantees.
7. Do not bypass store interfaces from API, scheduler, or worker code.
8. If a design decision is unclear, document the assumption in the relevant doc
   before implementing.
9. Prefer making a concept easier to understand over making code look advanced.
10. Before saying work is complete, run the relevant verification commands and
    report what passed or what could not be run.

## Definition of Done

A change is done only when:

- The behavior is implemented at the correct layer.
- Tests cover success and important failure paths.
- Relevant docs are updated.
- Formatting and verification commands pass.
- Operational impact is understood: logs, metrics, retries, failure behavior,
  and recovery behavior are considered.

