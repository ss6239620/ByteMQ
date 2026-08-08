# Developer Docs

This folder explains how to work on ByteMQ as a codebase. Conceptual material
lives in `concept-docs/`. Project-wide rules live in `rules.md`.

Before changing code, read:

1. `rules.md`
2. `architecture.md`
3. This file
4. The concept doc related to the part you are changing

## Developer Mindset

ByteMQ is a systems project. Small code changes can affect correctness under
crashes, retries, concurrent workers, and database contention.

When working on ByteMQ, always ask:

- Can this lose a job?
- Can this run a job twice?
- Can this leave a job stuck forever?
- Can this block shutdown?
- Can this break recovery after a process crash?
- Can an operator understand what happened?

Running a job twice is sometimes acceptable because ByteMQ is at-least-once.
Losing a job after enqueue succeeds is not acceptable.

## Expected Repository Shape

The initial implementation should follow this shape unless the architecture
changes deliberately:

```text
cmd/bytemq/
internal/api/
internal/app/
internal/config/
internal/domain/
internal/observability/
internal/scheduler/
internal/store/postgres/
internal/worker/
migrations/
dev-docs/
concept-docs/
```

Each folder should stay focused:

- `cmd/bytemq`: process startup and dependency wiring.
- `internal/api`: HTTP routes and request/response mapping.
- `internal/app`: use cases such as enqueue, lease, complete, fail, inspect.
- `internal/domain`: core types and state transition rules.
- `internal/scheduler`: leasing, expired lease recovery, retry scheduling.
- `internal/worker`: handler registration, execution, heartbeat, shutdown.
- `internal/store/postgres`: SQL-backed durable state.
- `internal/config`: config loading and validation.
- `internal/observability`: logs, metrics, and tracing setup.
- `migrations`: database schema changes.

## How to Approach a Change

1. Identify the layer where the behavior belongs.
2. Read the relevant concept doc.
3. Add or update tests for the behavior.
4. Implement the smallest change that satisfies the behavior.
5. Run formatting and verification.
6. Update docs if behavior or architecture changed.

Do not start in the HTTP layer unless the change is only API mapping. Most
business behavior belongs in `internal/app`, `internal/domain`,
`internal/scheduler`, `internal/worker`, or `internal/store/postgres`.

## Verification Expectations

Once the Go project exists, the default verification set should be:

```bash
gofmt -w .
go test ./...
go vet ./...
```

When concurrency-sensitive code changes:

```bash
go test -race ./...
```

When linting is configured:

```bash
golangci-lint run
```

When PostgreSQL integration tests exist, run the documented integration test
command before claiming storage, lease, or recovery behavior works.

## Review Checklist

Use this checklist before opening or accepting a change:

- Does it preserve at-least-once semantics?
- Does it avoid claiming exactly-once behavior?
- Are database transaction boundaries explicit?
- Are long-running operations outside database transactions?
- Does every goroutine have a shutdown path?
- Are contexts passed to blocking operations?
- Are state transitions validated?
- Are logs useful without exposing secrets or full payloads?
- Are tests deterministic?
- Are docs updated if behavior changed?

## Documentation Map

- `../rules.md`: mandatory coding rules and AI contributor policy.
- `../architecture.md`: project idea, components, lifecycle, and phases.
- `../concept-docs/job-lifecycle.md`: job states and transitions.
- `../concept-docs/leases-and-heartbeats.md`: worker ownership and recovery.
- `../concept-docs/retries-idempotency.md`: failure handling and duplicate
  safety.
- `../concept-docs/scheduling-and-backpressure.md`: scheduling direction and
  future pressure controls.
- `../concept-docs/observability-and-operations.md`: logs, metrics, timelines,
  and operational debugging.

