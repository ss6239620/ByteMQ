# Local Development

This document describes the intended local development workflow for ByteMQ. Some
commands will become active after the Go project and database migrations are
implemented.

## Local Runtime Goal

Local development should make the full job lifecycle easy to see:

```text
enqueue -> lease -> execute -> heartbeat -> complete
enqueue -> lease -> execute -> fail -> retry -> complete
enqueue -> lease -> worker crash -> lease expiry -> retry
```

The developer loop should favor clarity over hidden automation.

## Expected Local Dependencies

ByteMQ v1 should require:

- Go
- PostgreSQL
- A database migration tool selected during implementation
- Optional Docker Compose for local PostgreSQL

Redis, NATS, Kubernetes, and a web dashboard are not required for v1.

## Runtime Modes

The single binary should support these modes:

```bash
bytemq dev
bytemq server
bytemq scheduler
bytemq worker
```

Mode expectations:

- `dev`: starts API, scheduler, and a local worker in one process.
- `server`: starts only the API server.
- `scheduler`: starts only scheduling and recovery loops.
- `worker`: starts only worker execution.

Keeping separate modes early helps prove that the code can later split into
separate deployable services.

## Configuration

Configuration should be explicit. Expected settings:

```text
BYTEMQ_DATABASE_URL
BYTEMQ_HTTP_ADDR
BYTEMQ_WORKER_ID
BYTEMQ_LEASE_DURATION
BYTEMQ_HEARTBEAT_INTERVAL
BYTEMQ_POLL_INTERVAL
BYTEMQ_LOG_LEVEL
```

Production-critical defaults should be visible in startup logs, except secrets.

## Database Setup Direction

The project should provide a clear way to:

1. Start PostgreSQL locally.
2. Create the ByteMQ database.
3. Run migrations.
4. Reset local state when needed.

Migrations should be committed files. Runtime code should not silently create or
change tables unless an explicit dev-only command is chosen.

## Manual Reliability Exercises

The project should eventually include commands or scripts to test:

- Start ByteMQ in dev mode.
- Enqueue many jobs.
- Kill a worker during execution.
- Verify the lease expires.
- Verify another worker retries the job.
- Stop the scheduler.
- Restart the scheduler.
- Verify retry scheduling resumes.

These exercises are part of learning the system. They should be documented and
kept simple.

## Local Observability

Local logs should answer:

- Which job was leased?
- Which worker owns the lease?
- When did the heartbeat renew?
- Why did a job fail?
- Will it retry?
- When will it retry?
- Why did it dead-letter?

Metrics can start as logs or a simple endpoint, then move to Prometheus later.

