# PostgreSQL JobStore Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add ByteMQ's durable PostgreSQL foundation: store contracts, schema migrations, enqueue persistence, idempotent enqueue behavior, job lookup, and enqueue event recording.

**Architecture:** Keep storage behind `internal/store` contracts so API, scheduler, and worker code never mutate tables directly. Use PostgreSQL through a small `internal/store/postgres` adapter; this slice creates durable jobs and events but does not implement leasing yet.

**Tech Stack:** Go 1.26+, PostgreSQL, `github.com/jackc/pgx/v5` for PostgreSQL access, standard library `encoding/json`, `embed`, and `testing`.

## Global Constraints

- Follow `rules.md` before changing code.
- ByteMQ is learning-first and production-shaped.
- ByteMQ starts as one Go binary with modes: `dev`, `server`, `scheduler`, and `worker`.
- PostgreSQL is the v1 durable source of truth.
- ByteMQ provides at-least-once execution and must not claim exactly-once delivery.
- Store code must own durable PostgreSQL persistence and transaction boundaries.
- API, scheduler, worker, and CLI code must not mutate database tables directly.
- Use database migrations for schema changes.
- Use UTC timestamps in persisted job records.
- Do not add Redis, NATS, Kubernetes, dashboard, workflows, or multi-region features in this slice.
- Use TDD: write each behavior test, watch it fail, then implement the minimal code.
- Use only ASCII text in source files.

---

## File Structure

Create and modify this structure:

```text
go.mod
go.sum
internal/domain/errors.go
internal/store/store.go
internal/store/store_test.go
internal/store/postgres/migrations.go
internal/store/postgres/migrations_test.go
internal/store/postgres/store.go
internal/store/postgres/store_test.go
migrations/000001_init.down.sql
migrations/000001_init.up.sql
migrations/migrations.go
dev-docs/local-development.md
```

Responsibilities:

- `internal/store/store.go`: storage-facing request/record types and `JobStore` contract.
- `internal/store/store_test.go`: pure validation tests for enqueue requests.
- `migrations/000001_init.up.sql`: initial durable schema for jobs, attempts, and events.
- `migrations/000001_init.down.sql`: reversible teardown for the initial schema.
- `migrations/migrations.go`: embeds root-level SQL migration files so nested packages can read them.
- `internal/store/postgres/migrations.go`: embedded migration runner.
- `internal/store/postgres/migrations_test.go`: PostgreSQL integration test for schema creation.
- `internal/store/postgres/store.go`: PostgreSQL `JobStore` implementation for enqueue and get.
- `internal/store/postgres/store_test.go`: PostgreSQL integration tests for enqueue, idempotency, job lookup, and event recording.
- `dev-docs/local-development.md`: document `BYTEMQ_TEST_DATABASE_URL` for integration tests.

## Integration Test Policy

PostgreSQL tests must use `BYTEMQ_TEST_DATABASE_URL`.

If the environment variable is empty, integration tests skip with this exact message:

```text
set BYTEMQ_TEST_DATABASE_URL to run PostgreSQL integration tests
```

If the variable is set, tests must create a unique temporary schema, set `search_path` to that schema, run migrations there, and drop the schema at cleanup. Tests must not drop or truncate tables in the database's default schema.

## Task 1: Store Contract and Enqueue Validation

**Files:**
- Create: `internal/store/store.go`
- Test: `internal/store/store_test.go`

**Interfaces:**
- Consumes: `internal/domain.RetryPolicy`
- Consumes: `internal/domain.JobState`
- Produces: `var ErrInvalidJob error`
- Produces: `var ErrJobNotFound error`
- Produces: `type EnqueueJobRequest struct`
- Produces: `func (r EnqueueJobRequest) Validate() error`
- Produces: `type JobRecord struct`
- Produces: `type JobEventRecord struct`
- Produces: `type JobStore interface`

- [ ] **Step 1: Write failing store validation tests**

Create `internal/store/store_test.go`:

```go
package store

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/sharvesh/bytemq/internal/domain"
)

func validEnqueueRequest() EnqueueJobRequest {
	return EnqueueJobRequest{
		ID:      "job_123",
		Queue:   "default",
		Type:    "send_email",
		Payload: json.RawMessage(`{"email":"user@example.com"}`),
		RetryPolicy: domain.RetryPolicy{
			MaxAttempts:  3,
			Backoff:      domain.BackoffExponential,
			InitialDelay: time.Second,
			MaxDelay:     30 * time.Second,
		},
		RunAfter:       time.Date(2026, 8, 8, 10, 0, 0, 0, time.UTC),
		IdempotencyKey: "email:user@example.com",
	}
}

func TestEnqueueJobRequestValidateAcceptsValidRequest(t *testing.T) {
	req := validEnqueueRequest()

	if err := req.Validate(); err != nil {
		t.Fatalf("expected valid request, got %v", err)
	}
}

func TestEnqueueJobRequestValidateRejectsMissingRequiredFields(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*EnqueueJobRequest)
	}{
		{"missing id", func(req *EnqueueJobRequest) { req.ID = "" }},
		{"missing queue", func(req *EnqueueJobRequest) { req.Queue = "" }},
		{"missing type", func(req *EnqueueJobRequest) { req.Type = "" }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := validEnqueueRequest()
			tc.mutate(&req)

			if err := req.Validate(); err != ErrInvalidJob {
				t.Fatalf("expected ErrInvalidJob, got %v", err)
			}
		})
	}
}

func TestEnqueueJobRequestValidateRejectsInvalidPayload(t *testing.T) {
	req := validEnqueueRequest()
	req.Payload = json.RawMessage(`{"broken":`)

	if err := req.Validate(); err != ErrInvalidJob {
		t.Fatalf("expected ErrInvalidJob, got %v", err)
	}
}

func TestEnqueueJobRequestValidateRejectsInvalidRetryPolicy(t *testing.T) {
	req := validEnqueueRequest()
	req.RetryPolicy.MaxAttempts = 0

	if err := req.Validate(); err != ErrInvalidJob {
		t.Fatalf("expected ErrInvalidJob, got %v", err)
	}
}

func TestEnqueueJobRequestValidateDoesNotMutateRunAfter(t *testing.T) {
	req := validEnqueueRequest()
	localTime := time.Date(2026, 8, 8, 15, 30, 0, 0, time.FixedZone("IST", 19800))
	req.RunAfter = localTime

	if err := req.Validate(); err != nil {
		t.Fatalf("expected valid request, got %v", err)
	}
	if !req.RunAfter.Equal(localTime) || req.RunAfter.Location() != localTime.Location() {
		t.Fatalf("Validate should not mutate RunAfter")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/store`

Expected: FAIL because package `internal/store` and its types are missing.

- [ ] **Step 3: Implement store contract and validation**

Create `internal/store/store.go`:

```go
package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/sharvesh/bytemq/internal/domain"
)

var ErrInvalidJob = errors.New("invalid job")

var ErrJobNotFound = errors.New("job not found")

type EnqueueJobRequest struct {
	ID             string
	Queue          string
	Type           string
	Payload        json.RawMessage
	RetryPolicy    domain.RetryPolicy
	RunAfter       time.Time
	IdempotencyKey string
}

func (r EnqueueJobRequest) Validate() error {
	if strings.TrimSpace(r.ID) == "" {
		return ErrInvalidJob
	}
	if strings.TrimSpace(r.Queue) == "" {
		return ErrInvalidJob
	}
	if strings.TrimSpace(r.Type) == "" {
		return ErrInvalidJob
	}
	if len(r.Payload) == 0 || !json.Valid(r.Payload) {
		return ErrInvalidJob
	}
	if err := r.RetryPolicy.Validate(); err != nil {
		return ErrInvalidJob
	}
	return nil
}

type JobRecord struct {
	ID             string
	Queue          string
	Type           string
	Payload        json.RawMessage
	State          domain.JobState
	Attempt        int
	RetryPolicy    domain.RetryPolicy
	RunAfter       time.Time
	IdempotencyKey string
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type JobEventRecord struct {
	ID        int64
	JobID     string
	EventType string
	Message   string
	CreatedAt time.Time
}

type JobStore interface {
	EnqueueJob(ctx context.Context, req EnqueueJobRequest) (JobRecord, error)
	GetJob(ctx context.Context, id string) (JobRecord, error)
	ListJobEvents(ctx context.Context, jobID string) ([]JobEventRecord, error)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/store`

Expected: PASS.

## Task 2: Initial PostgreSQL Migrations

**Files:**
- Create: `migrations/000001_init.up.sql`
- Create: `migrations/000001_init.down.sql`
- Create: `migrations/migrations.go`
- Create: `internal/store/postgres/migrations.go`
- Test: `internal/store/postgres/migrations_test.go`
- Modify: `go.mod`
- Create or modify: `go.sum`

**Interfaces:**
- Consumes: `BYTEMQ_TEST_DATABASE_URL`
- Produces: `func Migrate(ctx context.Context, pool *pgxpool.Pool) error`
- Produces: PostgreSQL tables `schema_migrations`, `jobs`, `job_attempts`, `job_events`
- Produces: helper `newTestPool(t *testing.T) *pgxpool.Pool` in test files

- [ ] **Step 1: Add pgx dependency**

Run:

```bash
go get github.com/jackc/pgx/v5
```

Expected: `go.mod` and `go.sum` include pgx dependencies.

- [ ] **Step 2: Write failing migration integration test**

Create `internal/store/postgres/migrations_test.go`:

```go
package postgres

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	databaseURL := os.Getenv("BYTEMQ_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set BYTEMQ_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect admin pool: %v", err)
	}
	schema := fmt.Sprintf("bytemq_test_%d", time.Now().UnixNano())
	if _, err := adminPool.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		adminPool.Close()
		t.Fatalf("create test schema: %v", err)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = adminPool.Exec(cleanupCtx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
		adminPool.Close()
	})

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse pool config: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect schema pool: %v", err)
	}
	t.Cleanup(pool.Close)

	return pool
}

func TestMigrateCreatesInitialTables(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	for _, table := range []string{"schema_migrations", "jobs", "job_attempts", "job_events"} {
		t.Run(table, func(t *testing.T) {
			var exists bool
			err := pool.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM information_schema.tables
					WHERE table_schema = current_schema()
					AND table_name = $1
				)
			`, table).Scan(&exists)
			if err != nil {
				t.Fatalf("query table existence: %v", err)
			}
			if !exists {
				t.Fatalf("expected table %s to exist", table)
			}
		})
	}
}

func TestMigrateRecordsVersionOnce(t *testing.T) {
	ctx := context.Background()
	pool := newTestPool(t)

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("second migrate: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations WHERE version = $1`, "000001_init").Scan(&count); err != nil {
		t.Fatalf("count migration rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected migration version recorded once, got %d", count)
	}
}

func TestTestPoolUsesTemporarySchema(t *testing.T) {
	pool := newTestPool(t)

	var schema string
	if err := pool.QueryRow(context.Background(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatalf("query current schema: %v", err)
	}
	if !strings.HasPrefix(schema, "bytemq_test_") {
		t.Fatalf("expected temporary test schema, got %q", schema)
	}
}
```

- [ ] **Step 3: Run test to verify it fails or skips correctly**

Run: `go test ./internal/store/postgres`

Expected without `BYTEMQ_TEST_DATABASE_URL`: PASS with tests skipped.

Run with a test database:

```bash
BYTEMQ_TEST_DATABASE_URL=postgres://localhost/bytemq_test go test ./internal/store/postgres
```

Expected with a reachable test database: FAIL because `Migrate` is undefined.

- [ ] **Step 4: Add migration SQL files**

Create `migrations/000001_init.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS schema_migrations (
    version text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
    id text PRIMARY KEY,
    queue text NOT NULL,
    type text NOT NULL,
    payload jsonb NOT NULL,
    state text NOT NULL,
    attempt integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL,
    backoff_type text NOT NULL,
    initial_delay_ms bigint NOT NULL,
    max_delay_ms bigint NOT NULL DEFAULT 0,
    run_after timestamptz NOT NULL,
    leased_by text,
    lease_id text,
    lease_expires_at timestamptz,
    idempotency_key text,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT jobs_state_check CHECK (state IN ('queued', 'leased', 'running', 'completed', 'failed', 'retry_scheduled', 'dead_lettered')),
    CONSTRAINT jobs_attempt_check CHECK (attempt >= 0),
    CONSTRAINT jobs_max_attempts_check CHECK (max_attempts >= 1),
    CONSTRAINT jobs_delay_check CHECK (initial_delay_ms > 0 AND max_delay_ms >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_queue_idempotency_key_idx
    ON jobs (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS jobs_state_run_after_idx
    ON jobs (state, run_after, created_at);

CREATE INDEX IF NOT EXISTS jobs_lease_expiry_idx
    ON jobs (lease_expires_at)
    WHERE lease_expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS job_attempts (
    id bigserial PRIMARY KEY,
    job_id text NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt integer NOT NULL,
    worker_id text,
    lease_id text,
    started_at timestamptz,
    finished_at timestamptz,
    error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT job_attempts_attempt_check CHECK (attempt >= 1)
);

CREATE INDEX IF NOT EXISTS job_attempts_job_id_idx
    ON job_attempts (job_id, attempt);

CREATE TABLE IF NOT EXISTS job_events (
    id bigserial PRIMARY KEY,
    job_id text NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    event_type text NOT NULL,
    message text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS job_events_job_id_created_at_idx
    ON job_events (job_id, created_at, id);
```

Create `migrations/000001_init.down.sql`:

```sql
DROP TABLE IF EXISTS job_events;
DROP TABLE IF EXISTS job_attempts;
DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS schema_migrations;
```

- [ ] **Step 5: Implement root migration embedding**

Create `migrations/migrations.go`:

```go
package migrations

import "embed"

//go:embed *.up.sql
var Files embed.FS
```

- [ ] **Step 6: Implement embedded migration runner**

Create `internal/store/postgres/migrations.go`:

```go
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sharvesh/bytemq/migrations"
)

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version text PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("ensure schema_migrations: %w", err)
	}

	const version = "000001_init"
	var applied bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, version).Scan(&applied); err != nil {
		return fmt.Errorf("check migration %s: %w", version, err)
	}
	if applied {
		return nil
	}

	sqlBytes, err := migrations.Files.ReadFile("000001_init.up.sql")
	if err != nil {
		return fmt.Errorf("read migration %s: %w", version, err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", version, err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
		return fmt.Errorf("apply migration %s: %w", version, err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
		return fmt.Errorf("record migration %s: %w", version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %s: %w", version, err)
	}
	return nil
}
```

- [ ] **Step 7: Run migration tests**

Run:

```bash
go test ./internal/store/postgres
```

Expected without `BYTEMQ_TEST_DATABASE_URL`: PASS with tests skipped.

Run with a test database when available:

```bash
BYTEMQ_TEST_DATABASE_URL=postgres://localhost/bytemq_test go test ./internal/store/postgres
```

Expected with a reachable test database: PASS.

## Task 3: PostgreSQL Enqueue and GetJob

**Files:**
- Create: `internal/store/postgres/store.go`
- Test: `internal/store/postgres/store_test.go`

**Interfaces:**
- Consumes: `store.EnqueueJobRequest`
- Consumes: `store.JobRecord`
- Consumes: `store.JobEventRecord`
- Produces: `type Store struct`
- Produces: `func New(pool *pgxpool.Pool) *Store`
- Produces: `func (s *Store) EnqueueJob(ctx context.Context, req store.EnqueueJobRequest) (store.JobRecord, error)`
- Produces: `func (s *Store) GetJob(ctx context.Context, id string) (store.JobRecord, error)`
- Produces: `func (s *Store) ListJobEvents(ctx context.Context, jobID string) ([]store.JobEventRecord, error)`

- [ ] **Step 1: Write failing PostgreSQL store tests**

Create `internal/store/postgres/store_test.go`:

```go
package postgres

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sharvesh/bytemq/internal/domain"
	"github.com/sharvesh/bytemq/internal/store"
)

func validStoreRequest(id string) store.EnqueueJobRequest {
	return store.EnqueueJobRequest{
		ID:      id,
		Queue:   "default",
		Type:    "send_email",
		Payload: json.RawMessage(`{"email":"user@example.com"}`),
		RetryPolicy: domain.RetryPolicy{
			MaxAttempts:  3,
			Backoff:      domain.BackoffExponential,
			InitialDelay: time.Second,
			MaxDelay:     30 * time.Second,
		},
		RunAfter:       time.Date(2026, 8, 8, 10, 0, 0, 0, time.UTC),
		IdempotencyKey: "email:user@example.com",
	}
}

func migratedStore(t *testing.T) (*Store, context.Context) {
	t.Helper()

	ctx := context.Background()
	pool := newTestPool(t)
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return New(pool), ctx
}

func TestStoreEnqueueJobPersistsQueuedJob(t *testing.T) {
	storeClient, ctx := migratedStore(t)

	job, err := storeClient.EnqueueJob(ctx, validStoreRequest("job_enqueue_1"))
	if err != nil {
		t.Fatalf("enqueue job: %v", err)
	}

	if job.ID != "job_enqueue_1" {
		t.Fatalf("expected job id, got %q", job.ID)
	}
	if job.State != domain.JobStateQueued {
		t.Fatalf("expected queued state, got %s", job.State)
	}
	if job.Attempt != 0 {
		t.Fatalf("expected attempt 0, got %d", job.Attempt)
	}
	if string(job.Payload) != `{"email": "user@example.com"}` && string(job.Payload) != `{"email":"user@example.com"}` {
		t.Fatalf("unexpected payload %s", string(job.Payload))
	}
}

func TestStoreGetJobReturnsPersistedJob(t *testing.T) {
	storeClient, ctx := migratedStore(t)

	created, err := storeClient.EnqueueJob(ctx, validStoreRequest("job_get_1"))
	if err != nil {
		t.Fatalf("enqueue job: %v", err)
	}

	got, err := storeClient.GetJob(ctx, created.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	if got.ID != created.ID {
		t.Fatalf("expected job %q, got %q", created.ID, got.ID)
	}
	if got.RetryPolicy.MaxAttempts != 3 {
		t.Fatalf("expected max attempts 3, got %d", got.RetryPolicy.MaxAttempts)
	}
}

func TestStoreGetJobReturnsNotFound(t *testing.T) {
	storeClient, ctx := migratedStore(t)

	_, err := storeClient.GetJob(ctx, "missing")
	if err != store.ErrJobNotFound {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestStoreEnqueueJobReturnsExistingJobForSameIdempotencyKey(t *testing.T) {
	storeClient, ctx := migratedStore(t)

	first, err := storeClient.EnqueueJob(ctx, validStoreRequest("job_idempotent_1"))
	if err != nil {
		t.Fatalf("first enqueue: %v", err)
	}

	secondReq := validStoreRequest("job_idempotent_2")
	secondReq.Payload = json.RawMessage(`{"email":"second@example.com"}`)

	second, err := storeClient.EnqueueJob(ctx, secondReq)
	if err != nil {
		t.Fatalf("second enqueue: %v", err)
	}

	if second.ID != first.ID {
		t.Fatalf("expected existing job id %q, got %q", first.ID, second.ID)
	}
	if string(second.Payload) != string(first.Payload) {
		t.Fatalf("expected existing payload to be preserved")
	}
}

func TestStoreEnqueueJobRecordsEvent(t *testing.T) {
	storeClient, ctx := migratedStore(t)

	job, err := storeClient.EnqueueJob(ctx, validStoreRequest("job_event_1"))
	if err != nil {
		t.Fatalf("enqueue job: %v", err)
	}

	events, err := storeClient.ListJobEvents(ctx, job.ID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].EventType != "job_enqueued" {
		t.Fatalf("expected job_enqueued event, got %q", events[0].EventType)
	}
}

func TestStoreEnqueueJobDoesNotRecordSecondEventForIdempotentDuplicate(t *testing.T) {
	storeClient, ctx := migratedStore(t)

	first, err := storeClient.EnqueueJob(ctx, validStoreRequest("job_event_idempotent_1"))
	if err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	secondReq := validStoreRequest("job_event_idempotent_2")
	if _, err := storeClient.EnqueueJob(ctx, secondReq); err != nil {
		t.Fatalf("second enqueue: %v", err)
	}

	events, err := storeClient.ListJobEvents(ctx, first.ID)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected duplicate idempotent enqueue to preserve one event, got %d", len(events))
	}
}
```

- [ ] **Step 2: Run test to verify it fails or skips correctly**

Run: `go test ./internal/store/postgres`

Expected without `BYTEMQ_TEST_DATABASE_URL`: PASS with tests skipped.

Run with a test database:

```bash
BYTEMQ_TEST_DATABASE_URL=postgres://localhost/bytemq_test go test ./internal/store/postgres
```

Expected with a reachable test database: FAIL because `Store`, `New`, `EnqueueJob`, `GetJob`, and `ListJobEvents` are undefined.

- [ ] **Step 3: Implement PostgreSQL store**

Create `internal/store/postgres/store.go`:

```go
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sharvesh/bytemq/internal/domain"
	"github.com/sharvesh/bytemq/internal/store"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) EnqueueJob(ctx context.Context, req store.EnqueueJobRequest) (store.JobRecord, error) {
	if err := req.Validate(); err != nil {
		return store.JobRecord{}, err
	}

	runAfter := req.RunAfter.UTC()
	if runAfter.IsZero() {
		runAfter = time.Now().UTC()
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return store.JobRecord{}, fmt.Errorf("begin enqueue job: %w", err)
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
		INSERT INTO jobs (
			id, queue, type, payload, state, attempt,
			max_attempts, backoff_type, initial_delay_ms, max_delay_ms,
			run_after, idempotency_key
		)
		VALUES ($1, $2, $3, $4, $5, 0, $6, $7, $8, $9, $10, NULLIF($11, ''))
		ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL
		DO UPDATE SET updated_at = jobs.updated_at
		RETURNING
			id, queue, type, payload, state, attempt,
			max_attempts, backoff_type, initial_delay_ms, max_delay_ms,
			run_after, COALESCE(idempotency_key, ''), COALESCE(last_error, ''),
			created_at, updated_at, (xmax = 0) AS inserted
	`,
		req.ID,
		req.Queue,
		req.Type,
		req.Payload,
		domain.JobStateQueued,
		req.RetryPolicy.MaxAttempts,
		req.RetryPolicy.Backoff,
		req.RetryPolicy.InitialDelay.Milliseconds(),
		req.RetryPolicy.MaxDelay.Milliseconds(),
		runAfter,
		req.IdempotencyKey,
	)

	job, inserted, err := scanJobWithInserted(row)
	if err != nil {
		return store.JobRecord{}, fmt.Errorf("insert job: %w", err)
	}

	if inserted {
		if _, err := tx.Exec(ctx, `
			INSERT INTO job_events (job_id, event_type, message)
			VALUES ($1, $2, $3)
		`, job.ID, "job_enqueued", "job accepted"); err != nil {
			return store.JobRecord{}, fmt.Errorf("insert job event: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return store.JobRecord{}, fmt.Errorf("commit enqueue job: %w", err)
	}
	return job, nil
}

func (s *Store) GetJob(ctx context.Context, id string) (store.JobRecord, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT
			id, queue, type, payload, state, attempt,
			max_attempts, backoff_type, initial_delay_ms, max_delay_ms,
			run_after, COALESCE(idempotency_key, ''), COALESCE(last_error, ''),
			created_at, updated_at
		FROM jobs
		WHERE id = $1
	`, id)

	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.JobRecord{}, store.ErrJobNotFound
	}
	if err != nil {
		return store.JobRecord{}, fmt.Errorf("get job: %w", err)
	}
	return job, nil
}

func (s *Store) ListJobEvents(ctx context.Context, jobID string) ([]store.JobEventRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, job_id, event_type, message, created_at
		FROM job_events
		WHERE job_id = $1
		ORDER BY created_at, id
	`, jobID)
	if err != nil {
		return nil, fmt.Errorf("list job events: %w", err)
	}
	defer rows.Close()

	var events []store.JobEventRecord
	for rows.Next() {
		var event store.JobEventRecord
		if err := rows.Scan(&event.ID, &event.JobID, &event.EventType, &event.Message, &event.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan job event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate job events: %w", err)
	}
	return events, nil
}

type jobScanner interface {
	Scan(dest ...any) error
}

func scanJob(row jobScanner) (store.JobRecord, error) {
	job, err := scanJobFields(row, nil)
	return job, err
}

func scanJobWithInserted(row jobScanner) (store.JobRecord, bool, error) {
	var inserted bool
	job, err := scanJobFields(row, &inserted)
	return job, inserted, err
}

func scanJobFields(row jobScanner, inserted *bool) (store.JobRecord, error) {
	var job store.JobRecord
	var payload []byte
	var maxAttempts int
	var backoff domain.BackoffType
	var initialDelayMS int64
	var maxDelayMS int64

	dest := []any{
		&job.ID,
		&job.Queue,
		&job.Type,
		&payload,
		&job.State,
		&job.Attempt,
		&maxAttempts,
		&backoff,
		&initialDelayMS,
		&maxDelayMS,
		&job.RunAfter,
		&job.IdempotencyKey,
		&job.LastError,
		&job.CreatedAt,
		&job.UpdatedAt,
	}
	if inserted != nil {
		dest = append(dest, inserted)
	}

	err := row.Scan(dest...)
	if err != nil {
		return store.JobRecord{}, err
	}

	job.Payload = json.RawMessage(payload)
	job.RetryPolicy = domain.RetryPolicy{
		MaxAttempts:  maxAttempts,
		Backoff:      backoff,
		InitialDelay: time.Duration(initialDelayMS) * time.Millisecond,
		MaxDelay:     time.Duration(maxDelayMS) * time.Millisecond,
	}

	return job, nil
}
```

- [ ] **Step 4: Run PostgreSQL store tests**

Run:

```bash
go test ./internal/store/postgres
```

Expected without `BYTEMQ_TEST_DATABASE_URL`: PASS with tests skipped.

Run with a test database when available:

```bash
BYTEMQ_TEST_DATABASE_URL=postgres://localhost/bytemq_test go test ./internal/store/postgres
```

Expected with a reachable test database: PASS.

## Task 4: Developer Documentation and Verification

**Files:**
- Modify: `dev-docs/local-development.md`

**Interfaces:**
- Documents: `BYTEMQ_TEST_DATABASE_URL`
- Documents: PostgreSQL integration test behavior
- Verifies: full Go test suite and vet

- [ ] **Step 1: Update local development docs**

Add this section to `dev-docs/local-development.md`:

```markdown
## PostgreSQL Integration Tests

Store integration tests use `BYTEMQ_TEST_DATABASE_URL`.

Example:

```bash
BYTEMQ_TEST_DATABASE_URL=postgres://localhost/bytemq_test go test ./internal/store/postgres
```

When the variable is not set, PostgreSQL integration tests skip with a clear
message. When it is set, tests create a temporary schema, set `search_path` to
that schema, run migrations, and drop the schema during cleanup.

Never point `BYTEMQ_TEST_DATABASE_URL` at a production or shared database.
```

- [ ] **Step 2: Run formatting**

Run:

```bash
gofmt -w internal
```

Expected: command exits 0.

- [ ] **Step 3: Run full tests**

Run:

```bash
go test -count=1 ./...
```

Expected without `BYTEMQ_TEST_DATABASE_URL`: all packages pass; PostgreSQL integration tests skip.

- [ ] **Step 4: Run vet**

Run:

```bash
go vet ./...
```

Expected: command exits 0.

- [ ] **Step 5: Run PostgreSQL integration tests when a test database is available**

Run:

```bash
BYTEMQ_TEST_DATABASE_URL=postgres://localhost/bytemq_test go test -count=1 ./internal/store/postgres
```

Expected with a reachable test database: PASS.

If no local PostgreSQL test database is available, record that this integration command could not be run and keep the skip behavior verified by `go test -count=1 ./...`.

## Self-Review Checklist

- Store contract does not depend on PostgreSQL packages.
- PostgreSQL code lives under `internal/store/postgres`.
- Domain code remains independent from database drivers.
- Migrations create `jobs`, `job_attempts`, and `job_events`.
- Enqueue persists queued jobs durably.
- Enqueue records a `job_enqueued` event for newly inserted jobs.
- Repeated enqueue with the same queue and idempotency key returns the existing job.
- Tests do not drop or truncate default-schema tables.
- PostgreSQL integration tests skip clearly when `BYTEMQ_TEST_DATABASE_URL` is absent.
- No leasing, heartbeat, worker runtime, API server, dashboard, Redis, NATS, Kubernetes, or workflow behavior is added in this slice.
- No exactly-once claim is introduced.
