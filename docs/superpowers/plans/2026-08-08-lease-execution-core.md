# Lease Execution Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete ByteMQ's reliable execution protocol: leasing, start, heartbeat, completion, failure, retry scheduling, dead-lettering, and expired-lease recovery.

**Architecture:** Extend the storage contract with ownership-sensitive job operations, then implement them in PostgreSQL with short transactions and row-level locks. Worker and API code will depend on these methods later; this plan does not add HTTP handlers or a worker loop.

**Tech Stack:** Go 1.26+, PostgreSQL, `github.com/jackc/pgx/v5`, existing ByteMQ domain/store packages.

## Global Constraints

- Follow `rules.md` before changing code.
- ByteMQ is learning-first and production-shaped.
- PostgreSQL is the v1 durable source of truth.
- ByteMQ provides at-least-once execution and must not claim exactly-once delivery.
- Store code must own durable PostgreSQL persistence and transaction boundaries.
- Use row-level locking deliberately for leasing and recovery.
- Use database time for lease expiry decisions.
- State transitions must be explicit and validated.
- Only the active lease owner may start, heartbeat, complete, or fail a job.
- Do not add API server, worker runtime, dashboard, Redis, NATS, Kubernetes, workflows, or multi-region behavior in this slice.
- Use TDD: write each behavior test, watch it fail, then implement the minimal code.
- Use only ASCII text in source files.

---

## File Structure

Create and modify this structure:

```text
internal/store/store.go
internal/store/store_test.go
internal/store/postgres/store.go
internal/store/postgres/store_test.go
dev-docs/testing-strategy.md
```

Responsibilities:

- `internal/store/store.go`: add lease/start/heartbeat/complete/fail/recovery request types and errors.
- `internal/store/store_test.go`: pure request validation tests.
- `internal/store/postgres/store.go`: PostgreSQL implementation of ownership-sensitive execution operations.
- `internal/store/postgres/store_test.go`: integration tests for leasing and recovery behavior.
- `dev-docs/testing-strategy.md`: document execution-core integration coverage.

## Task 1: Store Execution Contract

**Files:**
- Modify: `internal/store/store.go`
- Modify: `internal/store/store_test.go`

**Interfaces:**
- Produces: `var ErrInvalidLease error`
- Produces: `var ErrNoJobAvailable error`
- Produces: `var ErrLeaseNotOwned error`
- Produces: `type LeaseJobRequest struct { WorkerID string; LeaseID string; LeaseDuration time.Duration }`
- Produces: `func (r LeaseJobRequest) Validate() error`
- Produces: `type StartJobRequest struct { JobID string; WorkerID string; LeaseID string }`
- Produces: `func (r StartJobRequest) Validate() error`
- Produces: `type HeartbeatJobRequest struct { JobID string; WorkerID string; LeaseID string; LeaseDuration time.Duration }`
- Produces: `func (r HeartbeatJobRequest) Validate() error`
- Produces: `type CompleteJobRequest struct { JobID string; WorkerID string; LeaseID string }`
- Produces: `func (r CompleteJobRequest) Validate() error`
- Produces: `type FailJobRequest struct { JobID string; WorkerID string; LeaseID string; Error string }`
- Produces: `func (r FailJobRequest) Validate() error`
- Produces: `type RecoverExpiredLeasesRequest struct { Limit int }`
- Produces: `func (r RecoverExpiredLeasesRequest) Validate() error`
- Extends: `JobRecord` with `LeasedBy string`, `LeaseID string`, `LeaseExpiresAt time.Time`
- Extends: `JobStore` with `LeaseNextJob`, `StartJob`, `HeartbeatJob`, `CompleteJob`, `FailJob`, and `RecoverExpiredLeases`

- [ ] **Step 1: Write failing request validation tests**

Append to `internal/store/store_test.go`:

```go
func TestLeaseJobRequestValidate(t *testing.T) {
	valid := LeaseJobRequest{WorkerID: "worker-1", LeaseID: "lease-1", LeaseDuration: 30 * time.Second}
	if err := valid.Validate(); err != nil {
		t.Fatalf("expected valid lease request, got %v", err)
	}

	cases := []LeaseJobRequest{
		{WorkerID: "", LeaseID: "lease-1", LeaseDuration: 30 * time.Second},
		{WorkerID: "worker-1", LeaseID: "", LeaseDuration: 30 * time.Second},
		{WorkerID: "worker-1", LeaseID: "lease-1", LeaseDuration: 0},
	}
	for _, req := range cases {
		if err := req.Validate(); err != ErrInvalidLease {
			t.Fatalf("expected ErrInvalidLease for %+v, got %v", req, err)
		}
	}
}

func TestOwnershipRequestValidate(t *testing.T) {
	validStart := StartJobRequest{JobID: "job-1", WorkerID: "worker-1", LeaseID: "lease-1"}
	if err := validStart.Validate(); err != nil {
		t.Fatalf("expected valid start request, got %v", err)
	}
	validComplete := CompleteJobRequest{JobID: "job-1", WorkerID: "worker-1", LeaseID: "lease-1"}
	if err := validComplete.Validate(); err != nil {
		t.Fatalf("expected valid complete request, got %v", err)
	}
	validFail := FailJobRequest{JobID: "job-1", WorkerID: "worker-1", LeaseID: "lease-1", Error: "boom"}
	if err := validFail.Validate(); err != nil {
		t.Fatalf("expected valid fail request, got %v", err)
	}

	invalidStart := StartJobRequest{JobID: "", WorkerID: "worker-1", LeaseID: "lease-1"}
	if err := invalidStart.Validate(); err != ErrInvalidLease {
		t.Fatalf("expected ErrInvalidLease for invalid start, got %v", err)
	}
	invalidComplete := CompleteJobRequest{JobID: "job-1", WorkerID: "", LeaseID: "lease-1"}
	if err := invalidComplete.Validate(); err != ErrInvalidLease {
		t.Fatalf("expected ErrInvalidLease for invalid complete, got %v", err)
	}
	invalidFail := FailJobRequest{JobID: "job-1", WorkerID: "worker-1", LeaseID: ""}
	if err := invalidFail.Validate(); err != ErrInvalidLease {
		t.Fatalf("expected ErrInvalidLease for invalid fail, got %v", err)
	}
}

func TestHeartbeatJobRequestValidate(t *testing.T) {
	valid := HeartbeatJobRequest{JobID: "job-1", WorkerID: "worker-1", LeaseID: "lease-1", LeaseDuration: 30 * time.Second}
	if err := valid.Validate(); err != nil {
		t.Fatalf("expected valid heartbeat, got %v", err)
	}

	invalid := HeartbeatJobRequest{JobID: "job-1", WorkerID: "worker-1", LeaseID: "lease-1", LeaseDuration: 0}
	if err := invalid.Validate(); err != ErrInvalidLease {
		t.Fatalf("expected ErrInvalidLease, got %v", err)
	}
}

func TestRecoverExpiredLeasesRequestValidate(t *testing.T) {
	if err := (RecoverExpiredLeasesRequest{Limit: 10}).Validate(); err != nil {
		t.Fatalf("expected valid recovery request, got %v", err)
	}
	if err := (RecoverExpiredLeasesRequest{Limit: 0}).Validate(); err != ErrInvalidLease {
		t.Fatalf("expected ErrInvalidLease, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/store`

Expected: FAIL because lease request types and errors are undefined.

- [ ] **Step 3: Implement request types and validation**

Modify `internal/store/store.go`:

```go
var ErrInvalidLease = errors.New("invalid lease")

var ErrNoJobAvailable = errors.New("no job available")

var ErrLeaseNotOwned = errors.New("lease not owned")
```

Add these fields to `JobRecord`:

```go
LeasedBy       string
LeaseID        string
LeaseExpiresAt time.Time
```

Add request types and validation helpers:

```go
type LeaseJobRequest struct {
	WorkerID      string
	LeaseID       string
	LeaseDuration time.Duration
}

func (r LeaseJobRequest) Validate() error {
	if strings.TrimSpace(r.WorkerID) == "" || strings.TrimSpace(r.LeaseID) == "" || r.LeaseDuration <= 0 {
		return ErrInvalidLease
	}
	return nil
}

type StartJobRequest struct {
	JobID    string
	WorkerID string
	LeaseID  string
}

func (r StartJobRequest) Validate() error {
	return validateOwnedLease(r.JobID, r.WorkerID, r.LeaseID)
}

type HeartbeatJobRequest struct {
	JobID         string
	WorkerID      string
	LeaseID       string
	LeaseDuration time.Duration
}

func (r HeartbeatJobRequest) Validate() error {
	if err := validateOwnedLease(r.JobID, r.WorkerID, r.LeaseID); err != nil {
		return err
	}
	if r.LeaseDuration <= 0 {
		return ErrInvalidLease
	}
	return nil
}

type CompleteJobRequest struct {
	JobID    string
	WorkerID string
	LeaseID  string
}

func (r CompleteJobRequest) Validate() error {
	return validateOwnedLease(r.JobID, r.WorkerID, r.LeaseID)
}

type FailJobRequest struct {
	JobID    string
	WorkerID string
	LeaseID  string
	Error    string
}

func (r FailJobRequest) Validate() error {
	return validateOwnedLease(r.JobID, r.WorkerID, r.LeaseID)
}

type RecoverExpiredLeasesRequest struct {
	Limit int
}

func (r RecoverExpiredLeasesRequest) Validate() error {
	if r.Limit < 1 {
		return ErrInvalidLease
	}
	return nil
}

func validateOwnedLease(jobID string, workerID string, leaseID string) error {
	if strings.TrimSpace(jobID) == "" || strings.TrimSpace(workerID) == "" || strings.TrimSpace(leaseID) == "" {
		return ErrInvalidLease
	}
	return nil
}
```

Extend `JobStore`:

```go
LeaseNextJob(ctx context.Context, req LeaseJobRequest) (JobRecord, error)
StartJob(ctx context.Context, req StartJobRequest) (JobRecord, error)
HeartbeatJob(ctx context.Context, req HeartbeatJobRequest) (JobRecord, error)
CompleteJob(ctx context.Context, req CompleteJobRequest) (JobRecord, error)
FailJob(ctx context.Context, req FailJobRequest) (JobRecord, error)
RecoverExpiredLeases(ctx context.Context, req RecoverExpiredLeasesRequest) (int, error)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/store`

Expected: PASS.

## Task 2: Lease, Start, Heartbeat, Complete

**Files:**
- Modify: `internal/store/postgres/store.go`
- Modify: `internal/store/postgres/store_test.go`

**Interfaces:**
- Implements: `LeaseNextJob`
- Implements: `StartJob`
- Implements: `HeartbeatJob`
- Implements: `CompleteJob`

- [ ] **Step 1: Write failing integration tests**

Append to `internal/store/postgres/store_test.go`:

```go
func TestStoreLeaseNextJobAssignsActiveLease(t *testing.T) {
	storeClient, ctx := migratedStore(t)
	if _, err := storeClient.EnqueueJob(ctx, validStoreRequest("job_lease_1")); err != nil {
		t.Fatalf("enqueue job: %v", err)
	}

	job, err := storeClient.LeaseNextJob(ctx, store.LeaseJobRequest{
		WorkerID:      "worker-1",
		LeaseID:       "lease-1",
		LeaseDuration: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("lease job: %v", err)
	}

	if job.State != domain.JobStateLeased {
		t.Fatalf("expected leased state, got %s", job.State)
	}
	if job.LeasedBy != "worker-1" || job.LeaseID != "lease-1" {
		t.Fatalf("unexpected lease owner: worker=%q lease=%q", job.LeasedBy, job.LeaseID)
	}
	if job.LeaseExpiresAt.IsZero() {
		t.Fatal("expected lease expiry")
	}
}

func TestStoreLeaseNextJobReturnsNoJobAvailable(t *testing.T) {
	storeClient, ctx := migratedStore(t)

	_, err := storeClient.LeaseNextJob(ctx, store.LeaseJobRequest{
		WorkerID:      "worker-1",
		LeaseID:       "lease-1",
		LeaseDuration: 30 * time.Second,
	})
	if err != store.ErrNoJobAvailable {
		t.Fatalf("expected ErrNoJobAvailable, got %v", err)
	}
}

func TestStoreStartJobRequiresActiveLeaseOwner(t *testing.T) {
	storeClient, ctx := migratedStore(t)
	if _, err := storeClient.EnqueueJob(ctx, validStoreRequest("job_start_1")); err != nil {
		t.Fatalf("enqueue job: %v", err)
	}
	leased, err := storeClient.LeaseNextJob(ctx, store.LeaseJobRequest{WorkerID: "worker-1", LeaseID: "lease-1", LeaseDuration: 30 * time.Second})
	if err != nil {
		t.Fatalf("lease job: %v", err)
	}

	_, err = storeClient.StartJob(ctx, store.StartJobRequest{JobID: leased.ID, WorkerID: "worker-2", LeaseID: "lease-1"})
	if err != store.ErrLeaseNotOwned {
		t.Fatalf("expected ErrLeaseNotOwned for wrong worker, got %v", err)
	}

	started, err := storeClient.StartJob(ctx, store.StartJobRequest{JobID: leased.ID, WorkerID: "worker-1", LeaseID: "lease-1"})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}
	if started.State != domain.JobStateRunning {
		t.Fatalf("expected running state, got %s", started.State)
	}
	if started.Attempt != 1 {
		t.Fatalf("expected attempt 1, got %d", started.Attempt)
	}
}

func TestStoreHeartbeatExtendsActiveLease(t *testing.T) {
	storeClient, ctx := migratedStore(t)
	if _, err := storeClient.EnqueueJob(ctx, validStoreRequest("job_heartbeat_1")); err != nil {
		t.Fatalf("enqueue job: %v", err)
	}
	leased, err := storeClient.LeaseNextJob(ctx, store.LeaseJobRequest{WorkerID: "worker-1", LeaseID: "lease-1", LeaseDuration: time.Second})
	if err != nil {
		t.Fatalf("lease job: %v", err)
	}

	heartbeat, err := storeClient.HeartbeatJob(ctx, store.HeartbeatJobRequest{JobID: leased.ID, WorkerID: "worker-1", LeaseID: "lease-1", LeaseDuration: 30 * time.Second})
	if err != nil {
		t.Fatalf("heartbeat job: %v", err)
	}
	if !heartbeat.LeaseExpiresAt.After(leased.LeaseExpiresAt) {
		t.Fatalf("expected heartbeat to extend lease from %s to after it, got %s", leased.LeaseExpiresAt, heartbeat.LeaseExpiresAt)
	}
}

func TestStoreCompleteJobClearsLeaseAndMarksCompleted(t *testing.T) {
	storeClient, ctx := migratedStore(t)
	if _, err := storeClient.EnqueueJob(ctx, validStoreRequest("job_complete_1")); err != nil {
		t.Fatalf("enqueue job: %v", err)
	}
	leased, err := storeClient.LeaseNextJob(ctx, store.LeaseJobRequest{WorkerID: "worker-1", LeaseID: "lease-1", LeaseDuration: 30 * time.Second})
	if err != nil {
		t.Fatalf("lease job: %v", err)
	}
	if _, err := storeClient.StartJob(ctx, store.StartJobRequest{JobID: leased.ID, WorkerID: "worker-1", LeaseID: "lease-1"}); err != nil {
		t.Fatalf("start job: %v", err)
	}

	completed, err := storeClient.CompleteJob(ctx, store.CompleteJobRequest{JobID: leased.ID, WorkerID: "worker-1", LeaseID: "lease-1"})
	if err != nil {
		t.Fatalf("complete job: %v", err)
	}
	if completed.State != domain.JobStateCompleted {
		t.Fatalf("expected completed state, got %s", completed.State)
	}
	if completed.LeasedBy != "" || completed.LeaseID != "" || !completed.LeaseExpiresAt.IsZero() {
		t.Fatalf("expected completed job lease fields to be cleared")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/store/postgres`

Expected: FAIL because methods are undefined, or PASS with skip if no `BYTEMQ_TEST_DATABASE_URL`; run with Docker-backed PostgreSQL for the red failure.

- [ ] **Step 3: Implement leasing, start, heartbeat, complete**

In `internal/store/postgres/store.go`, update all `RETURNING` and `SELECT` field lists to include:

```sql
COALESCE(leased_by, ''), COALESCE(lease_id, ''), lease_expires_at,
```

Update `scanJobFields` to scan nullable `lease_expires_at` into `*time.Time` and assign `JobRecord.LeaseExpiresAt` only when present.

Add methods:

```go
func (s *Store) LeaseNextJob(ctx context.Context, req store.LeaseJobRequest) (store.JobRecord, error)
func (s *Store) StartJob(ctx context.Context, req store.StartJobRequest) (store.JobRecord, error)
func (s *Store) HeartbeatJob(ctx context.Context, req store.HeartbeatJobRequest) (store.JobRecord, error)
func (s *Store) CompleteJob(ctx context.Context, req store.CompleteJobRequest) (store.JobRecord, error)
```

Implementation requirements:

- `LeaseNextJob` uses a CTE with `FOR UPDATE SKIP LOCKED`.
- `LeaseNextJob` only selects `queued` jobs whose `run_after <= now()`.
- `LeaseNextJob` sets `state='leased'`, `leased_by`, `lease_id`, `lease_expires_at=now()+duration`, and records `job_leased`.
- `StartJob` verifies active owner and non-expired lease, sets `state='running'`, increments `attempt`, inserts `job_attempts`, and records `job_started`.
- `HeartbeatJob` verifies active owner and non-expired lease, extends `lease_expires_at`, and records `job_heartbeat`.
- `CompleteJob` verifies active owner and non-expired lease, sets `state='completed'`, clears lease fields, updates latest attempt `finished_at`, and records `job_completed`.
- Ownership failures return `store.ErrLeaseNotOwned`.

- [ ] **Step 4: Run tests**

Run:

```bash
go test -count=1 ./internal/store/postgres
```

Run with Docker-backed PostgreSQL:

```bash
BYTEMQ_TEST_DATABASE_URL=postgres://postgres:bytemq@localhost:55432/bytemq_test?sslmode=disable go test -count=1 ./internal/store/postgres
```

Expected with PostgreSQL: PASS.

## Task 3: Fail, Retry, Dead-Letter, Recovery

**Files:**
- Modify: `internal/store/postgres/store.go`
- Modify: `internal/store/postgres/store_test.go`

**Interfaces:**
- Implements: `FailJob`
- Implements: `RecoverExpiredLeases`

- [ ] **Step 1: Write failing integration tests**

Append to `internal/store/postgres/store_test.go`:

```go
func TestStoreFailJobSchedulesRetryWhenAttemptsRemain(t *testing.T) {
	storeClient, ctx := migratedStore(t)
	req := validStoreRequest("job_fail_retry_1")
	req.RunAfter = time.Now().UTC()
	if _, err := storeClient.EnqueueJob(ctx, req); err != nil {
		t.Fatalf("enqueue job: %v", err)
	}
	leased, err := storeClient.LeaseNextJob(ctx, store.LeaseJobRequest{WorkerID: "worker-1", LeaseID: "lease-1", LeaseDuration: 30 * time.Second})
	if err != nil {
		t.Fatalf("lease job: %v", err)
	}
	if _, err := storeClient.StartJob(ctx, store.StartJobRequest{JobID: leased.ID, WorkerID: "worker-1", LeaseID: "lease-1"}); err != nil {
		t.Fatalf("start job: %v", err)
	}

	failed, err := storeClient.FailJob(ctx, store.FailJobRequest{JobID: leased.ID, WorkerID: "worker-1", LeaseID: "lease-1", Error: "boom"})
	if err != nil {
		t.Fatalf("fail job: %v", err)
	}

	if failed.State != domain.JobStateRetryScheduled {
		t.Fatalf("expected retry_scheduled state, got %s", failed.State)
	}
	if failed.LastError != "boom" {
		t.Fatalf("expected last error boom, got %q", failed.LastError)
	}
	if !failed.RunAfter.After(req.RunAfter) {
		t.Fatalf("expected retry run_after after original run_after")
	}
	if failed.LeasedBy != "" || failed.LeaseID != "" || !failed.LeaseExpiresAt.IsZero() {
		t.Fatalf("expected failed retry job lease fields to be cleared")
	}
}

func TestStoreFailJobDeadLettersWhenAttemptsExhausted(t *testing.T) {
	storeClient, ctx := migratedStore(t)
	req := validStoreRequest("job_fail_dead_1")
	req.RetryPolicy.MaxAttempts = 1
	req.RunAfter = time.Now().UTC()
	if _, err := storeClient.EnqueueJob(ctx, req); err != nil {
		t.Fatalf("enqueue job: %v", err)
	}
	leased, err := storeClient.LeaseNextJob(ctx, store.LeaseJobRequest{WorkerID: "worker-1", LeaseID: "lease-1", LeaseDuration: 30 * time.Second})
	if err != nil {
		t.Fatalf("lease job: %v", err)
	}
	if _, err := storeClient.StartJob(ctx, store.StartJobRequest{JobID: leased.ID, WorkerID: "worker-1", LeaseID: "lease-1"}); err != nil {
		t.Fatalf("start job: %v", err)
	}

	failed, err := storeClient.FailJob(ctx, store.FailJobRequest{JobID: leased.ID, WorkerID: "worker-1", LeaseID: "lease-1", Error: "boom"})
	if err != nil {
		t.Fatalf("fail job: %v", err)
	}

	if failed.State != domain.JobStateDeadLettered {
		t.Fatalf("expected dead_lettered state, got %s", failed.State)
	}
	if failed.LastError != "boom" {
		t.Fatalf("expected last error boom, got %q", failed.LastError)
	}
}

func TestStoreRecoverExpiredLeasesRequeuesUnstartedLeasedJob(t *testing.T) {
	storeClient, ctx := migratedStore(t)
	if _, err := storeClient.EnqueueJob(ctx, validStoreRequest("job_recover_leased_1")); err != nil {
		t.Fatalf("enqueue job: %v", err)
	}
	leased, err := storeClient.LeaseNextJob(ctx, store.LeaseJobRequest{WorkerID: "worker-1", LeaseID: "lease-1", LeaseDuration: 30 * time.Second})
	if err != nil {
		t.Fatalf("lease job: %v", err)
	}
	if _, err := storeClient.pool.Exec(ctx, `UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, leased.ID); err != nil {
		t.Fatalf("force lease expiry: %v", err)
	}

	recovered, err := storeClient.RecoverExpiredLeases(ctx, store.RecoverExpiredLeasesRequest{Limit: 10})
	if err != nil {
		t.Fatalf("recover expired leases: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("expected 1 recovered job, got %d", recovered)
	}
	got, err := storeClient.GetJob(ctx, leased.ID)
	if err != nil {
		t.Fatalf("get recovered job: %v", err)
	}
	if got.State != domain.JobStateQueued {
		t.Fatalf("expected queued state, got %s", got.State)
	}
	if got.LeasedBy != "" || got.LeaseID != "" || !got.LeaseExpiresAt.IsZero() {
		t.Fatalf("expected recovered job lease fields to be cleared")
	}
}

func TestStoreRecoverExpiredLeasesSchedulesRetryForExpiredRunningJob(t *testing.T) {
	storeClient, ctx := migratedStore(t)
	req := validStoreRequest("job_recover_running_1")
	req.RunAfter = time.Now().UTC()
	if _, err := storeClient.EnqueueJob(ctx, req); err != nil {
		t.Fatalf("enqueue job: %v", err)
	}
	leased, err := storeClient.LeaseNextJob(ctx, store.LeaseJobRequest{WorkerID: "worker-1", LeaseID: "lease-1", LeaseDuration: 30 * time.Second})
	if err != nil {
		t.Fatalf("lease job: %v", err)
	}
	if _, err := storeClient.StartJob(ctx, store.StartJobRequest{JobID: leased.ID, WorkerID: "worker-1", LeaseID: "lease-1"}); err != nil {
		t.Fatalf("start job: %v", err)
	}
	if _, err := storeClient.pool.Exec(ctx, `UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, leased.ID); err != nil {
		t.Fatalf("force lease expiry: %v", err)
	}

	recovered, err := storeClient.RecoverExpiredLeases(ctx, store.RecoverExpiredLeasesRequest{Limit: 10})
	if err != nil {
		t.Fatalf("recover expired leases: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("expected 1 recovered job, got %d", recovered)
	}
	got, err := storeClient.GetJob(ctx, leased.ID)
	if err != nil {
		t.Fatalf("get recovered job: %v", err)
	}
	if got.State != domain.JobStateRetryScheduled {
		t.Fatalf("expected retry_scheduled state, got %s", got.State)
	}
	if got.LeasedBy != "" || got.LeaseID != "" || !got.LeaseExpiresAt.IsZero() {
		t.Fatalf("expected recovered running job lease fields to be cleared")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run with Docker-backed PostgreSQL.

Expected: FAIL because `FailJob` and `RecoverExpiredLeases` are undefined or incomplete.

- [ ] **Step 3: Implement failure and recovery**

Implementation requirements:

- `FailJob` verifies active owner and non-expired lease.
- `FailJob` updates the latest attempt with `finished_at` and error.
- `FailJob` uses `RetryPolicy.ShouldRetry(job.Attempt)`.
- If retrying, set `state='retry_scheduled'`, `run_after=db_now+DelayForAttempt(job.Attempt)`, clear lease fields, set `last_error`, record `job_failed` and `job_retry_scheduled`.
- If exhausted, set `state='dead_lettered'`, clear lease fields, set `last_error`, record `job_failed` and `job_dead_lettered`.
- `RecoverExpiredLeases` selects expired `leased` or `running` jobs with `FOR UPDATE SKIP LOCKED`, using database time from `SELECT now()`.
- Expired `leased` jobs return to `queued` without incrementing attempt.
- Expired `running` jobs retry or dead-letter using the same attempt policy as `FailJob`.
- Every recovered job records `lease_expired`.

- [ ] **Step 4: Run tests**

Run with Docker-backed PostgreSQL.

Expected: PASS.

## Task 4: Documentation and Verification

**Files:**
- Modify: `dev-docs/testing-strategy.md`

**Interfaces:**
- Documents: lease/start/heartbeat/complete/fail/recovery integration coverage
- Verifies: full Go suite, vet, Docker-backed PostgreSQL integration suite

- [ ] **Step 1: Update testing docs**

Add this section to `dev-docs/testing-strategy.md`:

```markdown
## Execution-Core Integration Tests

The PostgreSQL execution-core tests cover the job ownership protocol:

- Leasing only due queued jobs.
- Returning `ErrNoJobAvailable` when no job can be leased.
- Rejecting start, heartbeat, complete, and fail operations from the wrong worker or lease.
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
```

- [ ] **Step 2: Run formatting**

Run: `gofmt -w internal`

Expected: command exits 0.

- [ ] **Step 3: Run full tests and vet**

Run:

```bash
go test -count=1 ./...
go vet ./...
```

Expected: PASS.

- [ ] **Step 4: Run Docker-backed PostgreSQL tests**

Run a disposable PostgreSQL container and execute:

```bash
BYTEMQ_TEST_DATABASE_URL=postgres://postgres:bytemq@localhost:55432/bytemq_test?sslmode=disable go test -count=1 ./internal/store/postgres
```

Expected: PASS.

## Self-Review Checklist

- Store contract remains database-driver independent.
- PostgreSQL implementation owns transactions and row locking.
- Lease operations use database time for expiry checks.
- Wrong worker or wrong lease cannot start, heartbeat, complete, or fail a job.
- Completion clears lease fields.
- Failure preserves error details.
- Retry and dead-letter behavior uses existing `RetryPolicy`.
- Expired leases recover without leaving jobs permanently stuck.
- Tests cover success and ownership failure paths.
- No API server, worker loop, dashboard, Redis, NATS, Kubernetes, workflow, or exactly-once claim is introduced.
