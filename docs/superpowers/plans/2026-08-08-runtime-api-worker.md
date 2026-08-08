# Runtime API Worker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first usable Phase 1 runtime layer on top of the reliable store: app enqueue service, HTTP API handlers, worker single-job execution, heartbeat loop, and scheduler recovery wrapper.

**Architecture:** Keep business use cases in `internal/app`, HTTP mapping in `internal/api`, worker execution in `internal/worker`, and expired-lease recovery in `internal/scheduler`. All packages depend on the `internal/store.JobStore` contract, not PostgreSQL tables.

**Tech Stack:** Go 1.26+, standard library `net/http`, `httptest`, `encoding/json`, existing ByteMQ domain and store packages.

## Global Constraints

- Follow `rules.md` before changing code.
- ByteMQ remains a modular monolith with clean service boundaries.
- Store code owns database mutation; API, worker, scheduler, and CLI use store interfaces.
- ByteMQ provides at-least-once execution and must not claim exactly-once delivery.
- Worker execution must complete or fail through active lease ownership.
- Heartbeat must run while a handler is active.
- Do not add dashboard, Redis, NATS, Kubernetes, workflows, or multi-region behavior.
- Use TDD: write each behavior test, watch it fail, then implement minimal code.
- Use only ASCII text in source files.

---

## Task 1: App Service

**Files:**
- Create: `internal/app/service.go`
- Test: `internal/app/service_test.go`

**Interfaces:**
- Produces: `type Service struct`
- Produces: `func New(jobStore store.JobStore, newID func() string) *Service`
- Produces: `type EnqueueRequest struct`
- Produces: `func (s *Service) EnqueueJob(ctx context.Context, req EnqueueRequest) (store.JobRecord, error)`
- Produces: `func (s *Service) GetJob(ctx context.Context, id string) (store.JobRecord, error)`
- Produces: `func DefaultRetryPolicy() domain.RetryPolicy`

**Required tests:**
- Enqueue uses injected ID generator.
- Enqueue applies default retry policy when none is supplied.
- Enqueue passes queue, type, payload, run_after, and idempotency key to the store.
- GetJob delegates to store.

## Task 2: HTTP API

**Files:**
- Create: `internal/api/server.go`
- Test: `internal/api/server_test.go`

**Interfaces:**
- Produces: `func NewHandler(service *app.Service) http.Handler`

**Required tests:**
- `POST /v1/jobs` accepts JSON, enqueues a job, and returns `201`.
- `POST /v1/jobs` rejects invalid JSON with `400`.
- `GET /v1/jobs/{id}` returns job JSON.
- `GET /v1/jobs/{id}` returns `404` for `store.ErrJobNotFound`.

## Task 3: Worker Runner

**Files:**
- Create: `internal/worker/runner.go`
- Test: `internal/worker/runner_test.go`

**Interfaces:**
- Produces: `type Handler func(context.Context, json.RawMessage) error`
- Produces: `type Runner struct`
- Produces: `type Config struct`
- Produces: `func New(jobStore store.JobStore, cfg Config, handlers map[string]Handler) *Runner`
- Produces: `func (r *Runner) ProcessOne(ctx context.Context) (bool, error)`

**Required tests:**
- No available job returns `(false, nil)`.
- Successful handler leases, starts, executes, and completes a job.
- Failing handler leases, starts, executes, and fails a job.
- Missing handler fails the job.
- Heartbeat is called while a handler is active.

## Task 4: Scheduler Recovery

**Files:**
- Create: `internal/scheduler/recovery.go`
- Test: `internal/scheduler/recovery_test.go`

**Interfaces:**
- Produces: `type Recovery struct`
- Produces: `func NewRecovery(jobStore store.JobStore, limit int) *Recovery`
- Produces: `func (r *Recovery) RecoverOnce(ctx context.Context) (int, error)`

**Required tests:**
- RecoverOnce calls `RecoverExpiredLeases` with configured limit.
- Invalid limit defaults to `100`.

## Task 5: Runtime Docs and Verification

**Files:**
- Modify: `architecture.md`
- Modify: `dev-docs/testing-strategy.md`

**Required verification:**
- `gofmt -w internal`
- `go test -count=1 ./...`
- `go vet ./...`
- Docker-backed `go test -count=1 ./internal/store/postgres`

## Self-Review Checklist

- API, worker, scheduler, and app packages do not import PostgreSQL.
- Worker uses store lease/start/heartbeat/complete/fail operations.
- Heartbeat loop stops after handler completion.
- App service applies a production-shaped default retry policy.
- HTTP API does not expose internal database errors.
- Tests cover success and failure paths.
- No exactly-once claim is introduced.

