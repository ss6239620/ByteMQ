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
