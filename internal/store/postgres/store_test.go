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
