package app

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sharvesh/bytemq/internal/domain"
	"github.com/sharvesh/bytemq/internal/store"
)

type fakeJobStore struct {
	enqueued store.EnqueueJobRequest
	job      store.JobRecord
}

func (f *fakeJobStore) EnqueueJob(ctx context.Context, req store.EnqueueJobRequest) (store.JobRecord, error) {
	f.enqueued = req
	f.job = store.JobRecord{
		ID:          req.ID,
		Queue:       req.Queue,
		Type:        req.Type,
		Payload:     req.Payload,
		State:       domain.JobStateQueued,
		RetryPolicy: req.RetryPolicy,
		RunAfter:    req.RunAfter,
	}
	return f.job, nil
}

func (f *fakeJobStore) GetJob(ctx context.Context, id string) (store.JobRecord, error) {
	return f.job, nil
}

func (f *fakeJobStore) ListJobEvents(ctx context.Context, jobID string) ([]store.JobEventRecord, error) {
	return nil, nil
}

func (f *fakeJobStore) LeaseNextJob(ctx context.Context, req store.LeaseJobRequest) (store.JobRecord, error) {
	return store.JobRecord{}, store.ErrNoJobAvailable
}

func (f *fakeJobStore) StartJob(ctx context.Context, req store.StartJobRequest) (store.JobRecord, error) {
	return store.JobRecord{}, nil
}

func (f *fakeJobStore) HeartbeatJob(ctx context.Context, req store.HeartbeatJobRequest) (store.JobRecord, error) {
	return store.JobRecord{}, nil
}

func (f *fakeJobStore) CompleteJob(ctx context.Context, req store.CompleteJobRequest) (store.JobRecord, error) {
	return store.JobRecord{}, nil
}

func (f *fakeJobStore) FailJob(ctx context.Context, req store.FailJobRequest) (store.JobRecord, error) {
	return store.JobRecord{}, nil
}

func (f *fakeJobStore) RecoverExpiredLeases(ctx context.Context, req store.RecoverExpiredLeasesRequest) (int, error) {
	return 0, nil
}

func TestServiceEnqueueJobUsesInjectedIDAndDefaults(t *testing.T) {
	jobStore := &fakeJobStore{}
	service := New(jobStore, func() string { return "job-fixed" })

	job, err := service.EnqueueJob(context.Background(), EnqueueRequest{
		Queue:   "default",
		Type:    "send_email",
		Payload: json.RawMessage(`{"email":"user@example.com"}`),
	})
	if err != nil {
		t.Fatalf("enqueue job: %v", err)
	}

	if job.ID != "job-fixed" {
		t.Fatalf("expected injected id, got %q", job.ID)
	}
	if jobStore.enqueued.RetryPolicy.MaxAttempts != 3 {
		t.Fatalf("expected default max attempts 3, got %d", jobStore.enqueued.RetryPolicy.MaxAttempts)
	}
	if jobStore.enqueued.RetryPolicy.Backoff != domain.BackoffExponential {
		t.Fatalf("expected exponential default backoff, got %s", jobStore.enqueued.RetryPolicy.Backoff)
	}
}

func TestServiceEnqueueJobPassesRequestFields(t *testing.T) {
	jobStore := &fakeJobStore{}
	service := New(jobStore, func() string { return "job-fixed" })
	runAfter := time.Date(2026, 8, 8, 10, 0, 0, 0, time.UTC)
	retry := domain.RetryPolicy{
		MaxAttempts:  5,
		Backoff:      domain.BackoffFixed,
		InitialDelay: 2 * time.Second,
		MaxDelay:     10 * time.Second,
	}

	_, err := service.EnqueueJob(context.Background(), EnqueueRequest{
		Queue:          "critical",
		Type:           "render",
		Payload:        json.RawMessage(`{"id":1}`),
		RetryPolicy:    retry,
		RunAfter:       runAfter,
		IdempotencyKey: "render:1",
	})
	if err != nil {
		t.Fatalf("enqueue job: %v", err)
	}

	if jobStore.enqueued.Queue != "critical" {
		t.Fatalf("expected queue critical, got %q", jobStore.enqueued.Queue)
	}
	if jobStore.enqueued.Type != "render" {
		t.Fatalf("expected type render, got %q", jobStore.enqueued.Type)
	}
	if string(jobStore.enqueued.Payload) != `{"id":1}` {
		t.Fatalf("unexpected payload %s", jobStore.enqueued.Payload)
	}
	if jobStore.enqueued.IdempotencyKey != "render:1" {
		t.Fatalf("expected idempotency key render:1, got %q", jobStore.enqueued.IdempotencyKey)
	}
	if !jobStore.enqueued.RunAfter.Equal(runAfter) {
		t.Fatalf("expected run_after %s, got %s", runAfter, jobStore.enqueued.RunAfter)
	}
	if jobStore.enqueued.RetryPolicy.MaxAttempts != retry.MaxAttempts {
		t.Fatalf("expected custom retry policy")
	}
}

func TestServiceGetJobDelegatesToStore(t *testing.T) {
	jobStore := &fakeJobStore{job: store.JobRecord{ID: "job-1", Queue: "default"}}
	service := New(jobStore, func() string { return "unused" })

	job, err := service.GetJob(context.Background(), "job-1")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.ID != "job-1" {
		t.Fatalf("expected job-1, got %q", job.ID)
	}
}
