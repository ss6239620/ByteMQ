package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sharvesh/bytemq/internal/app"
	"github.com/sharvesh/bytemq/internal/domain"
	"github.com/sharvesh/bytemq/internal/store"
)

type apiFakeStore struct {
	job store.JobRecord
	err error
}

func (f *apiFakeStore) EnqueueJob(ctx context.Context, req store.EnqueueJobRequest) (store.JobRecord, error) {
	f.job = store.JobRecord{
		ID:             req.ID,
		Queue:          req.Queue,
		Type:           req.Type,
		Payload:        req.Payload,
		State:          domain.JobStateQueued,
		IdempotencyKey: req.IdempotencyKey,
	}
	return f.job, f.err
}

func (f *apiFakeStore) GetJob(ctx context.Context, id string) (store.JobRecord, error) {
	if f.err != nil {
		return store.JobRecord{}, f.err
	}
	return f.job, nil
}

func (f *apiFakeStore) ListJobEvents(ctx context.Context, jobID string) ([]store.JobEventRecord, error) {
	return nil, nil
}

func (f *apiFakeStore) LeaseNextJob(ctx context.Context, req store.LeaseJobRequest) (store.JobRecord, error) {
	return store.JobRecord{}, store.ErrNoJobAvailable
}

func (f *apiFakeStore) StartJob(ctx context.Context, req store.StartJobRequest) (store.JobRecord, error) {
	return store.JobRecord{}, nil
}

func (f *apiFakeStore) HeartbeatJob(ctx context.Context, req store.HeartbeatJobRequest) (store.JobRecord, error) {
	return store.JobRecord{}, nil
}

func (f *apiFakeStore) CompleteJob(ctx context.Context, req store.CompleteJobRequest) (store.JobRecord, error) {
	return store.JobRecord{}, nil
}

func (f *apiFakeStore) FailJob(ctx context.Context, req store.FailJobRequest) (store.JobRecord, error) {
	return store.JobRecord{}, nil
}

func (f *apiFakeStore) RecoverExpiredLeases(ctx context.Context, req store.RecoverExpiredLeasesRequest) (int, error) {
	return 0, nil
}

func TestPostJobsEnqueuesJob(t *testing.T) {
	fakeStore := &apiFakeStore{}
	service := app.New(fakeStore, func() string { return "job-http-1" })
	handler := NewHandler(service)
	body := bytes.NewBufferString(`{"queue":"default","type":"send_email","payload":{"email":"user@example.com"},"idempotencyKey":"email:1"}`)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", body)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d with body %s", rec.Code, rec.Body.String())
	}

	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response["id"] != "job-http-1" {
		t.Fatalf("expected id job-http-1, got %v", response["id"])
	}
	if response["state"] != string(domain.JobStateQueued) {
		t.Fatalf("expected queued state, got %v", response["state"])
	}
}

func TestPostJobsRejectsInvalidJSON(t *testing.T) {
	service := app.New(&apiFakeStore{}, func() string { return "job-http-1" })
	handler := NewHandler(service)

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(`{"queue":`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
}

func TestGetJobReturnsJob(t *testing.T) {
	fakeStore := &apiFakeStore{job: store.JobRecord{ID: "job-1", Queue: "default", Type: "send_email", State: domain.JobStateQueued}}
	service := app.New(fakeStore, func() string { return "unused" })
	handler := NewHandler(service)

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/job-1", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d with body %s", rec.Code, rec.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response["id"] != "job-1" {
		t.Fatalf("expected id job-1, got %v", response["id"])
	}
}

func TestGetJobReturnsNotFound(t *testing.T) {
	fakeStore := &apiFakeStore{err: store.ErrJobNotFound}
	service := app.New(fakeStore, func() string { return "unused" })
	handler := NewHandler(service)

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs/missing", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rec.Code)
	}
}
