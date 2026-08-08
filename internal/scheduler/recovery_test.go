package scheduler

import (
	"context"
	"testing"

	"github.com/sharvesh/bytemq/internal/store"
)

type schedulerFakeStore struct {
	limit int
}

func (f *schedulerFakeStore) EnqueueJob(ctx context.Context, req store.EnqueueJobRequest) (store.JobRecord, error) {
	return store.JobRecord{}, nil
}

func (f *schedulerFakeStore) GetJob(ctx context.Context, id string) (store.JobRecord, error) {
	return store.JobRecord{}, nil
}

func (f *schedulerFakeStore) ListJobEvents(ctx context.Context, jobID string) ([]store.JobEventRecord, error) {
	return nil, nil
}

func (f *schedulerFakeStore) LeaseNextJob(ctx context.Context, req store.LeaseJobRequest) (store.JobRecord, error) {
	return store.JobRecord{}, store.ErrNoJobAvailable
}

func (f *schedulerFakeStore) StartJob(ctx context.Context, req store.StartJobRequest) (store.JobRecord, error) {
	return store.JobRecord{}, nil
}

func (f *schedulerFakeStore) HeartbeatJob(ctx context.Context, req store.HeartbeatJobRequest) (store.JobRecord, error) {
	return store.JobRecord{}, nil
}

func (f *schedulerFakeStore) CompleteJob(ctx context.Context, req store.CompleteJobRequest) (store.JobRecord, error) {
	return store.JobRecord{}, nil
}

func (f *schedulerFakeStore) FailJob(ctx context.Context, req store.FailJobRequest) (store.JobRecord, error) {
	return store.JobRecord{}, nil
}

func (f *schedulerFakeStore) RecoverExpiredLeases(ctx context.Context, req store.RecoverExpiredLeasesRequest) (int, error) {
	f.limit = req.Limit
	return 7, nil
}

func TestRecoveryRecoverOnceUsesConfiguredLimit(t *testing.T) {
	jobStore := &schedulerFakeStore{}
	recovery := NewRecovery(jobStore, 25)

	recovered, err := recovery.RecoverOnce(context.Background())
	if err != nil {
		t.Fatalf("recover once: %v", err)
	}
	if recovered != 7 {
		t.Fatalf("expected recovered count 7, got %d", recovered)
	}
	if jobStore.limit != 25 {
		t.Fatalf("expected limit 25, got %d", jobStore.limit)
	}
}

func TestRecoveryDefaultsInvalidLimit(t *testing.T) {
	jobStore := &schedulerFakeStore{}
	recovery := NewRecovery(jobStore, 0)

	if _, err := recovery.RecoverOnce(context.Background()); err != nil {
		t.Fatalf("recover once: %v", err)
	}
	if jobStore.limit != 100 {
		t.Fatalf("expected default limit 100, got %d", jobStore.limit)
	}
}
