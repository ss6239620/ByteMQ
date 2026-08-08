package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/sharvesh/bytemq/internal/domain"
	"github.com/sharvesh/bytemq/internal/store"
)

type workerFakeStore struct {
	job             store.JobRecord
	leaseErr        error
	started         bool
	completed       bool
	failed          bool
	heartbeatCount  int
	heartbeatSignal chan struct{}
	failMessage     string
}

func (f *workerFakeStore) EnqueueJob(ctx context.Context, req store.EnqueueJobRequest) (store.JobRecord, error) {
	return store.JobRecord{}, nil
}

func (f *workerFakeStore) GetJob(ctx context.Context, id string) (store.JobRecord, error) {
	return f.job, nil
}

func (f *workerFakeStore) ListJobEvents(ctx context.Context, jobID string) ([]store.JobEventRecord, error) {
	return nil, nil
}

func (f *workerFakeStore) LeaseNextJob(ctx context.Context, req store.LeaseJobRequest) (store.JobRecord, error) {
	if f.leaseErr != nil {
		return store.JobRecord{}, f.leaseErr
	}
	f.job.LeasedBy = req.WorkerID
	f.job.LeaseID = req.LeaseID
	f.job.State = domain.JobStateLeased
	return f.job, nil
}

func (f *workerFakeStore) StartJob(ctx context.Context, req store.StartJobRequest) (store.JobRecord, error) {
	f.started = true
	f.job.State = domain.JobStateRunning
	f.job.Attempt++
	return f.job, nil
}

func (f *workerFakeStore) HeartbeatJob(ctx context.Context, req store.HeartbeatJobRequest) (store.JobRecord, error) {
	f.heartbeatCount++
	if f.heartbeatSignal != nil {
		select {
		case f.heartbeatSignal <- struct{}{}:
		default:
		}
	}
	return f.job, nil
}

func (f *workerFakeStore) CompleteJob(ctx context.Context, req store.CompleteJobRequest) (store.JobRecord, error) {
	f.completed = true
	f.job.State = domain.JobStateCompleted
	return f.job, nil
}

func (f *workerFakeStore) FailJob(ctx context.Context, req store.FailJobRequest) (store.JobRecord, error) {
	f.failed = true
	f.failMessage = req.Error
	f.job.State = domain.JobStateRetryScheduled
	return f.job, nil
}

func (f *workerFakeStore) RecoverExpiredLeases(ctx context.Context, req store.RecoverExpiredLeasesRequest) (int, error) {
	return 0, nil
}

func newWorkerTestRunner(jobStore *workerFakeStore, handlers map[string]Handler) *Runner {
	return New(jobStore, Config{
		WorkerID:          "worker-1",
		LeaseDuration:     30 * time.Second,
		HeartbeatInterval: time.Millisecond,
		NewLeaseID:        func() string { return "lease-1" },
	}, handlers)
}

func TestProcessOneReturnsFalseWhenNoJobAvailable(t *testing.T) {
	jobStore := &workerFakeStore{leaseErr: store.ErrNoJobAvailable}
	runner := newWorkerTestRunner(jobStore, nil)

	processed, err := runner.ProcessOne(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if processed {
		t.Fatal("expected processed=false")
	}
}

func TestProcessOneCompletesSuccessfulJob(t *testing.T) {
	jobStore := &workerFakeStore{job: store.JobRecord{ID: "job-1", Type: "echo", Payload: json.RawMessage(`{"ok":true}`)}}
	runner := newWorkerTestRunner(jobStore, map[string]Handler{
		"echo": func(ctx context.Context, payload json.RawMessage) error { return nil },
	})

	processed, err := runner.ProcessOne(context.Background())
	if err != nil {
		t.Fatalf("process one: %v", err)
	}
	if !processed {
		t.Fatal("expected processed=true")
	}
	if !jobStore.started || !jobStore.completed {
		t.Fatalf("expected job to start and complete")
	}
	if jobStore.failed {
		t.Fatal("did not expect job failure")
	}
}

func TestProcessOneFailsHandlerError(t *testing.T) {
	jobStore := &workerFakeStore{job: store.JobRecord{ID: "job-1", Type: "echo"}}
	runner := newWorkerTestRunner(jobStore, map[string]Handler{
		"echo": func(ctx context.Context, payload json.RawMessage) error { return errors.New("boom") },
	})

	processed, err := runner.ProcessOne(context.Background())
	if err != nil {
		t.Fatalf("process one: %v", err)
	}
	if !processed {
		t.Fatal("expected processed=true")
	}
	if !jobStore.failed {
		t.Fatal("expected job failure")
	}
	if jobStore.failMessage != "boom" {
		t.Fatalf("expected fail message boom, got %q", jobStore.failMessage)
	}
}

func TestProcessOneFailsMissingHandler(t *testing.T) {
	jobStore := &workerFakeStore{job: store.JobRecord{ID: "job-1", Type: "missing"}}
	runner := newWorkerTestRunner(jobStore, nil)

	processed, err := runner.ProcessOne(context.Background())
	if err != nil {
		t.Fatalf("process one: %v", err)
	}
	if !processed || !jobStore.failed {
		t.Fatal("expected missing handler to fail job")
	}
}

func TestProcessOneHeartbeatsWhileHandlerRuns(t *testing.T) {
	heartbeatSignal := make(chan struct{}, 1)
	jobStore := &workerFakeStore{
		job:             store.JobRecord{ID: "job-1", Type: "slow"},
		heartbeatSignal: heartbeatSignal,
	}
	runner := newWorkerTestRunner(jobStore, map[string]Handler{
		"slow": func(ctx context.Context, payload json.RawMessage) error {
			select {
			case <-heartbeatSignal:
				return nil
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for heartbeat")
				return nil
			}
		},
	})

	processed, err := runner.ProcessOne(context.Background())
	if err != nil {
		t.Fatalf("process one: %v", err)
	}
	if !processed {
		t.Fatal("expected processed=true")
	}
	if jobStore.heartbeatCount == 0 {
		t.Fatal("expected at least one heartbeat")
	}
}
