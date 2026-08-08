package worker

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/sharvesh/bytemq/internal/store"
)

type Handler func(context.Context, json.RawMessage) error

type Config struct {
	WorkerID          string
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	NewLeaseID        func() string
}

type Runner struct {
	jobStore store.JobStore
	cfg      Config
	handlers map[string]Handler
}

func New(jobStore store.JobStore, cfg Config, handlers map[string]Handler) *Runner {
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 10 * time.Second
	}
	if cfg.NewLeaseID == nil {
		cfg.NewLeaseID = func() string { return time.Now().UTC().Format("20060102150405.000000000") }
	}
	return &Runner{jobStore: jobStore, cfg: cfg, handlers: handlers}
}

func (r *Runner) ProcessOne(ctx context.Context) (bool, error) {
	leaseID := r.cfg.NewLeaseID()
	job, err := r.jobStore.LeaseNextJob(ctx, store.LeaseJobRequest{
		WorkerID:      r.cfg.WorkerID,
		LeaseID:       leaseID,
		LeaseDuration: r.cfg.LeaseDuration,
	})
	if errors.Is(err, store.ErrNoJobAvailable) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	job, err = r.jobStore.StartJob(ctx, store.StartJobRequest{
		JobID:    job.ID,
		WorkerID: r.cfg.WorkerID,
		LeaseID:  leaseID,
	})
	if err != nil {
		return true, err
	}

	handler := r.handlers[job.Type]
	if handler == nil {
		_, failErr := r.jobStore.FailJob(ctx, store.FailJobRequest{
			JobID:    job.ID,
			WorkerID: r.cfg.WorkerID,
			LeaseID:  leaseID,
			Error:    "no handler registered for job type: " + job.Type,
		})
		return true, failErr
	}

	handlerCtx, cancelHeartbeat := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})
	go r.heartbeat(handlerCtx, heartbeatDone, job.ID, leaseID)

	handlerErr := handler(ctx, job.Payload)
	cancelHeartbeat()
	<-heartbeatDone

	if handlerErr != nil {
		_, failErr := r.jobStore.FailJob(ctx, store.FailJobRequest{
			JobID:    job.ID,
			WorkerID: r.cfg.WorkerID,
			LeaseID:  leaseID,
			Error:    handlerErr.Error(),
		})
		return true, failErr
	}

	_, err = r.jobStore.CompleteJob(ctx, store.CompleteJobRequest{
		JobID:    job.ID,
		WorkerID: r.cfg.WorkerID,
		LeaseID:  leaseID,
	})
	return true, err
}

func (r *Runner) heartbeat(ctx context.Context, done chan<- struct{}, jobID string, leaseID string) {
	defer close(done)

	ticker := time.NewTicker(r.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = r.jobStore.HeartbeatJob(ctx, store.HeartbeatJobRequest{
				JobID:         jobID,
				WorkerID:      r.cfg.WorkerID,
				LeaseID:       leaseID,
				LeaseDuration: r.cfg.LeaseDuration,
			})
		}
	}
}
