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

var ErrInvalidLease = errors.New("invalid lease")

var ErrNoJobAvailable = errors.New("no job available")

var ErrLeaseNotOwned = errors.New("lease not owned")

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
	LeasedBy       string
	LeaseID        string
	LeaseExpiresAt time.Time
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
	LeaseNextJob(ctx context.Context, req LeaseJobRequest) (JobRecord, error)
	StartJob(ctx context.Context, req StartJobRequest) (JobRecord, error)
	HeartbeatJob(ctx context.Context, req HeartbeatJobRequest) (JobRecord, error)
	CompleteJob(ctx context.Context, req CompleteJobRequest) (JobRecord, error)
	FailJob(ctx context.Context, req FailJobRequest) (JobRecord, error)
	RecoverExpiredLeases(ctx context.Context, req RecoverExpiredLeasesRequest) (int, error)
}

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
