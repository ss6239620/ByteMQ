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
}
