package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/sharvesh/bytemq/internal/domain"
	"github.com/sharvesh/bytemq/internal/store"
)

type Service struct {
	jobStore store.JobStore
	newID    func() string
}

type EnqueueRequest struct {
	Queue          string
	Type           string
	Payload        json.RawMessage
	RetryPolicy    domain.RetryPolicy
	RunAfter       time.Time
	IdempotencyKey string
}

func New(jobStore store.JobStore, newID func() string) *Service {
	if newID == nil {
		newID = NewJobID
	}
	return &Service{jobStore: jobStore, newID: newID}
}

func (s *Service) EnqueueJob(ctx context.Context, req EnqueueRequest) (store.JobRecord, error) {
	retryPolicy := req.RetryPolicy
	if retryPolicy.MaxAttempts == 0 {
		retryPolicy = DefaultRetryPolicy()
	}

	return s.jobStore.EnqueueJob(ctx, store.EnqueueJobRequest{
		ID:             s.newID(),
		Queue:          req.Queue,
		Type:           req.Type,
		Payload:        req.Payload,
		RetryPolicy:    retryPolicy,
		RunAfter:       req.RunAfter,
		IdempotencyKey: req.IdempotencyKey,
	})
}

func (s *Service) GetJob(ctx context.Context, id string) (store.JobRecord, error) {
	return s.jobStore.GetJob(ctx, id)
}

func DefaultRetryPolicy() domain.RetryPolicy {
	return domain.RetryPolicy{
		MaxAttempts:  3,
		Backoff:      domain.BackoffExponential,
		InitialDelay: time.Second,
		MaxDelay:     30 * time.Second,
	}
}

func NewJobID() string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "job_" + time.Now().UTC().Format("20060102150405.000000000")
	}
	return "job_" + hex.EncodeToString(bytes[:])
}
