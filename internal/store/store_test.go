package store

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/sharvesh/bytemq/internal/domain"
)

func validEnqueueRequest() EnqueueJobRequest {
	return EnqueueJobRequest{
		ID:      "job_123",
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

func TestEnqueueJobRequestValidateAcceptsValidRequest(t *testing.T) {
	req := validEnqueueRequest()

	if err := req.Validate(); err != nil {
		t.Fatalf("expected valid request, got %v", err)
	}
}

func TestEnqueueJobRequestValidateRejectsMissingRequiredFields(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*EnqueueJobRequest)
	}{
		{"missing id", func(req *EnqueueJobRequest) { req.ID = "" }},
		{"missing queue", func(req *EnqueueJobRequest) { req.Queue = "" }},
		{"missing type", func(req *EnqueueJobRequest) { req.Type = "" }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := validEnqueueRequest()
			tc.mutate(&req)

			if err := req.Validate(); err != ErrInvalidJob {
				t.Fatalf("expected ErrInvalidJob, got %v", err)
			}
		})
	}
}

func TestEnqueueJobRequestValidateRejectsInvalidPayload(t *testing.T) {
	req := validEnqueueRequest()
	req.Payload = json.RawMessage(`{"broken":`)

	if err := req.Validate(); err != ErrInvalidJob {
		t.Fatalf("expected ErrInvalidJob, got %v", err)
	}
}

func TestEnqueueJobRequestValidateRejectsInvalidRetryPolicy(t *testing.T) {
	req := validEnqueueRequest()
	req.RetryPolicy.MaxAttempts = 0

	if err := req.Validate(); err != ErrInvalidJob {
		t.Fatalf("expected ErrInvalidJob, got %v", err)
	}
}

func TestEnqueueJobRequestValidateDoesNotMutateRunAfter(t *testing.T) {
	req := validEnqueueRequest()
	localTime := time.Date(2026, 8, 8, 15, 30, 0, 0, time.FixedZone("IST", 19800))
	req.RunAfter = localTime

	if err := req.Validate(); err != nil {
		t.Fatalf("expected valid request, got %v", err)
	}
	if !req.RunAfter.Equal(localTime) || req.RunAfter.Location() != localTime.Location() {
		t.Fatalf("Validate should not mutate RunAfter")
	}
}
