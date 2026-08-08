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

func TestLeaseJobRequestValidate(t *testing.T) {
	valid := LeaseJobRequest{WorkerID: "worker-1", LeaseID: "lease-1", LeaseDuration: 30 * time.Second}
	if err := valid.Validate(); err != nil {
		t.Fatalf("expected valid lease request, got %v", err)
	}

	cases := []LeaseJobRequest{
		{WorkerID: "", LeaseID: "lease-1", LeaseDuration: 30 * time.Second},
		{WorkerID: "worker-1", LeaseID: "", LeaseDuration: 30 * time.Second},
		{WorkerID: "worker-1", LeaseID: "lease-1", LeaseDuration: 0},
	}
	for _, req := range cases {
		if err := req.Validate(); err != ErrInvalidLease {
			t.Fatalf("expected ErrInvalidLease for %+v, got %v", req, err)
		}
	}
}

func TestOwnershipRequestValidate(t *testing.T) {
	validStart := StartJobRequest{JobID: "job-1", WorkerID: "worker-1", LeaseID: "lease-1"}
	if err := validStart.Validate(); err != nil {
		t.Fatalf("expected valid start request, got %v", err)
	}
	validComplete := CompleteJobRequest{JobID: "job-1", WorkerID: "worker-1", LeaseID: "lease-1"}
	if err := validComplete.Validate(); err != nil {
		t.Fatalf("expected valid complete request, got %v", err)
	}
	validFail := FailJobRequest{JobID: "job-1", WorkerID: "worker-1", LeaseID: "lease-1", Error: "boom"}
	if err := validFail.Validate(); err != nil {
		t.Fatalf("expected valid fail request, got %v", err)
	}

	invalidStart := StartJobRequest{JobID: "", WorkerID: "worker-1", LeaseID: "lease-1"}
	if err := invalidStart.Validate(); err != ErrInvalidLease {
		t.Fatalf("expected ErrInvalidLease for invalid start, got %v", err)
	}
	invalidComplete := CompleteJobRequest{JobID: "job-1", WorkerID: "", LeaseID: "lease-1"}
	if err := invalidComplete.Validate(); err != ErrInvalidLease {
		t.Fatalf("expected ErrInvalidLease for invalid complete, got %v", err)
	}
	invalidFail := FailJobRequest{JobID: "job-1", WorkerID: "worker-1", LeaseID: ""}
	if err := invalidFail.Validate(); err != ErrInvalidLease {
		t.Fatalf("expected ErrInvalidLease for invalid fail, got %v", err)
	}
}

func TestHeartbeatJobRequestValidate(t *testing.T) {
	valid := HeartbeatJobRequest{JobID: "job-1", WorkerID: "worker-1", LeaseID: "lease-1", LeaseDuration: 30 * time.Second}
	if err := valid.Validate(); err != nil {
		t.Fatalf("expected valid heartbeat, got %v", err)
	}

	invalid := HeartbeatJobRequest{JobID: "job-1", WorkerID: "worker-1", LeaseID: "lease-1", LeaseDuration: 0}
	if err := invalid.Validate(); err != ErrInvalidLease {
		t.Fatalf("expected ErrInvalidLease, got %v", err)
	}
}

func TestRecoverExpiredLeasesRequestValidate(t *testing.T) {
	if err := (RecoverExpiredLeasesRequest{Limit: 10}).Validate(); err != nil {
		t.Fatalf("expected valid recovery request, got %v", err)
	}
	if err := (RecoverExpiredLeasesRequest{Limit: 0}).Validate(); err != ErrInvalidLease {
		t.Fatalf("expected ErrInvalidLease, got %v", err)
	}
}
