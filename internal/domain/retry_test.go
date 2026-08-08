package domain

import (
	"testing"
	"time"
)

func TestRetryPolicyShouldRetryWhileAttemptsRemain(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 3, Backoff: BackoffFixed, InitialDelay: time.Second}

	if !policy.ShouldRetry(1) {
		t.Fatal("attempt 1 should retry when max attempts is 3")
	}
	if !policy.ShouldRetry(2) {
		t.Fatal("attempt 2 should retry when max attempts is 3")
	}
	if policy.ShouldRetry(3) {
		t.Fatal("attempt 3 should not retry because max attempts is exhausted")
	}
}

func TestRetryPolicyFixedBackoffUsesInitialDelay(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 5, Backoff: BackoffFixed, InitialDelay: 5 * time.Second}

	if got := policy.DelayForAttempt(3); got != 5*time.Second {
		t.Fatalf("expected fixed delay 5s, got %s", got)
	}
}

func TestRetryPolicyExponentialBackoffDoublesByAttempt(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 5, Backoff: BackoffExponential, InitialDelay: time.Second}

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
	}

	for _, tc := range cases {
		if got := policy.DelayForAttempt(tc.attempt); got != tc.want {
			t.Fatalf("attempt %d: expected %s, got %s", tc.attempt, tc.want, got)
		}
	}
}

func TestRetryPolicyExponentialBackoffRespectsMaxDelay(t *testing.T) {
	policy := RetryPolicy{
		MaxAttempts:  10,
		Backoff:      BackoffExponential,
		InitialDelay: time.Second,
		MaxDelay:     3 * time.Second,
	}

	if got := policy.DelayForAttempt(5); got != 3*time.Second {
		t.Fatalf("expected capped delay 3s, got %s", got)
	}
}

func TestRetryPolicyValidateRejectsInvalidValues(t *testing.T) {
	cases := []RetryPolicy{
		{MaxAttempts: 0, Backoff: BackoffFixed, InitialDelay: time.Second},
		{MaxAttempts: 3, Backoff: "linear", InitialDelay: time.Second},
		{MaxAttempts: 3, Backoff: BackoffFixed, InitialDelay: 0},
	}

	for _, policy := range cases {
		if err := policy.Validate(); err != ErrInvalidRetryPolicy {
			t.Fatalf("expected ErrInvalidRetryPolicy for %+v, got %v", policy, err)
		}
	}
}
