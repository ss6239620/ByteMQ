package domain

import "testing"

func TestCanTransitionAllowsCoreLifecycle(t *testing.T) {
	cases := []struct {
		name string
		from JobState
		to   JobState
	}{
		{"queued to leased", JobStateQueued, JobStateLeased},
		{"leased to running", JobStateLeased, JobStateRunning},
		{"running to completed", JobStateRunning, JobStateCompleted},
		{"running to failed", JobStateRunning, JobStateFailed},
		{"failed to retry scheduled", JobStateFailed, JobStateRetryScheduled},
		{"retry scheduled to queued", JobStateRetryScheduled, JobStateQueued},
		{"failed to dead lettered", JobStateFailed, JobStateDeadLettered},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !CanTransition(tc.from, tc.to) {
				t.Fatalf("expected transition %s -> %s to be allowed", tc.from, tc.to)
			}
		})
	}
}

func TestCanTransitionRejectsUnsafeLifecycleChanges(t *testing.T) {
	cases := []struct {
		name string
		from JobState
		to   JobState
	}{
		{"completed to queued", JobStateCompleted, JobStateQueued},
		{"dead lettered to queued", JobStateDeadLettered, JobStateQueued},
		{"queued to completed", JobStateQueued, JobStateCompleted},
		{"leased to completed without running", JobStateLeased, JobStateCompleted},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if CanTransition(tc.from, tc.to) {
				t.Fatalf("expected transition %s -> %s to be rejected", tc.from, tc.to)
			}
		})
	}
}

func TestTransitionJobStateReturnsInvalidTransitionError(t *testing.T) {
	err := TransitionJobState(JobStateCompleted, JobStateQueued)
	if err == nil {
		t.Fatal("expected invalid transition error")
	}
	if err != ErrInvalidStateTransition {
		t.Fatalf("expected ErrInvalidStateTransition, got %v", err)
	}
}

func TestTerminalIdentifiesCompletedAndDeadLettered(t *testing.T) {
	if !JobStateCompleted.Terminal() {
		t.Fatal("completed should be terminal")
	}
	if !JobStateDeadLettered.Terminal() {
		t.Fatal("dead lettered should be terminal")
	}
	if JobStateQueued.Terminal() {
		t.Fatal("queued should not be terminal")
	}
}
