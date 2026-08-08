package domain

type JobState string

const (
	JobStateQueued         JobState = "queued"
	JobStateLeased         JobState = "leased"
	JobStateRunning        JobState = "running"
	JobStateCompleted      JobState = "completed"
	JobStateFailed         JobState = "failed"
	JobStateRetryScheduled JobState = "retry_scheduled"
	JobStateDeadLettered   JobState = "dead_lettered"
)

func (s JobState) Terminal() bool {
	return s == JobStateCompleted || s == JobStateDeadLettered
}

func CanTransition(from JobState, to JobState) bool {
	allowed := map[JobState][]JobState{
		JobStateQueued:         {JobStateLeased},
		JobStateLeased:         {JobStateRunning, JobStateQueued},
		JobStateRunning:        {JobStateCompleted, JobStateFailed},
		JobStateFailed:         {JobStateRetryScheduled, JobStateDeadLettered},
		JobStateRetryScheduled: {JobStateQueued},
	}

	for _, candidate := range allowed[from] {
		if candidate == to {
			return true
		}
	}
	return false
}

func TransitionJobState(from JobState, to JobState) error {
	if !CanTransition(from, to) {
		return ErrInvalidStateTransition
	}
	return nil
}
