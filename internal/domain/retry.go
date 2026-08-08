package domain

import "time"

type BackoffType string

const (
	BackoffFixed       BackoffType = "fixed"
	BackoffExponential BackoffType = "exponential"
)

type RetryPolicy struct {
	MaxAttempts  int
	Backoff      BackoffType
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

func (p RetryPolicy) Validate() error {
	if p.MaxAttempts < 1 {
		return ErrInvalidRetryPolicy
	}
	if p.InitialDelay <= 0 {
		return ErrInvalidRetryPolicy
	}
	if p.Backoff != BackoffFixed && p.Backoff != BackoffExponential {
		return ErrInvalidRetryPolicy
	}
	return nil
}

func (p RetryPolicy) ShouldRetry(attempt int) bool {
	return attempt < p.MaxAttempts
}

func (p RetryPolicy) DelayForAttempt(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	delay := p.InitialDelay
	if p.Backoff == BackoffExponential {
		for range attempt - 1 {
			delay *= 2
		}
	}

	if p.MaxDelay > 0 && delay > p.MaxDelay {
		return p.MaxDelay
	}
	return delay
}
