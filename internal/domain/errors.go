package domain

import "errors"

var ErrInvalidStateTransition = errors.New("invalid job state transition")

var ErrInvalidRetryPolicy = errors.New("invalid retry policy")

var ErrInvalidIdempotencyScope = errors.New("invalid idempotency scope")
