package domain

import "strings"

type IdempotencyScope struct {
	Queue string
	Key   string
}

func NewIdempotencyScope(queue string, key string) (IdempotencyScope, error) {
	scope := IdempotencyScope{
		Queue: strings.TrimSpace(queue),
		Key:   strings.TrimSpace(key),
	}

	if scope.Key != "" && scope.Queue == "" {
		return IdempotencyScope{}, ErrInvalidIdempotencyScope
	}
	return scope, nil
}

func (s IdempotencyScope) Empty() bool {
	return s.Key == ""
}
