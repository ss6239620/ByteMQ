package scheduler

import (
	"context"

	"github.com/sharvesh/bytemq/internal/store"
)

type Recovery struct {
	jobStore store.JobStore
	limit    int
}

func NewRecovery(jobStore store.JobStore, limit int) *Recovery {
	if limit < 1 {
		limit = 100
	}
	return &Recovery{jobStore: jobStore, limit: limit}
}

func (r *Recovery) RecoverOnce(ctx context.Context) (int, error) {
	return r.jobStore.RecoverExpiredLeases(ctx, store.RecoverExpiredLeasesRequest{Limit: r.limit})
}
