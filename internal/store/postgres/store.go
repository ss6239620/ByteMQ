package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sharvesh/bytemq/internal/domain"
	"github.com/sharvesh/bytemq/internal/store"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) EnqueueJob(ctx context.Context, req store.EnqueueJobRequest) (store.JobRecord, error) {
	if err := req.Validate(); err != nil {
		return store.JobRecord{}, err
	}

	runAfter := req.RunAfter.UTC()
	if runAfter.IsZero() {
		runAfter = time.Now().UTC()
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return store.JobRecord{}, fmt.Errorf("begin enqueue job: %w", err)
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
		INSERT INTO jobs (
			id, queue, type, payload, state, attempt,
			max_attempts, backoff_type, initial_delay_ms, max_delay_ms,
			run_after, idempotency_key
		)
		VALUES ($1, $2, $3, $4, $5, 0, $6, $7, $8, $9, $10, NULLIF($11, ''))
		ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL
		DO UPDATE SET updated_at = jobs.updated_at
		RETURNING
			id, queue, type, payload, state, attempt,
			max_attempts, backoff_type, initial_delay_ms, max_delay_ms,
			run_after, COALESCE(idempotency_key, ''), COALESCE(last_error, ''),
			created_at, updated_at, (xmax = 0) AS inserted
	`,
		req.ID,
		req.Queue,
		req.Type,
		req.Payload,
		domain.JobStateQueued,
		req.RetryPolicy.MaxAttempts,
		req.RetryPolicy.Backoff,
		req.RetryPolicy.InitialDelay.Milliseconds(),
		req.RetryPolicy.MaxDelay.Milliseconds(),
		runAfter,
		req.IdempotencyKey,
	)

	job, inserted, err := scanJobWithInserted(row)
	if err != nil {
		return store.JobRecord{}, fmt.Errorf("insert job: %w", err)
	}

	if inserted {
		if _, err := tx.Exec(ctx, `
			INSERT INTO job_events (job_id, event_type, message)
			VALUES ($1, $2, $3)
		`, job.ID, "job_enqueued", "job accepted"); err != nil {
			return store.JobRecord{}, fmt.Errorf("insert job event: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return store.JobRecord{}, fmt.Errorf("commit enqueue job: %w", err)
	}
	return job, nil
}

func (s *Store) GetJob(ctx context.Context, id string) (store.JobRecord, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT
			id, queue, type, payload, state, attempt,
			max_attempts, backoff_type, initial_delay_ms, max_delay_ms,
			run_after, COALESCE(idempotency_key, ''), COALESCE(last_error, ''),
			created_at, updated_at
		FROM jobs
		WHERE id = $1
	`, id)

	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.JobRecord{}, store.ErrJobNotFound
	}
	if err != nil {
		return store.JobRecord{}, fmt.Errorf("get job: %w", err)
	}
	return job, nil
}

func (s *Store) ListJobEvents(ctx context.Context, jobID string) ([]store.JobEventRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, job_id, event_type, message, created_at
		FROM job_events
		WHERE job_id = $1
		ORDER BY created_at, id
	`, jobID)
	if err != nil {
		return nil, fmt.Errorf("list job events: %w", err)
	}
	defer rows.Close()

	var events []store.JobEventRecord
	for rows.Next() {
		var event store.JobEventRecord
		if err := rows.Scan(&event.ID, &event.JobID, &event.EventType, &event.Message, &event.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan job event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate job events: %w", err)
	}
	return events, nil
}

type jobScanner interface {
	Scan(dest ...any) error
}

func scanJob(row jobScanner) (store.JobRecord, error) {
	job, err := scanJobFields(row, nil)
	return job, err
}

func scanJobWithInserted(row jobScanner) (store.JobRecord, bool, error) {
	var inserted bool
	job, err := scanJobFields(row, &inserted)
	return job, inserted, err
}

func scanJobFields(row jobScanner, inserted *bool) (store.JobRecord, error) {
	var job store.JobRecord
	var payload []byte
	var maxAttempts int
	var backoff domain.BackoffType
	var initialDelayMS int64
	var maxDelayMS int64

	dest := []any{
		&job.ID,
		&job.Queue,
		&job.Type,
		&payload,
		&job.State,
		&job.Attempt,
		&maxAttempts,
		&backoff,
		&initialDelayMS,
		&maxDelayMS,
		&job.RunAfter,
		&job.IdempotencyKey,
		&job.LastError,
		&job.CreatedAt,
		&job.UpdatedAt,
	}
	if inserted != nil {
		dest = append(dest, inserted)
	}

	err := row.Scan(dest...)
	if err != nil {
		return store.JobRecord{}, err
	}

	job.Payload = json.RawMessage(payload)
	job.RetryPolicy = domain.RetryPolicy{
		MaxAttempts:  maxAttempts,
		Backoff:      backoff,
		InitialDelay: time.Duration(initialDelayMS) * time.Millisecond,
		MaxDelay:     time.Duration(maxDelayMS) * time.Millisecond,
	}

	return job, nil
}
