package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
			COALESCE(leased_by, ''), COALESCE(lease_id, ''), lease_expires_at,
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
			COALESCE(leased_by, ''), COALESCE(lease_id, ''), lease_expires_at,
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

func (s *Store) LeaseNextJob(ctx context.Context, req store.LeaseJobRequest) (store.JobRecord, error) {
	if err := req.Validate(); err != nil {
		return store.JobRecord{}, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return store.JobRecord{}, fmt.Errorf("begin lease job: %w", err)
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
		WITH candidate AS (
			SELECT id
			FROM jobs
			WHERE state = $1
			AND run_after <= now()
			ORDER BY run_after, created_at, id
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs
		SET state = $2,
			leased_by = $3,
			lease_id = $4,
			lease_expires_at = now() + ($5::bigint * interval '1 millisecond'),
			updated_at = now()
		FROM candidate
		WHERE jobs.id = candidate.id
		RETURNING
			jobs.id, jobs.queue, jobs.type, jobs.payload, jobs.state, jobs.attempt,
			jobs.max_attempts, jobs.backoff_type, jobs.initial_delay_ms, jobs.max_delay_ms,
			jobs.run_after, COALESCE(jobs.idempotency_key, ''), COALESCE(jobs.last_error, ''),
			COALESCE(jobs.leased_by, ''), COALESCE(jobs.lease_id, ''), jobs.lease_expires_at,
			jobs.created_at, jobs.updated_at
	`, domain.JobStateQueued, domain.JobStateLeased, req.WorkerID, req.LeaseID, req.LeaseDuration.Milliseconds())

	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.JobRecord{}, store.ErrNoJobAvailable
	}
	if err != nil {
		return store.JobRecord{}, fmt.Errorf("lease job: %w", err)
	}
	if err := insertJobEvent(ctx, tx, job.ID, "job_leased", "job leased"); err != nil {
		return store.JobRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.JobRecord{}, fmt.Errorf("commit lease job: %w", err)
	}
	return job, nil
}

func (s *Store) StartJob(ctx context.Context, req store.StartJobRequest) (store.JobRecord, error) {
	if err := req.Validate(); err != nil {
		return store.JobRecord{}, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return store.JobRecord{}, fmt.Errorf("begin start job: %w", err)
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
		UPDATE jobs
		SET state = $4,
			attempt = attempt + 1,
			updated_at = now()
		WHERE id = $1
		AND leased_by = $2
		AND lease_id = $3
		AND state = $5
		AND lease_expires_at > now()
		RETURNING
			id, queue, type, payload, state, attempt,
			max_attempts, backoff_type, initial_delay_ms, max_delay_ms,
			run_after, COALESCE(idempotency_key, ''), COALESCE(last_error, ''),
			COALESCE(leased_by, ''), COALESCE(lease_id, ''), lease_expires_at,
			created_at, updated_at
	`, req.JobID, req.WorkerID, req.LeaseID, domain.JobStateRunning, domain.JobStateLeased)

	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.JobRecord{}, store.ErrLeaseNotOwned
	}
	if err != nil {
		return store.JobRecord{}, fmt.Errorf("start job: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO job_attempts (job_id, attempt, worker_id, lease_id, started_at)
		VALUES ($1, $2, $3, $4, now())
	`, job.ID, job.Attempt, req.WorkerID, req.LeaseID); err != nil {
		return store.JobRecord{}, fmt.Errorf("insert job attempt: %w", err)
	}
	if err := insertJobEvent(ctx, tx, job.ID, "job_started", "job started"); err != nil {
		return store.JobRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.JobRecord{}, fmt.Errorf("commit start job: %w", err)
	}
	return job, nil
}

func (s *Store) HeartbeatJob(ctx context.Context, req store.HeartbeatJobRequest) (store.JobRecord, error) {
	if err := req.Validate(); err != nil {
		return store.JobRecord{}, err
	}

	row := s.pool.QueryRow(ctx, `
		UPDATE jobs
		SET lease_expires_at = now() + ($4::bigint * interval '1 millisecond'),
			updated_at = now()
		WHERE id = $1
		AND leased_by = $2
		AND lease_id = $3
		AND state IN ($5, $6)
		AND lease_expires_at > now()
		RETURNING
			id, queue, type, payload, state, attempt,
			max_attempts, backoff_type, initial_delay_ms, max_delay_ms,
			run_after, COALESCE(idempotency_key, ''), COALESCE(last_error, ''),
			COALESCE(leased_by, ''), COALESCE(lease_id, ''), lease_expires_at,
			created_at, updated_at
	`, req.JobID, req.WorkerID, req.LeaseID, req.LeaseDuration.Milliseconds(), domain.JobStateLeased, domain.JobStateRunning)

	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.JobRecord{}, store.ErrLeaseNotOwned
	}
	if err != nil {
		return store.JobRecord{}, fmt.Errorf("heartbeat job: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO job_events (job_id, event_type, message)
		VALUES ($1, $2, $3)
	`, job.ID, "job_heartbeat", "job heartbeat"); err != nil {
		return store.JobRecord{}, fmt.Errorf("insert job event: %w", err)
	}
	return job, nil
}

func (s *Store) CompleteJob(ctx context.Context, req store.CompleteJobRequest) (store.JobRecord, error) {
	if err := req.Validate(); err != nil {
		return store.JobRecord{}, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return store.JobRecord{}, fmt.Errorf("begin complete job: %w", err)
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
		UPDATE jobs
		SET state = $4,
			leased_by = NULL,
			lease_id = NULL,
			lease_expires_at = NULL,
			updated_at = now()
		WHERE id = $1
		AND leased_by = $2
		AND lease_id = $3
		AND state = $5
		AND lease_expires_at > now()
		RETURNING
			id, queue, type, payload, state, attempt,
			max_attempts, backoff_type, initial_delay_ms, max_delay_ms,
			run_after, COALESCE(idempotency_key, ''), COALESCE(last_error, ''),
			COALESCE(leased_by, ''), COALESCE(lease_id, ''), lease_expires_at,
			created_at, updated_at
	`, req.JobID, req.WorkerID, req.LeaseID, domain.JobStateCompleted, domain.JobStateRunning)

	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.JobRecord{}, store.ErrLeaseNotOwned
	}
	if err != nil {
		return store.JobRecord{}, fmt.Errorf("complete job: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE job_attempts
		SET finished_at = now()
		WHERE job_id = $1
		AND attempt = $2
	`, job.ID, job.Attempt); err != nil {
		return store.JobRecord{}, fmt.Errorf("finish job attempt: %w", err)
	}
	if err := insertJobEvent(ctx, tx, job.ID, "job_completed", "job completed"); err != nil {
		return store.JobRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.JobRecord{}, fmt.Errorf("commit complete job: %w", err)
	}
	return job, nil
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
	var leaseExpiresAt *time.Time

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
		&job.LeasedBy,
		&job.LeaseID,
		&leaseExpiresAt,
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
	if leaseExpiresAt != nil {
		job.LeaseExpiresAt = leaseExpiresAt.UTC()
	}

	return job, nil
}

type txExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func insertJobEvent(ctx context.Context, exec txExecutor, jobID string, eventType string, message string) error {
	if _, err := exec.Exec(ctx, `
		INSERT INTO job_events (job_id, event_type, message)
		VALUES ($1, $2, $3)
	`, jobID, eventType, message); err != nil {
		return fmt.Errorf("insert job event: %w", err)
	}
	return nil
}
