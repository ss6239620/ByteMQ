CREATE TABLE IF NOT EXISTS schema_migrations (
    version text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
    id text PRIMARY KEY,
    queue text NOT NULL,
    type text NOT NULL,
    payload jsonb NOT NULL,
    state text NOT NULL,
    attempt integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL,
    backoff_type text NOT NULL,
    initial_delay_ms bigint NOT NULL,
    max_delay_ms bigint NOT NULL DEFAULT 0,
    run_after timestamptz NOT NULL,
    leased_by text,
    lease_id text,
    lease_expires_at timestamptz,
    idempotency_key text,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT jobs_state_check CHECK (state IN ('queued', 'leased', 'running', 'completed', 'failed', 'retry_scheduled', 'dead_lettered')),
    CONSTRAINT jobs_attempt_check CHECK (attempt >= 0),
    CONSTRAINT jobs_max_attempts_check CHECK (max_attempts >= 1),
    CONSTRAINT jobs_delay_check CHECK (initial_delay_ms > 0 AND max_delay_ms >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_queue_idempotency_key_idx
    ON jobs (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS jobs_state_run_after_idx
    ON jobs (state, run_after, created_at);

CREATE INDEX IF NOT EXISTS jobs_lease_expiry_idx
    ON jobs (lease_expires_at)
    WHERE lease_expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS job_attempts (
    id bigserial PRIMARY KEY,
    job_id text NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt integer NOT NULL,
    worker_id text,
    lease_id text,
    started_at timestamptz,
    finished_at timestamptz,
    error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT job_attempts_attempt_check CHECK (attempt >= 1)
);

CREATE INDEX IF NOT EXISTS job_attempts_job_id_idx
    ON job_attempts (job_id, attempt);

CREATE TABLE IF NOT EXISTS job_events (
    id bigserial PRIMARY KEY,
    job_id text NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    event_type text NOT NULL,
    message text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS job_events_job_id_created_at_idx
    ON job_events (job_id, created_at, id);
