CREATE TYPE schedule_status AS ENUM ('pending', 'claimed', 'completed', 'failed');

CREATE TABLE schedules (
    schedule_id UUID PRIMARY KEY,
    household_id UUID NOT NULL,
    workflow_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    run_at TIMESTAMP NOT NULL,
    recurrence JSONB NULL,
    status schedule_status NOT NULL DEFAULT 'pending',
    idempotency_key TEXT NOT NULL UNIQUE,
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    last_run_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_retry_count_non_negative CHECK (retry_count >= 0),
    CONSTRAINT chk_max_retries_non_negative CHECK (max_retries >= 0),
    CONSTRAINT chk_retry_count_lte_max_retries CHECK (retry_count <= max_retries)
);

CREATE INDEX idx_schedules_status_run_at ON schedules(status, run_at);
