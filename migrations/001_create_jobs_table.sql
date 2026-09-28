-- +goose Up
CREATE TABLE jobs
(
    id           VARCHAR(36) PRIMARY KEY,
    type         VARCHAR(100) NOT NULL,
    payload      JSONB        NOT NULL,
    priority     INTEGER      NOT NULL DEFAULT 5,
    status       VARCHAR(20)  NOT NULL DEFAULT 'pending',
    queue        VARCHAR(100) NOT NULL DEFAULT 'default',
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error        TEXT,
    retry_count  INTEGER      NOT NULL DEFAULT 0,
    max_retries  INTEGER      NOT NULL DEFAULT 3
);

CREATE INDEX idx_jobs_status ON jobs (status);
CREATE INDEX idx_jobs_queue ON jobs (queue);
CREATE INDEX idx_jobs_created_at ON jobs (created_at DESC);

-- +goose Down
DROP TABLE jobs;