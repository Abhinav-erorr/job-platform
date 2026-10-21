CREATE TABLE IF NOT EXISTS job_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);