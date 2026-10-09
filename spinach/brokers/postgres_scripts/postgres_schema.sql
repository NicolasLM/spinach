-- Spinach Postgres broker schema, version 1.
-- Applied statement by statement. Do not put a semicolon inside a statement.
-- The runtime role needs DML on these tables. The application role used with
-- join_transaction needs INSERT on spinach_queue_job and spinach_future_job
-- plus USAGE on spinach_queue_job_position_seq.

-- name: apply_schema()#
CREATE TABLE IF NOT EXISTS spinach_schema (
    version integer PRIMARY KEY
);

INSERT INTO spinach_schema (version) VALUES (1)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS spinach_queue_job (
    namespace text NOT NULL,
    queue text NOT NULL,
    position bigserial,
    job_id uuid NOT NULL,
    task_name text NOT NULL,
    payload text NOT NULL,
    PRIMARY KEY (namespace, job_id)
);

CREATE INDEX IF NOT EXISTS spinach_queue_job_fifo
    ON spinach_queue_job (namespace, queue, position);

CREATE TABLE IF NOT EXISTS spinach_future_job (
    namespace text NOT NULL,
    job_id uuid NOT NULL,
    at timestamptz NOT NULL,
    payload text NOT NULL,
    PRIMARY KEY (namespace, job_id)
);

CREATE INDEX IF NOT EXISTS spinach_future_job_due
    ON spinach_future_job (namespace, at);

CREATE TABLE IF NOT EXISTS spinach_running_job (
    namespace text NOT NULL,
    broker_id uuid NOT NULL,
    job_id uuid NOT NULL,
    task_name text NOT NULL,
    queue text NOT NULL,
    max_retries integer NOT NULL,
    retries integer NOT NULL,
    payload text NOT NULL,
    PRIMARY KEY (namespace, broker_id, job_id)
);

CREATE TABLE IF NOT EXISTS spinach_broker (
    namespace text NOT NULL,
    broker_id uuid NOT NULL,
    last_seen_at bigint NOT NULL,
    info text NOT NULL,
    PRIMARY KEY (namespace, broker_id)
);

CREATE INDEX IF NOT EXISTS spinach_broker_last_seen
    ON spinach_broker (namespace, last_seen_at);

CREATE TABLE IF NOT EXISTS spinach_periodic_task (
    namespace text NOT NULL,
    name text NOT NULL,
    periodicity_seconds integer NOT NULL,
    next_at timestamptz NOT NULL,
    payload text NOT NULL,
    PRIMARY KEY (namespace, name)
);

CREATE INDEX IF NOT EXISTS spinach_periodic_task_due
    ON spinach_periodic_task (namespace, next_at);

CREATE TABLE IF NOT EXISTS spinach_concurrency (
    namespace text NOT NULL,
    task_name text NOT NULL,
    max_concurrency integer NOT NULL,
    current_concurrency integer NOT NULL DEFAULT 0,
    PRIMARY KEY (namespace, task_name)
);

CREATE TABLE IF NOT EXISTS spinach_idempotency (
    namespace text NOT NULL,
    token text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace, token)
);
