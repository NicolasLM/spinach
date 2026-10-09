-- Spinach Postgres broker queries

-- =========================================================================
-- Schema verification
-- =========================================================================

-- name: lock_schema()!
-- Session-scoped advisory lock, held while the schema script runs.
SELECT pg_advisory_lock(hashtext('spinach.schema')::bigint);

-- name: unlock_schema()!
SELECT pg_advisory_unlock(hashtext('spinach.schema')::bigint);

-- name: cluster_identity()^
-- (system_identifier, database name) of the current session.
SELECT system_identifier, current_database()
FROM pg_control_system();

-- name: regclass(name)$
-- Follows search_path, so a stub in an earlier schema cannot hide a table.
SELECT to_regclass(:name);

-- name: relation_oid(name)$
SELECT to_regclass(:name)::oid;

-- name: table_columns(name)
-- (column, format_type, not_null) for each live column.
SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull
FROM pg_attribute a
WHERE a.attrelid = to_regclass(:name)
AND a.attnum > 0 AND NOT a.attisdropped;

-- name: primary_key_columns(name)
SELECT a.attname
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid
AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = to_regclass(:name) AND i.indisprimary
ORDER BY array_position(i.indkey, a.attnum);

-- name: column_defaults(name, columns)
SELECT a.attname, pg_get_expr(d.adbin, d.adrelid)
FROM pg_attribute a
LEFT JOIN pg_attrdef d
ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attrelid = to_regclass(:name)
AND a.attname = ANY(:columns)
AND a.attnum > 0 AND NOT a.attisdropped;

-- name: schema_versions()
SELECT version FROM spinach_schema;

-- =========================================================================
-- Enqueue
-- =========================================================================

-- name: insert_idempotency_token(namespace, token)<!
-- Returns the token, or None when it was already recorded.
INSERT INTO spinach_idempotency (namespace, token)
VALUES (:namespace, :token)
ON CONFLICT DO NOTHING
RETURNING token;

-- name: insert_queue_job(namespace, queue, job_id, task_name, payload)!
INSERT INTO spinach_queue_job
(namespace, queue, job_id, task_name, payload)
VALUES (:namespace, :queue, :job_id, :task_name, :payload);

-- name: insert_future_job(namespace, job_id, at, payload)!
INSERT INTO spinach_future_job
(namespace, job_id, at, payload)
VALUES (:namespace, :job_id, :at, :payload);

-- name: notify(channel, payload)!
SELECT pg_notify(:channel, :payload);

-- =========================================================================
-- Concurrency
-- =========================================================================

-- name: lock_concurrency(namespace)!
-- Fixed order avoids deadlocks between brokers.
SELECT task_name FROM spinach_concurrency
WHERE namespace = :namespace
ORDER BY task_name
FOR UPDATE;

-- name: increment_concurrency(namespace, task_name, amount)!
UPDATE spinach_concurrency
SET current_concurrency = current_concurrency + :amount
WHERE namespace = :namespace AND task_name = :task_name;

-- name: decrement_concurrency(namespace, task_name, amount)!
UPDATE spinach_concurrency
SET current_concurrency = GREATEST(current_concurrency - :amount, 0)
WHERE namespace = :namespace AND task_name = :task_name;

-- name: upsert_concurrency(namespace, task_name, max_concurrency)!
INSERT INTO spinach_concurrency
(namespace, task_name, max_concurrency, current_concurrency)
VALUES (:namespace, :task_name, :max_concurrency, 0)
ON CONFLICT (namespace, task_name) DO UPDATE
SET max_concurrency = EXCLUDED.max_concurrency;

-- name: delete_concurrency_not_in(namespace, task_names)!
DELETE FROM spinach_concurrency
WHERE namespace = :namespace AND NOT (task_name = ANY(:task_names));

-- name: delete_all_concurrency(namespace)!
DELETE FROM spinach_concurrency WHERE namespace = :namespace;

-- =========================================================================
-- Claiming jobs
-- =========================================================================

-- name: claim_next_job(namespace, queue, skipped)^
-- Next queued job: (payload, max_concurrency, current_concurrency).
-- `skipped` holds job ids already passed over for concurrency limits.
SELECT q.payload, c.max_concurrency, c.current_concurrency
FROM spinach_queue_job AS q
LEFT JOIN spinach_concurrency AS c
ON c.namespace = q.namespace
AND c.task_name = q.task_name
WHERE q.namespace = :namespace AND q.queue = :queue
AND NOT (q.job_id = ANY(:skipped::uuid[]))
ORDER BY q.position
LIMIT 1
FOR UPDATE OF q SKIP LOCKED;

-- name: delete_queue_job(namespace, job_id)!
DELETE FROM spinach_queue_job
WHERE namespace = :namespace AND job_id = :job_id;

-- name: insert_running_job(namespace, broker_id, job_id, task_name, queue, max_retries, retries, payload)!
INSERT INTO spinach_running_job (
    namespace, broker_id, job_id, task_name, queue,
    max_retries, retries, payload
) VALUES (
    :namespace, :broker_id, :job_id, :task_name, :queue,
    :max_retries, :retries, :payload
);

-- name: delete_running_job(namespace, broker_id, job_id)!
DELETE FROM spinach_running_job
WHERE namespace = :namespace AND broker_id = :broker_id
AND job_id = :job_id;

-- name: is_queue_empty(namespace, queue)$
SELECT NOT EXISTS (
    SELECT 1 FROM spinach_queue_job
    WHERE namespace = :namespace AND queue = :queue
);

-- =========================================================================
-- Future jobs
-- =========================================================================

-- name: next_future_job_payload(namespace)$
SELECT payload FROM spinach_future_job
WHERE namespace = :namespace
ORDER BY at
LIMIT 1;

-- name: select_due_future_jobs(namespace, due)
-- (job_id, payload), locked until the transaction ends.
SELECT job_id, payload FROM spinach_future_job
WHERE namespace = :namespace AND at <= :due
ORDER BY at
LIMIT 1000
FOR UPDATE;

-- name: delete_future_job(namespace, job_id)!
DELETE FROM spinach_future_job
WHERE namespace = :namespace AND job_id = :job_id;

-- =========================================================================
-- Periodic tasks
-- =========================================================================

-- name: select_periodic_tasks_for_update(namespace)
-- (name, periodicity_seconds, payload)
SELECT name, periodicity_seconds, payload
FROM spinach_periodic_task
WHERE namespace = :namespace
ORDER BY name
FOR UPDATE;

-- name: select_due_periodic_tasks(namespace, due, max_tasks)
-- (name, payload, periodicity_seconds)
SELECT name, payload, periodicity_seconds
FROM spinach_periodic_task
WHERE namespace = :namespace AND next_at <= :due
ORDER BY next_at
LIMIT :max_tasks
FOR UPDATE;

-- name: insert_periodic_task(namespace, name, periodicity_seconds, next_at, payload)!
INSERT INTO spinach_periodic_task
(namespace, name, periodicity_seconds, next_at, payload)
VALUES (:namespace, :name, :periodicity_seconds, :next_at, :payload);

-- name: update_periodic_task_schedule(namespace, name, periodicity_seconds, next_at, payload)!
UPDATE spinach_periodic_task
SET periodicity_seconds = :periodicity_seconds,
    next_at = :next_at,
    payload = :payload
WHERE namespace = :namespace AND name = :name;

-- name: update_periodic_task_payload(namespace, name, payload)!
UPDATE spinach_periodic_task SET payload = :payload
WHERE namespace = :namespace AND name = :name;

-- name: advance_periodic_task(namespace, name, next_at)!
UPDATE spinach_periodic_task SET next_at = :next_at
WHERE namespace = :namespace AND name = :name;

-- name: delete_periodic_task(namespace, name)!
DELETE FROM spinach_periodic_task
WHERE namespace = :namespace AND name = :name;

-- name: list_periodic_tasks(namespace)
-- (next_at, name)
SELECT next_at, name FROM spinach_periodic_task
WHERE namespace = :namespace
ORDER BY next_at, name;

-- name: next_periodic_at(namespace)$
SELECT next_at FROM spinach_periodic_task
WHERE namespace = :namespace
ORDER BY next_at
LIMIT 1;

-- =========================================================================
-- Brokers
-- =========================================================================

-- name: upsert_broker(namespace, broker_id, last_seen_at, info)!
INSERT INTO spinach_broker
(namespace, broker_id, last_seen_at, info)
VALUES (:namespace, :broker_id, :last_seen_at, :info)
ON CONFLICT (namespace, broker_id) DO UPDATE
SET last_seen_at = EXCLUDED.last_seen_at,
    info = EXCLUDED.info;

-- name: list_broker_infos(namespace)
SELECT info FROM spinach_broker WHERE namespace = :namespace;

-- name: find_dead_brokers(namespace, cutoff)
-- Broker ids, as text, not seen since `cutoff`.
SELECT broker_id::text FROM spinach_broker
WHERE namespace = :namespace AND last_seen_at <= :cutoff
ORDER BY last_seen_at
LIMIT 10;

-- name: lock_broker(namespace, broker_id)!
SELECT broker_id FROM spinach_broker
WHERE namespace = :namespace AND broker_id = :broker_id
FOR UPDATE;

-- name: delete_broker(namespace, broker_id)!
DELETE FROM spinach_broker
WHERE namespace = :namespace AND broker_id = :broker_id;

-- name: select_running_jobs_of_broker(namespace, broker_id)
-- Payloads of jobs a (dead) broker was running.
SELECT payload FROM spinach_running_job
WHERE namespace = :namespace AND broker_id = :broker_id
ORDER BY task_name, job_id
FOR UPDATE;

-- name: delete_running_jobs_of_broker(namespace, broker_id)!
DELETE FROM spinach_running_job
WHERE namespace = :namespace AND broker_id = :broker_id;

-- name: purge_idempotency_tokens(namespace)!
DELETE FROM spinach_idempotency
WHERE namespace = :namespace
AND created_at < statement_timestamp() - interval '1 hour';

-- =========================================================================
-- Flush
-- =========================================================================

-- name: flush(namespace)!
-- Delete everything in a namespace.  Implemented as a CTE for aiosql reasons.
WITH
queue_jobs AS (DELETE FROM spinach_queue_job WHERE namespace = :namespace),
future_jobs AS (DELETE FROM spinach_future_job WHERE namespace = :namespace),
running_jobs AS (DELETE FROM spinach_running_job WHERE namespace = :namespace),
brokers AS (DELETE FROM spinach_broker WHERE namespace = :namespace),
periodic_tasks AS (DELETE FROM spinach_periodic_task WHERE namespace = :namespace),
concurrency AS (DELETE FROM spinach_concurrency WHERE namespace = :namespace)
DELETE FROM spinach_idempotency WHERE namespace = :namespace;
