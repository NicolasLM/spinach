.. _postgres-broker:

Postgres broker
===============

:class:`spinach.brokers.postgres.PostgresBroker` stores jobs in Postgres.
Each broker call is one transaction. Application code can put its own writes
in that transaction, so the job row and the local rows commit together or
roll back together.

How to run it is in :ref:`postgres`.

Module
------

The implementation is ``spinach.brokers.postgres``. Importing ``spinach``
does not import psycopg. :meth:`PostgresBroker.__init__` raises
``ImportError`` when the ``postgres`` extra is missing.

.. code:: python

    import os

    from spinach import Engine
    from spinach.brokers.postgres import PostgresBroker

    broker = PostgresBroker(os.environ["SPINACH_POSTGRES_DSN"])
    spin = Engine(broker, namespace="prod")

Constructor arguments:

- ``dsn``: libpq connection string. Required. The application reads it from
  the environment or a secrets manager.
- ``enqueue_job_max_retries``: retries of a pooled enqueue after
  ``psycopg.OperationalError``. Default ``DEFAULT_ENQUEUE_JOB_RETRIES``.
- ``ensure_schema``: default ``True``. Startup applies
  ``spinach/brokers/postgres_schema.sql``. Production sets this to
  ``False`` and applies that file with a migration role.
- ``require_ssl``: default ``True``. The session must be using TLS and
  the DSN ``sslmode`` must be ``verify-full``. ``sslmode=require`` and
  an implicit ``prefer`` are rejected. Set this to False only for a
  local server that has no TLS.

``broker_dead_threshold_seconds`` is ``1800``. ``must_stop_periodicity`` is
``1`` second.

Co-committing with application writes
--------------------------------------

:meth:`Engine.schedule` arguments are the task's arguments. The database
connection is passed to :meth:`Engine.join_transaction`. ``schedule``,
``schedule_at``, and ``schedule_batch`` inside the block call
``enqueue_in_transaction`` on that connection.

.. code:: python

    import os

    import psycopg

    dsn = os.environ["SPINACH_POSTGRES_DSN"]

    with psycopg.connect(dsn) as conn:
        with conn.transaction():
            conn.execute(
                "INSERT INTO orders (id, sku) VALUES (%s, %s)",
                (order_id, sku),
            )
            with spin.join_transaction(conn):
                spin.schedule(process_order, order_id)
                spin.schedule(send_receipt, order_id)

``with conn.transaction()`` is the commit boundary. Spinach runs ``INSERT``
and ``pg_notify`` on the given connection. It does not call ``commit``,
``rollback``, ``close``, or ``BEGIN``, and it does not open a savepoint.
A failed statement aborts the transaction. Spinach re-raises that error and
the caller rolls back.

After ``schedule`` returns, workers still cannot see the job. Workers read
under ``READ COMMITTED``. Postgres delivers ``NOTIFY`` at commit. The
subscriber sets the local ``threading.Event`` when that notification
arrives. The joined path does not set the event itself. A rollback delivers
no notification and leaves no job row.

The active connection is a :class:`contextvars.ContextVar`,
``spinach_pg_joined``, holding ``(id(broker), connection)``. A new thread
starts empty, so the arbiter and the result notifier do not inherit a
request transaction.

Rules for the joined connection:

- It is a psycopg 3 ``Connection`` or ``Cursor``. The broker compares
  ``pg_control_system().system_identifier`` and ``current_database()``.
  The same cluster and database match, whatever host string or socket
  was used. It also compares ``to_regclass`` for ``spinach_queue_job``
  and ``spinach_future_job`` with the relations the broker resolved at
  startup. A different cluster, database, or table raises
  ``RuntimeError``.
- ``autocommit`` must be off.
- The broker uses the connection only during ``enqueue_in_transaction``.
  It does not keep the reference, and the connection must not be shared
  with another thread during that call.
- A statement error is re-raised. The broker does not retry it. Retry
  belongs to the pooled path, where Spinach owns the transaction.
- Nested ``join_transaction`` on the same broker and the same connection is
  allowed. A different connection or a different broker raises.
- :meth:`Engine.join_transaction` raises ``RuntimeError`` when
  ``supports_join_transaction`` is false.
- The result notifier calls ``enqueue_jobs(..., from_failure=True)`` on the
  pool. That call is outside the request transaction.

``django_connection(alias='default')`` returns the psycopg connection Django
has open for that alias. Call it inside ``transaction.atomic()``.
``sqlalchemy_connection(session)`` returns
``session.connection().driver_connection``. Django and SQLAlchemy are
imported only when those helpers run. Both must be using psycopg 3 against
the broker's database.

A committed request that the client retries can insert a second business
row and a second job. The idempotency token below covers a retried broker
write, not a retried application request. Uniqueness of the business action
belongs on the application row.

Job rows
--------

The payload column is :meth:`spinach.job.Job.serialize` text. ``task_name``,
``queue``, ``at``, ``max_retries``, and ``retries`` are also columns, so
statements do not parse the payload. A status or retry change writes the
columns and the payload together.

A future job's ``at`` column is ``int(job.at.timestamp()) + 1`` second, as
a ``timestamptz``. The job stays in ``spinach_future_job`` until that
instant. Comparisons use the ``datetime.now`` value passed from Python, so
tests can patch the clock. Idempotency expiry is the exception: it uses
``statement_timestamp()``.

Schema
------

``spinach/brokers/postgres_schema.sql`` is applied one statement at a time
under ``pg_advisory_lock(hashtext('spinach.schema')::bigint)``. The lock is
session-level and is unlocked after the schema transaction commits or rolls
back. Statements in that file contain no internal semicolons. The loader
strips ``--`` comments and splits on ``;``.

After the file is applied, or instead of applying it when
``ensure_schema`` is False, startup resolves each table with
``to_regclass`` and checks column types, nullability, and the primary
key. ``spinach_queue_job.position`` must default to ``nextval`` of
``spinach_queue_job_position_seq``. ``spinach_idempotency.created_at``
must default to ``now()``. It then reads ``spinach_schema.version``,
which must be ``1``. A missing table, a stub earlier on ``search_path``,
a wrong type, a missing default, or any other version raises
``RuntimeError``.

Tables, all keyed by ``namespace``:

- ``spinach_schema`` holds schema version ``1``.
- ``spinach_queue_job`` is the FIFO queue. ``position`` is a ``bigserial``.
- ``spinach_future_job`` holds jobs whose ``at`` is still ahead.
- ``spinach_running_job`` holds jobs a broker has claimed.
- ``spinach_broker`` holds ``last_seen_at`` and a JSON info blob.
- ``spinach_periodic_task`` holds the next fire time.
- ``spinach_concurrency`` holds ``max_concurrency`` and
  ``current_concurrency``.
- ``spinach_idempotency`` holds enqueue tokens.

``flush()`` deletes one namespace from each table. It does not drop tables.

Lock order for a transaction that touches more than one table:

1. ``spinach_broker`` rows, by ``broker_id``
2. ``spinach_concurrency`` rows, by ``task_name``
3. queue, future, and running rows, by ``position`` or ``job_id``

``get_jobs_from_queue``, ``remove_job_from_running``, and a
``from_failure`` enqueue lock the concurrency rows first.

Connections
-----------

Commands use a ``psycopg_pool.ConnectionPool`` with ``min_size=1``,
``max_size=4``, ``num_workers=1``, and ``autocommit=False``. The pool's
connection context commits on success and rolls back on error. Broker
methods do not open a second transaction or a savepoint inside it.

``LISTEN`` uses a dedicated autocommit connection owned by the subscriber
thread. Listen state is per session, so that connection is not taken from
the pool.

``start()`` starts the subscriber thread. ``stop()`` sets the must-stop
event, joins the subscriber, and deletes this broker's ``spinach_broker``
row. The pool stays open. ``close()`` stops the subscriber when it is
running and then closes the pool.

Notifications
-------------

The channel name is the constant ``spinach_notify``. ``LISTEN`` cannot take
a placeholder. The payload is the namespace, sent with
``pg_notify(%s, %s)``. A job body is never the payload. Postgres limits a
notification payload to 8000 bytes.

Every enqueue calls ``pg_notify``, including an enqueue that only inserts
future jobs. ``move_future_jobs`` notifies when it appends at least one
queue row. ``remove_job_from_running`` does not notify. It sets the local
``threading.Event`` so this process can fill the free worker slot.

The subscriber:

- ``LISTEN spinach_notify`` with autocommit on.
- Waits up to ``must_stop_periodicity``.
- Sets ``_something_happened`` when the payload equals this namespace.
- On ``stop()``, exits and closes the connection.

``wait_for_event`` is the base-class wait. It returns on the local event,
the next future job, or the next periodic task, whichever comes first.

Enqueue
-------

Pooled ``enqueue_jobs`` is idempotent. One UUID token is generated before
:func:`spinach.utils.call_with_retry` and reused if the call is retried.
In the same transaction:

1. ``INSERT INTO spinach_idempotency ... ON CONFLICT DO NOTHING RETURNING
   token``. No returned row means this token was already committed. The
   method logs that and writes nothing.
2. When ``from_failure`` is set, decrement ``current_concurrency`` for each
   job's task, with a floor of zero, and delete the job id from this
   broker's running rows.
3. A ``QUEUED`` job is inserted into ``spinach_queue_job``. Any other status
   is inserted into ``spinach_future_job``.
4. ``pg_notify``.

The token and the job rows commit together. A failure before commit does
not burn the token. ``move_future_jobs`` deletes tokens for this namespace
older than one hour.

``enqueue_in_transaction`` runs the insert and ``pg_notify`` with no
token and does not commit. It does not decrement concurrency and does
not delete running rows. The engine uses it while ``join_transaction``
is active.

``_dispatch_jobs`` checks ``supports_join_transaction`` on the broker
class, not on the instance, so a stand-in broker used in tests is not
probed for attributes it does not define.

Claim
-----

``get_jobs_from_queue`` locks the namespace concurrency rows, then claims
one queue row at a time:

.. code:: sql

    SELECT ... FROM spinach_queue_job AS q
    LEFT JOIN spinach_concurrency AS c
      ON c.namespace = q.namespace AND c.task_name = q.task_name
    WHERE q.namespace = %s AND q.queue = %s
      AND NOT (q.job_id = ANY(%s::uuid[]))
    ORDER BY q.position
    LIMIT 1
    FOR UPDATE OF q SKIP LOCKED

A task with no concurrency row is eligible. A tracked task is eligible
while ``current_concurrency`` plus claims already taken in this batch is
below ``max_concurrency``. A blocked row stays in the queue. Its id is
added to the skipped list so a later eligible row can be claimed in the
same call. ``max_concurrency`` of ``-1`` means the task is not tracked and
has no concurrency row.

Each claimed job is marked ``RUNNING``, inserted into
``spinach_running_job``, and deleted from ``spinach_queue_job``.
``current_concurrency`` is incremented after the claim loop, once per
claimed task.

Maintenance
-----------

``move_future_jobs`` uses ``ceil(datetime.now)`` as ``now`` and runs one
transaction:

1. Upsert this broker's info and ``last_seen_at`` (``int(time.time())``).
2. Delete idempotency rows older than one hour.
3. Read up to 10 broker ids in this namespace whose ``last_seen_at`` is at
   or before ``now - broker_dead_threshold_seconds``. The rows stay in
   place.
4. Move up to 1000 due future jobs onto their queues with status
   ``QUEUED``.
5. Fire due periodic tasks, at most ``_number_periodic_tasks`` of them.
   Each insert uses ``retries = 0`` and a new job id. ``next_at`` becomes
   ``now + periodicity``.
6. ``pg_notify`` when step 4 or 5 appended a queue row.

After commit, each dead id other than this broker is passed to
``enqueue_jobs_from_dead_broker``. Jobs that were not re-queued are passed
to :func:`spinach.job.advance_job_status`.

Periodic tasks
--------------

``register_periodic_tasks`` locks the namespace rows, then:

- A new name is inserted with ``next_at = now + periodicity``.
- An existing name with the same periodicity keeps ``next_at`` and replaces
  the payload.
- An existing name with a different periodicity replaces the payload and
  resets ``next_at``.
- A stored name absent from the new list is deleted.

``inspect_periodic_tasks`` returns ``(unix_timestamp, name)`` ordered by
``next_at``. The timestamp is ``int(round(next_at.timestamp()))``.
``next_future_periodic_delta`` is the seconds until the earliest
``next_at``, ``0`` when that time has passed, or ``None`` when the
namespace has no periodic rows.

Concurrency
-----------

``set_concurrency_keys`` receives serialized tasks. ``max_concurrency`` of
``-1`` is skipped. Every other task is upserted. ``current_concurrency`` is
set to ``0`` only on insert. An update changes ``max_concurrency`` and
leaves the live counter. Rows whose task name is absent from that set are
deleted. An empty task list deletes every concurrency row in the namespace.

``remove_job_from_running`` locks the counters, decrements with a floor of
zero, deletes the running row, and sets the local event.

Dead brokers
------------

``enqueue_jobs_from_dead_broker`` is one transaction:

1. Lock the broker row and its running jobs.
2. Every running job decrements ``current_concurrency`` when the task is
   tracked, including a job that will not be re-queued.
3. A running job with ``max_retries > 0`` and ``retries < max_retries``
   gains one retry, becomes ``QUEUED``, and is inserted into the queue.
   There is no further delay. The dead-broker threshold is the wait.
4. Any other running job is returned in the failed list as the original
   payload.
5. The running rows and the broker row are deleted.
6. ``pg_notify`` when at least one job was re-queued.
7. Return ``(number_requeued, failed_payloads)``.

A second call for the same broker id finds no rows and returns ``(0, [])``.
Row locks keep concurrent callers from appending the same job twice.

Tests
-----

Postgres tests run when ``SPINACH_TEST_POSTGRES_DSN`` is set. CI and
``tests/docker-compose.yml`` run Postgres 16 with trust authentication.
Production connections use SCRAM-SHA-256 and ``sslmode=verify-full``.

Security
--------

The worker role is not a superuser and does not own the tables. It has
``SELECT``, ``INSERT``, ``UPDATE``, and ``DELETE`` on the Spinach tables,
and ``USAGE`` on ``spinach_queue_job_position_seq``. Schema changes belong
to a migration role when ``ensure_schema`` is ``False``.

The application role used with ``join_transaction`` has ``INSERT`` on
``spinach_queue_job`` and ``spinach_future_job``, and ``USAGE`` on the
position sequence. Job bodies go through ``Job.serialize()``. That role
has no ``DELETE``, no access to running-job rows, and no ownership of the
tables.

Spinach does not open the joined connection and does not change its SSL
mode. In production the application opens it with SCRAM-SHA-256 and
``sslmode=verify-full``. ``require_ssl`` defaults to True and rejects a
pooled or joined session unless that mode is in effect and TLS is in
use. The test database passes False because it uses trust authentication.

Namespace, task, and job values are bound parameters. They are never
interpolated into table names, channel names, or SQL fragments.

Server setting ``password_encryption = scram-sha-256``. MD5 password
authentication stays disabled. A connection that leaves the host sets
``sslmode=verify-full`` and ``sslrootcert``. The CA file comes from the
environment. Certificate PEM data stays out of Python source.

Before a server certificate is trusted, ``openssl x509 -text -noout`` must
show validity dates that include the present, an RSA key of at least 2048
bits or an ECDSA P-256 key, and a SHA-256 or stronger signature. A
self-signed certificate is for local tests that configure that trust on
purpose.

Logs contain job ids, task names, and error classes. The DSN is not
logged, because a URL can contain a password.
