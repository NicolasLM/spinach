from contextlib import contextmanager
import contextvars
from datetime import datetime, timezone
import functools
import json
from logging import getLogger
import math
from os import path
import re
import threading
from typing import Iterable, List, Optional, Tuple
import uuid

try:
    import aiosql
    import psycopg
    from psycopg_pool import ConnectionPool
except ImportError:  # pragma: no cover
    aiosql = None
    psycopg = None
    ConnectionPool = None

from ..brokers.base import Broker, generate_idempotency_token
from ..const import DEFAULT_ENQUEUE_JOB_RETRIES
from ..job import Job, JobStatus, advance_job_status
from ..task import Task
from ..utils import call_with_retry, run_forever


logger = getLogger('spinach.broker')
here = path.abspath(path.dirname(__file__))

_SCHEMA_VERSION = 1
# (column, format_type, not_null). format_type is what Postgres reports.
_SCHEMA_COLUMNS = {
    'spinach_schema': (
        ('version', 'integer', True),
    ),
    'spinach_queue_job': (
        ('namespace', 'text', True),
        ('queue', 'text', True),
        ('position', 'bigint', True),
        ('job_id', 'uuid', True),
        ('task_name', 'text', True),
        ('payload', 'text', True),
    ),
    'spinach_future_job': (
        ('namespace', 'text', True),
        ('job_id', 'uuid', True),
        ('at', 'timestamp with time zone', True),
        ('payload', 'text', True),
    ),
    'spinach_running_job': (
        ('namespace', 'text', True),
        ('broker_id', 'uuid', True),
        ('job_id', 'uuid', True),
        ('task_name', 'text', True),
        ('queue', 'text', True),
        ('max_retries', 'integer', True),
        ('retries', 'integer', True),
        ('payload', 'text', True),
    ),
    'spinach_broker': (
        ('namespace', 'text', True),
        ('broker_id', 'uuid', True),
        ('last_seen_at', 'bigint', True),
        ('info', 'text', True),
    ),
    'spinach_periodic_task': (
        ('namespace', 'text', True),
        ('name', 'text', True),
        ('periodicity_seconds', 'integer', True),
        ('next_at', 'timestamp with time zone', True),
        ('payload', 'text', True),
    ),
    'spinach_concurrency': (
        ('namespace', 'text', True),
        ('task_name', 'text', True),
        ('max_concurrency', 'integer', True),
        ('current_concurrency', 'integer', True),
    ),
    'spinach_idempotency': (
        ('namespace', 'text', True),
        ('token', 'text', True),
        ('created_at', 'timestamp with time zone', True),
    ),
}
_SCHEMA_KEYS = {
    'spinach_schema': ('version',),
    'spinach_queue_job': ('namespace', 'job_id'),
    'spinach_future_job': ('namespace', 'job_id'),
    'spinach_running_job': ('namespace', 'broker_id', 'job_id'),
    'spinach_broker': ('namespace', 'broker_id'),
    'spinach_periodic_task': ('namespace', 'name'),
    'spinach_concurrency': ('namespace', 'task_name'),
    'spinach_idempotency': ('namespace', 'token'),
}
_VERIFIED_SSLMODE = 'verify-full'
# Tables a joined enqueue writes. Their OIDs must match the broker.
_JOINED_TABLES = ('spinach_queue_job', 'spinach_future_job')
_POSITION_DEFAULT = re.compile(
    r"^nextval\('(?:[A-Za-z_][A-Za-z0-9_]*\.)?"
    r"spinach_queue_job_position_seq'::regclass\)$"
)
_REQUIRED_DEFAULTS = {
    'spinach_queue_job': {
        'position': (
            _POSITION_DEFAULT,
            "nextval('spinach_queue_job_position_seq'::regclass)",
        ),
    },
    'spinach_idempotency': {
        'created_at': (re.compile(r'^now\(\)$'), 'now()'),
    },
}
_NOTIFY_CHANNEL = 'spinach_notify'

# Value is (id(broker), connection). A new thread starts empty, so the
# arbiter and the result notifier do not inherit a request transaction.
_joined = contextvars.ContextVar('spinach_pg_joined', default=None)


@functools.lru_cache(maxsize=None)
def _sql():
    return aiosql.from_path(
        path.join(here, 'postgres_scripts', 'postgres_queries.sql'), 'psycopg')


@functools.lru_cache(maxsize=None)
def _schema():
    return aiosql.from_path(
        path.join(here, 'postgres_scripts', 'postgres_schema.sql'), 'psycopg')


def _cluster_identity(conn):
    """The database cluster and database name this session is in.

    ``system_identifier`` is assigned at initdb and stays with that data
    directory. Two connections match when they are the same cluster and
    the same database, whatever host string or socket they used.
    """
    row = _sql().cluster_identity(conn)
    return (int(row[0]), row[1])


def _connection_sslmode(conn):
    """The sslmode libpq stored, or '' when it was left implicit."""
    info = getattr(conn, 'info', None)
    getter = getattr(info, 'get_parameters', None)
    if getter is None:
        return ''
    params = getter() or {}
    return str(params.get('sslmode') or '').lower()


def _assert_schema(conn):
    """Reject tables that unqualified queries would not actually use.

    ``to_regclass`` follows ``search_path``, so a stub in an earlier
    schema cannot hide a different table later on the path.
    """
    for table, expected in _SCHEMA_COLUMNS.items():
        _assert_table(conn, table, expected, _SCHEMA_KEYS[table])
    rows = list(_sql().schema_versions(conn))
    versions = {int(row[0]) for row in rows}
    if versions != {_SCHEMA_VERSION}:
        raise RuntimeError(
            'spinach_schema version must be %s' % _SCHEMA_VERSION
        )


def _assert_table(conn, table, expected, primary_key):
    relation = _sql().regclass(conn, name=table)
    if relation is None:
        raise RuntimeError(
            'spinach schema is missing %s. Apply '
            'spinach/brokers/postgres_scripts/postgres_schema.sql' % table
        )
    rows = list(_sql().table_columns(conn, name=table))
    found = {
        name: (typ, bool(not_null))
        for name, typ, not_null in rows
    }
    for name, typ, not_null in expected:
        if found.get(name) != (typ, not_null):
            raise RuntimeError(
                'spinach schema column %s.%s must be %s'
                % (table, name, typ)
            )
    key_rows = list(_sql().primary_key_columns(conn, name=table))
    got = tuple(row[0] for row in key_rows)
    if got != primary_key:
        raise RuntimeError(
            'spinach schema primary key for %s must be (%s)'
            % (table, ', '.join(primary_key))
        )
    _assert_defaults(conn, table)


def _assert_defaults(conn, table):
    """Reject defaults that the INSERT statements rely on."""
    required = _REQUIRED_DEFAULTS.get(table)
    if not required:
        return
    rows = list(
        _sql().column_defaults(conn, name=table, columns=list(required)))
    found = {name: expr for name, expr in rows}
    for column, (pattern, expected) in required.items():
        expr = found.get(column)
        if expr is None or pattern.match(expr) is None:
            raise RuntimeError(
                'spinach schema column %s.%s must default to %s'
                % (table, column, expected)
            )


def _relation_ids(conn):
    """OIDs of the tables an unqualified joined enqueue would write."""
    found = {}
    for table in _JOINED_TABLES:
        oid = _sql().relation_oid(conn, name=table)
        found[table] = None if oid is None else int(oid)
    return found


def _ssl_in_use(conn):
    # psycopg exposes libpq's PQsslInUse on the pgconn object.
    pgconn = getattr(conn, 'pgconn', None)
    if pgconn is not None and hasattr(pgconn, 'ssl_in_use'):
        return bool(pgconn.ssl_in_use)
    return False


def _tls_is_verified(conn):
    """True when the session is TLS and the server certificate was checked."""
    return (
        _ssl_in_use(conn)
        and _connection_sslmode(conn) == _VERIFIED_SSLMODE
    )


def _raw_connection(connection):
    """Return the psycopg connection behind a connection or cursor."""
    if connection is None:
        raise RuntimeError(
            'join_transaction requires a psycopg connection'
        )
    if getattr(connection, 'info', None) is not None and hasattr(
            connection, 'execute'):
        return connection
    inner = getattr(connection, 'connection', None)
    if inner is not None and getattr(inner, 'info', None) is not None:
        return inner
    raise RuntimeError(
        'join_transaction requires a psycopg Connection or Cursor'
    )


def _score_time(score):
    return datetime.fromtimestamp(int(score), tz=timezone.utc)


def _job_due(job: Job) -> datetime:
    # Due time is the job timestamp truncated to seconds, plus one second,
    # so the job does not start before its real timestamp.
    return _score_time(int(job.at.timestamp()) + 1)


def django_connection(alias='default'):
    """Return the open psycopg connection for a Django database alias.

    Call this inside ``transaction.atomic()``. Django is imported only when
    this function runs.
    """
    try:
        from django.db import connections
    except ImportError as exc:
        raise RuntimeError('django_connection requires Django') from exc
    wrapper = connections[alias]
    if wrapper.connection is None:
        wrapper.ensure_connection()
    raw = wrapper.connection
    if raw is None:
        raise RuntimeError('Django database connection is not open')
    return raw


def sqlalchemy_connection(session):
    """Return the psycopg connection behind a SQLAlchemy session.

    SQLAlchemy is not imported. The session must already be bound.
    """
    try:
        raw = session.connection().driver_connection
    except AttributeError as exc:
        raise RuntimeError(
            'SQLAlchemy session does not expose a driver connection'
        ) from exc
    if raw is None:
        raise RuntimeError(
            'SQLAlchemy driver connection is not available'
        )
    return raw


class PostgresBroker(Broker):
    """Postgres-backed broker.

    :arg dsn: libpq connection string. Required. Read it from the
        environment or a secrets manager.
    :arg enqueue_job_max_retries: retries for a dropped pooled enqueue.
    :arg ensure_schema: create tables on startup when True.
    :arg require_ssl: when True (the default), require TLS with
        ``sslmode=verify-full``. Set this to False only for a local
        server that has no TLS.

    ``close()`` releases the connection pool. ``stop()`` leaves the pool
    open so ``flush()`` can still run.
    """

    supports_join_transaction = True
    must_stop_periodicity = 1
    broker_dead_threshold_seconds = 1800

    def __init__(
            self,
            dsn: str,
            enqueue_job_max_retries: int = DEFAULT_ENQUEUE_JOB_RETRIES,
            ensure_schema: bool = True,
            require_ssl: bool = True):
        super().__init__()
        if dsn is None or not str(dsn).strip():
            raise ValueError('dsn is required')
        if aiosql is None or psycopg is None or ConnectionPool is None:
            raise ImportError(
                'PostgresBroker requires the postgres extra: '
                'pip install spinach[postgres]'
            )
        self._dsn = dsn
        self.enqueue_job_max_retries = enqueue_job_max_retries
        self._ensure_schema = ensure_schema
        self._require_ssl = require_ssl
        self._server_identity = None
        self._relation_ids = None
        self._reset()
        self._pool = ConnectionPool(
            self._dsn,
            min_size=1,
            max_size=4,
            num_workers=1,
            kwargs={'autocommit': False},
            open=True,
        )
        try:
            self._prepare_database()
        except Exception:
            self._pool.close()
            self._pool = None
            raise

    def _reset(self):
        self._subscriber_thread = None
        self._must_stop = threading.Event()
        self._number_periodic_tasks = 0

    def _check_ssl(self, conn):
        if self._require_ssl and not _tls_is_verified(conn):
            raise RuntimeError(
                'Postgres connection must use sslmode=verify-full'
            )

    def _prepare_database(self):
        with self._pool.connection() as conn:
            self._check_ssl(conn)
            self._server_identity = _cluster_identity(conn)
            if self._ensure_schema:
                self._apply_schema(conn)
            _assert_schema(conn)
            self._relation_ids = _relation_ids(conn)

    def _apply_schema(self, conn):
        _sql().lock_schema(conn)
        try:
            _schema().apply_schema(conn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            # The advisory lock is session-scoped, so unlock after the
            # schema transaction commits or rolls back.
            try:
                conn.rollback()
            except Exception:
                logger.exception(
                    'Ignoring rollback before schema unlock'
                )
            _sql().unlock_schema(conn)
            conn.commit()

    @contextmanager
    def _transaction(self):
        # The pool context commits on success and rolls back on error.
        with self._pool.connection() as conn:
            yield conn

    def check_connection(self, connection):
        """Raise unless ``connection`` can join this broker's database."""
        raw = _raw_connection(connection)
        if raw.closed:
            raise RuntimeError('joined connection is closed')
        if raw.autocommit:
            raise RuntimeError(
                'joined connection must not be in autocommit mode'
            )
        self._check_ssl(raw)
        if _cluster_identity(raw) != self._server_identity:
            raise RuntimeError(
                'joined connection must use the same database cluster '
                'as the PostgresBroker'
            )
        if _relation_ids(raw) != self._relation_ids:
            raise RuntimeError(
                'joined connection does not see the broker Spinach tables'
            )

    @contextmanager
    def join_transaction(self, connection):
        """Bind later enqueues in this context to ``connection``.

        The broker does not commit or roll back ``connection``.
        """
        self.check_connection(connection)
        current = _joined.get()
        if current is not None:
            broker_id, existing = current
            if broker_id != id(self) or existing is not connection:
                raise RuntimeError(
                    'a different transaction is already joined'
                )
            yield
            return
        token = _joined.set((id(self), connection))
        try:
            yield
        finally:
            _joined.reset(token)

    def joined_connection(self):
        current = _joined.get()
        if current is None:
            return None
        broker_id, connection = current
        if broker_id != id(self):
            raise RuntimeError(
                'join_transaction is active for a different broker'
            )
        return connection

    def enqueue_in_transaction(self, jobs: Iterable[Job], connection):
        """Insert jobs on ``connection`` without committing it."""
        jobs = list(jobs)
        if not jobs:
            return
        self.check_connection(connection)
        self._mark_initial_status(jobs)
        self._write_jobs(
            connection, jobs, from_failure=False, token=None
        )

    def enqueue_jobs(self, jobs: Iterable[Job], from_failure: bool = False):
        jobs = list(jobs)
        if not jobs:
            return
        self._mark_initial_status(jobs)
        token = generate_idempotency_token()
        call_with_retry(
            self._enqueue_pooled,
            (psycopg.OperationalError,),
            self.enqueue_job_max_retries,
            logger,
            jobs,
            from_failure,
            token,
        )

    def _enqueue_pooled(self, jobs, from_failure, token):
        with self._transaction() as conn:
            self._write_jobs(conn, jobs, from_failure, token)

    def _mark_initial_status(self, jobs):
        for job in jobs:
            if job.should_start:
                job.status = JobStatus.QUEUED
            else:
                job.status = JobStatus.WAITING

    def _write_jobs(self, conn, jobs, from_failure, token):
        if token is not None:
            stored = _sql().insert_idempotency_token(
                conn, namespace=self.namespace, token=token
            )
            if stored is None:
                logger.info(
                    'Enqueue not reprocessed because it was already '
                    'processed once'
                )
                return
        if from_failure:
            self._lock_concurrency(conn)
            self._decrement_concurrency(conn, jobs)
        for job in jobs:
            if job.status is JobStatus.QUEUED:
                self._insert_queue(conn, job)
            else:
                _sql().insert_future_job(
                    conn, namespace=self.namespace, job_id=job.id,
                    at=_job_due(job), payload=job.serialize(),
                )
            if from_failure:
                self._delete_running(conn, self._id, job.id)
        # NOTIFY is part of this transaction. A pooled enqueue commits it
        # before returning. A joined enqueue delivers it when the caller
        # commits. An enqueue of future jobs notifies as well.
        self._notify(conn)

    def _insert_queue(self, conn, job: Job):
        _sql().insert_queue_job(
            conn, namespace=self.namespace, queue=job.queue,
            job_id=job.id, task_name=job.task_name,
            payload=job.serialize(),
        )

    def _insert_running(self, conn, job: Job):
        _sql().insert_running_job(
            conn, namespace=self.namespace, broker_id=self._id,
            job_id=job.id, task_name=job.task_name, queue=job.queue,
            max_retries=job.max_retries, retries=job.retries,
            payload=job.serialize(),
        )

    def _delete_running(self, conn, broker_id, job_id):
        _sql().delete_running_job(
            conn, namespace=self.namespace, broker_id=broker_id,
            job_id=job_id,
        )

    def _notify(self, conn):
        _sql().notify(
            conn, channel=_NOTIFY_CHANNEL, payload=self.namespace
        )

    def _lock_concurrency(self, conn):
        _sql().lock_concurrency(conn, namespace=self.namespace)

    def _decrement_concurrency(self, conn, jobs):
        counts = {}
        for job in jobs:
            counts[job.task_name] = counts.get(job.task_name, 0) + 1
        for name in sorted(counts):
            _sql().decrement_concurrency(
                conn, namespace=self.namespace, task_name=name,
                amount=counts[name],
            )

    def _increment_concurrency(self, conn, task_names):
        counts = {}
        for name in task_names:
            counts[name] = counts.get(name, 0) + 1
        for name in sorted(counts):
            _sql().increment_concurrency(
                conn, namespace=self.namespace, task_name=name,
                amount=counts[name],
            )

    def get_jobs_from_queue(self, queue: str, max_jobs: int) -> List[Job]:
        if max_jobs < 1:
            return []
        with self._transaction() as conn:
            return self._claim_jobs(conn, queue, max_jobs)

    def _claim_jobs(self, conn, queue, max_jobs):
        self._lock_concurrency(conn)
        jobs = []
        tracked = []
        skipped = []
        while len(jobs) < max_jobs:
            row = _sql().claim_next_job(
                conn, namespace=self.namespace, queue=queue,
                skipped=skipped,
            )
            if row is None:
                break
            payload, maximum, current = row
            job = Job.deserialize(payload)
            already = tracked.count(job.task_name)
            if maximum is not None and current + already >= maximum:
                # Leave the row queued. A later eligible job in this batch
                # can still be claimed.
                skipped.append(job.id)
                continue
            job.status = JobStatus.RUNNING
            self._insert_running(conn, job)
            self._delete_queue_job(conn, job.id)
            jobs.append(job)
            if maximum is not None:
                tracked.append(job.task_name)
        self._increment_concurrency(conn, tracked)
        return jobs

    def _delete_queue_job(self, conn, job_id):
        _sql().delete_queue_job(
            conn, namespace=self.namespace, job_id=job_id
        )

    def remove_job_from_running(self, job: Job):
        with self._transaction() as conn:
            self._lock_concurrency(conn)
            self._decrement_concurrency(conn, [job])
            self._delete_running(conn, self._id, job.id)
        self._something_happened.set()

    def is_queue_empty(self, queue: str) -> bool:
        with self._transaction() as conn:
            empty = _sql().is_queue_empty(
                conn, namespace=self.namespace, queue=queue
            )
        return bool(empty)

    def _get_next_future_job(self) -> Optional[Job]:
        with self._transaction() as conn:
            payload = _sql().next_future_job_payload(
                conn, namespace=self.namespace
            )
        if payload is None:
            return None
        return Job.deserialize(payload)

    def move_future_jobs(self) -> int:
        now_score = int(math.ceil(
            datetime.now(timezone.utc).timestamp()
        ))
        now_dt = _score_time(now_score)
        with self._transaction() as conn:
            info = self._get_broker_info()
            _sql().upsert_broker(
                conn, namespace=self.namespace, broker_id=self._id,
                last_seen_at=info['last_seen_at'],
                info=json.dumps(info),
            )
            _sql().purge_idempotency_tokens(
                conn, namespace=self.namespace
            )
            cutoff = now_score - self.broker_dead_threshold_seconds
            dead_ids = [
                row[0] for row in _sql().find_dead_brokers(
                    conn, namespace=self.namespace, cutoff=cutoff
                )
            ]
            moved = self._move_due_future_jobs(conn, now_dt)
            moved += self._fire_periodic(conn, now_score, now_dt)
            if moved:
                self._notify(conn)
        self._requeue_detected_dead_brokers(dead_ids)
        return moved

    def _move_due_future_jobs(self, conn, now_dt):
        rows = list(_sql().select_due_future_jobs(
            conn, namespace=self.namespace, due=now_dt
        ))
        for job_id, payload in rows:
            job = Job.deserialize(payload)
            job.status = JobStatus.QUEUED
            self._insert_queue(conn, job)
            _sql().delete_future_job(
                conn, namespace=self.namespace, job_id=job_id
            )
        return len(rows)

    def _fire_periodic(self, conn, now_score, now_dt):
        if self._number_periodic_tasks < 1:
            return 0
        rows = list(_sql().select_due_periodic_tasks(
            conn, namespace=self.namespace, due=now_dt,
            max_tasks=self._number_periodic_tasks,
        ))
        at = _score_time(now_score)
        for name, payload, period in rows:
            task = json.loads(payload)
            job = Job(name, task['queue'], at, task['max_retries'])
            job.status = JobStatus.QUEUED
            self._insert_queue(conn, job)
            next_at = _score_time(int(now_score) + int(period))
            _sql().advance_periodic_task(
                conn, namespace=self.namespace, name=name,
                next_at=next_at,
            )
        return len(rows)

    def _requeue_detected_dead_brokers(self, dead_ids):
        if not dead_ids:
            return
        known = {
            item['id']: item for item in self.get_all_brokers()
        }
        for dead_id in dead_ids:
            if dead_id == str(self._id):
                continue
            dead_broker = known.get(dead_id) or {
                'id': dead_id,
                'name': dead_id,
            }
            logger.debug(
                'Worker %s on %s detected dead, re-enqueuing its jobs',
                dead_broker['id'], dead_broker['name'],
            )
            num, failed = self.enqueue_jobs_from_dead_broker(
                uuid.UUID(dead_id)
            )
            logger.warning(
                'Worker %s on %s marked as dead, %d jobs were re-enqueued',
                dead_broker['id'], dead_broker['name'], num,
            )
            err = Exception(
                'Worker %s died and max_retries exceeded'
                % dead_broker['name']
            )
            for payload in failed:
                advance_job_status(
                    self.namespace, Job.deserialize(payload),
                    duration=0.0, err=err,
                )

    def register_periodic_tasks(self, tasks: Iterable[Task]):
        tasks = list(tasks)
        self._number_periodic_tasks = len(tasks)
        now_score = int(self.start_at().timestamp())
        with self._transaction() as conn:
            rows = list(_sql().select_periodic_tasks_for_update(
                conn, namespace=self.namespace
            ))
            existing = {}
            for name, period, payload in rows:
                stored = json.loads(payload)
                existing[name] = (
                    int(period),
                    int(stored.get('periodicity_start') or 0),
                )
            seen = set()
            for task in tasks:
                payload = task.serialize()
                data = json.loads(payload)
                period = data['periodicity']
                if period is None:
                    raise ValueError(
                        'periodic task %s has no periodicity' % task.name
                    )
                period = int(period)
                start = int(data.get('periodicity_start') or 0)
                seen.add(task.name)
                previous = existing.get(task.name)
                if previous is None:
                    next_at = _score_time(now_score + period + start)
                    _sql().insert_periodic_task(
                        conn, namespace=self.namespace, name=task.name,
                        periodicity_seconds=period, next_at=next_at,
                        payload=payload,
                    )
                elif previous[1] != start:
                    next_at = _score_time(now_score + period + start)
                    _sql().update_periodic_task_schedule(
                        conn, namespace=self.namespace, name=task.name,
                        periodicity_seconds=period, next_at=next_at,
                        payload=payload,
                    )
                elif previous[0] != period:
                    next_at = _score_time(now_score + period)
                    _sql().update_periodic_task_schedule(
                        conn, namespace=self.namespace, name=task.name,
                        periodicity_seconds=period, next_at=next_at,
                        payload=payload,
                    )
                else:
                    _sql().update_periodic_task_payload(
                        conn, namespace=self.namespace, name=task.name,
                        payload=payload,
                    )
            for name in existing:
                if name not in seen:
                    _sql().delete_periodic_task(
                        conn, namespace=self.namespace, name=name
                    )

    def inspect_periodic_tasks(self) -> List[Tuple[int, str]]:
        with self._transaction() as conn:
            rows = list(_sql().list_periodic_tasks(
                conn, namespace=self.namespace
            ))
        return [
            (int(round(row[0].timestamp())), row[1]) for row in rows
        ]

    @property
    def next_future_periodic_delta(self) -> Optional[float]:
        with self._transaction() as conn:
            next_at = _sql().next_periodic_at(
                conn, namespace=self.namespace
            )
        if next_at is None:
            return None
        delta = (
            next_at - datetime.now(timezone.utc)
        ).total_seconds()
        if delta < 0:
            return 0
        return delta

    def set_concurrency_keys(self, tasks: Iterable[Task]):
        tasks = list(tasks)
        with self._transaction() as conn:
            self._lock_concurrency(conn)
            names = []
            for task in tasks:
                raw = json.loads(task.serialize())
                maximum = int(raw['max_concurrency'])
                if maximum == -1:
                    continue
                names.append(task.name)
                _sql().upsert_concurrency(
                    conn, namespace=self.namespace, task_name=task.name,
                    max_concurrency=maximum,
                )
            if names:
                _sql().delete_concurrency_not_in(
                    conn, namespace=self.namespace, task_names=names
                )
            else:
                _sql().delete_all_concurrency(
                    conn, namespace=self.namespace
                )

    def flush(self):
        with self._transaction() as conn:
            _sql().flush(conn, namespace=self.namespace)

    def get_all_brokers(self):
        with self._transaction() as conn:
            rows = list(_sql().list_broker_infos(
                conn, namespace=self.namespace
            ))
        return [json.loads(row[0]) for row in rows]

    def enqueue_jobs_from_dead_broker(
        self, dead_broker_id: uuid.UUID
    ) -> Tuple[int, list]:
        with self._transaction() as conn:
            return self._requeue_dead(conn, dead_broker_id)

    def _requeue_dead(self, conn, dead_broker_id):
        _sql().lock_broker(
            conn, namespace=self.namespace, broker_id=dead_broker_id
        )
        self._lock_concurrency(conn)
        rows = list(_sql().select_running_jobs_of_broker(
            conn, namespace=self.namespace, broker_id=dead_broker_id
        ))
        tracked = []
        retryable = []
        failed = []
        for (payload,) in rows:
            job = Job.deserialize(payload)
            tracked.append(job)
            if job.max_retries > 0 and job.retries < job.max_retries:
                job.retries += 1
                job.status = JobStatus.QUEUED
                retryable.append(job)
            else:
                failed.append(payload)
        self._decrement_concurrency(conn, tracked)
        for job in retryable:
            self._insert_queue(conn, job)
        _sql().delete_running_jobs_of_broker(
            conn, namespace=self.namespace, broker_id=dead_broker_id
        )
        _sql().delete_broker(
            conn, namespace=self.namespace, broker_id=dead_broker_id
        )
        if retryable:
            self._notify(conn)
        return len(retryable), failed

    def _subscriber_func(self):
        logger.debug('Postgres broker subscriber started')
        conn = psycopg.connect(self._dsn, autocommit=True)
        try:
            self._check_ssl(conn)
            # Channel name is a fixed identifier, not a bound parameter.
            # LISTEN does not accept a placeholder.
            conn.execute('LISTEN spinach_notify')
            while not self._must_stop.is_set():
                for notify in conn.notifies(
                        timeout=self.must_stop_periodicity):
                    if notify.payload == self.namespace:
                        logger.debug(
                            'Got a notification for namespace %s',
                            self.namespace,
                        )
                        self._something_happened.set()
            self._deregister()
        finally:
            conn.close()
        logger.debug('Postgres broker subscriber terminated')

    def _deregister(self):
        if self._pool is None or not self._namespace:
            return
        try:
            with self._transaction() as conn:
                _sql().delete_broker(
                    conn, namespace=self.namespace, broker_id=self._id
                )
        except Exception:
            logger.exception('Failed to deregister Postgres broker')

    def start(self):
        if self._subscriber_thread is not None:
            return
        self._subscriber_thread = threading.Thread(
            target=run_forever,
            args=(self._subscriber_func, self._must_stop, logger),
            name='{}-broker-subscriber'.format(self.namespace),
        )
        self._subscriber_thread.start()

    def stop(self):
        super().stop()
        self._must_stop.set()
        thread = self._subscriber_thread
        if thread is not None:
            thread.join()
        self._deregister()
        self._reset()

    def close(self):
        """Stop the subscriber, if it is running, and close the pool."""
        if self._subscriber_thread is not None:
            self.stop()
        pool = self._pool
        self._pool = None
        if pool is not None:
            pool.close()
