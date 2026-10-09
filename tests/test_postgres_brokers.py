from datetime import datetime, timedelta, timezone
import os
import threading
from unittest.mock import patch
import uuid

import pytest

try:
    import psycopg
except ImportError:
    psycopg = None

from spinach import Engine
from spinach.brokers.postgres import (
    PostgresBroker, _tls_is_verified, django_connection,
    sqlalchemy_connection,
)
from spinach.job import Job, JobStatus
from spinach.task import Task


def _dsn():
    return os.environ.get('SPINACH_TEST_POSTGRES_DSN')


def _open(**kwargs):
    # The test server has no TLS. Production leaves require_ssl on.
    kwargs.setdefault('require_ssl', False)
    return PostgresBroker(_dsn(), **kwargs)


pytestmark = pytest.mark.skipif(
    psycopg is None or not _dsn(),
    reason='Postgres tests need psycopg and SPINACH_TEST_POSTGRES_DSN',
)


def test_dsn_is_required():
    with pytest.raises(ValueError, match='dsn is required'):
        PostgresBroker('  ')


class _SslInfo:
    def __init__(self, mode):
        self._mode = mode

    def get_parameters(self):
        if self._mode is None:
            return {}
        return {'sslmode': self._mode}


class _SslConn:
    def __init__(self, mode, ssl=True):
        self.info = _SslInfo(mode)

        class Pg:
            ssl_in_use = ssl

        self.pgconn = Pg()


def test_tls_is_verified_only_for_verify_full():
    assert _tls_is_verified(_SslConn('verify-full'))
    assert not _tls_is_verified(_SslConn('verify-full', ssl=False))
    assert not _tls_is_verified(_SslConn('verify-ca'))
    assert not _tls_is_verified(_SslConn('require'))
    assert not _tls_is_verified(_SslConn('prefer'))
    assert not _tls_is_verified(_SslConn(None))


def test_require_ssl_rejects_a_plaintext_session():
    plaintext = psycopg.conninfo.make_conninfo(_dsn(), sslmode='disable')
    with pytest.raises(RuntimeError, match='verify-full'):
        PostgresBroker(plaintext)


def test_require_ssl_rejects_unverified_tls():
    dsn = psycopg.conninfo.make_conninfo(_dsn(), sslmode='require')
    try:
        conn = psycopg.connect(dsn, connect_timeout=3)
    except psycopg.OperationalError:
        pytest.skip('server does not accept sslmode=require')
    else:
        conn.close()
    with pytest.raises(RuntimeError, match='verify-full'):
        PostgresBroker(dsn)


def test_join_transaction_accepts_another_address_of_the_same_cluster():
    via_ip = psycopg.conninfo.make_conninfo(_dsn(), host='127.0.0.1')
    via_name = psycopg.conninfo.make_conninfo(_dsn(), host='localhost')
    broker = PostgresBroker(via_ip, require_ssl=False)
    try:
        with psycopg.connect(via_name) as conn:
            with broker.join_transaction(conn):
                pass
    finally:
        broker.close()


def test_startup_checks_the_schema():
    name = 'spinach_schemacheck'
    with psycopg.connect(_dsn(), autocommit=True) as admin:
        admin.execute(
            'DROP DATABASE IF EXISTS spinach_schemacheck'
        )
        admin.execute('CREATE DATABASE spinach_schemacheck')
    dsn = psycopg.conninfo.make_conninfo(_dsn(), dbname=name)
    try:
        with pytest.raises(RuntimeError, match='schema'):
            PostgresBroker(dsn, ensure_schema=False, require_ssl=False)
        created = PostgresBroker(dsn, require_ssl=False)
        created.close()
        checked = PostgresBroker(
            dsn, ensure_schema=False, require_ssl=False,
        )
        checked.close()
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(
                'ALTER TABLE spinach_queue_job '
                'ALTER COLUMN position DROP DEFAULT'
            )
        with pytest.raises(RuntimeError, match='spinach_queue_job.position'):
            PostgresBroker(dsn, ensure_schema=False, require_ssl=False)
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(
                'ALTER TABLE spinach_queue_job ALTER COLUMN position '
                "SET DEFAULT nextval("
                "'spinach_queue_job_position_seq'::regclass)"
            )
            conn.execute(
                'ALTER TABLE spinach_idempotency '
                'ALTER COLUMN created_at DROP DEFAULT'
            )
        with pytest.raises(
            RuntimeError, match='spinach_idempotency.created_at',
        ):
            PostgresBroker(dsn, ensure_schema=False, require_ssl=False)
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(
                'ALTER TABLE spinach_idempotency '
                'ALTER COLUMN created_at SET DEFAULT now()'
            )
            conn.execute('UPDATE spinach_schema SET version = 2')
        with pytest.raises(RuntimeError, match='version'):
            PostgresBroker(dsn, ensure_schema=False, require_ssl=False)
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute('UPDATE spinach_schema SET version = 1')
            conn.execute(
                'ALTER TABLE spinach_queue_job '
                'ALTER COLUMN queue TYPE integer USING 0'
            )
        with pytest.raises(RuntimeError, match='spinach_queue_job.queue'):
            PostgresBroker(dsn, ensure_schema=False, require_ssl=False)
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(
                'ALTER TABLE spinach_queue_job '
                'ALTER COLUMN queue TYPE text'
            )
            conn.execute(
                'ALTER TABLE spinach_queue_job '
                'ALTER COLUMN queue SET NOT NULL'
            )
            conn.execute('CREATE SCHEMA earlier')
            conn.execute(
                'CREATE TABLE earlier.spinach_queue_job '
                '(LIKE public.spinach_queue_job INCLUDING ALL)'
            )
            conn.execute(
                'ALTER TABLE earlier.spinach_queue_job '
                'ALTER COLUMN queue TYPE integer USING 0'
            )
        hidden = psycopg.conninfo.make_conninfo(
            dsn, options='-c search_path=earlier,public',
        )
        with pytest.raises(RuntimeError, match='spinach_queue_job.queue'):
            PostgresBroker(
                hidden, ensure_schema=False, require_ssl=False,
            )
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(
                'ALTER TABLE spinach_queue_job DROP COLUMN queue'
            )
        with pytest.raises(RuntimeError, match='spinach_queue_job.queue'):
            PostgresBroker(dsn, ensure_schema=False, require_ssl=False)
    finally:
        with psycopg.connect(_dsn(), autocommit=True) as admin:
            admin.execute(
                'SELECT pg_terminate_backend(pid) FROM pg_stat_activity '
                'WHERE datname = %s',
                (name,),
            )
            admin.execute(
                'DROP DATABASE IF EXISTS spinach_schemacheck'
            )


def test_django_connection_returns_the_open_connection(monkeypatch):
    django_db = pytest.importorskip('django.db')
    sentinel = object()

    class Wrapper:
        connection = sentinel

        def ensure_connection(self):
            raise AssertionError('connection is already open')

    class Connections:
        def __getitem__(self, alias):
            assert alias == 'reports'
            return Wrapper()

    monkeypatch.setattr(django_db, 'connections', Connections())
    assert django_connection('reports') is sentinel


def test_django_connection_opens_when_needed(monkeypatch):
    django_db = pytest.importorskip('django.db')
    sentinel = object()

    class Wrapper:
        connection = None

        def ensure_connection(self):
            self.connection = sentinel

    monkeypatch.setattr(
        django_db, 'connections', {'default': Wrapper()},
    )
    assert django_connection() is sentinel


def test_sqlalchemy_connection_returns_the_driver():
    driver = object()

    class Bound:
        driver_connection = driver

    class Session:
        def connection(self):
            return Bound()

    assert sqlalchemy_connection(Session()) is driver


def test_sqlalchemy_connection_rejects_a_missing_driver():
    class Bound:
        driver_connection = None

    class Session:
        def connection(self):
            return Bound()

    with pytest.raises(RuntimeError, match='not available'):
        sqlalchemy_connection(Session())


@pytest.fixture
def broker():
    instance = _open()
    instance.namespace = 'poc-' + uuid.uuid4().hex
    instance.must_stop_periodicity = 0.01
    instance.flush()
    instance.start()
    try:
        yield instance
    finally:
        instance.stop()
        instance.flush()
        instance.close()


def _ensure_order_table():
    with psycopg.connect(_dsn(), autocommit=True) as conn:
        conn.execute(
            'CREATE TABLE IF NOT EXISTS spinach_poc_order ('
            'namespace text NOT NULL, order_id text NOT NULL)'
        )


def _count(conn, table, namespace):
    # Table names here are fixed test literals, not user input.
    row = conn.execute(
        'SELECT count(*) FROM {} WHERE namespace = %s'.format(table),
        (namespace,),
    ).fetchone()
    return row[0]


def _spin():
    namespace = 'poc-' + uuid.uuid4().hex
    broker = _open()
    broker.must_stop_periodicity = 0.01
    return Engine(broker, namespace=namespace), broker


def test_join_transaction_commits_with_the_local_row():
    _ensure_order_table()
    spin, broker = _spin()
    namespace = broker.namespace

    @spin.task(name='record')
    def record(order_id):
        return order_id

    try:
        with psycopg.connect(_dsn(), autocommit=True) as watcher:
            watcher.execute('LISTEN spinach_notify')
            list(watcher.notifies(timeout=0))
            with psycopg.connect(_dsn()) as conn:
                with conn.transaction():
                    conn.execute(
                        'INSERT INTO spinach_poc_order '
                        '(namespace, order_id) VALUES (%s, %s)',
                        (namespace, 'order-1'),
                    )
                    with spin.join_transaction(conn):
                        spin.schedule(record, 'order-1')
                        spin.schedule(record, 'order-2')
                    assert _count(
                        watcher, 'spinach_poc_order', namespace
                    ) == 0
                    assert _count(
                        watcher, 'spinach_queue_job', namespace
                    ) == 0
                    assert list(watcher.notifies(timeout=0.2)) == []
            assert _count(watcher, 'spinach_poc_order', namespace) == 1
            assert _count(watcher, 'spinach_queue_job', namespace) == 2
            notes = list(watcher.notifies(timeout=2))
            assert any(note.payload == namespace for note in notes)
        jobs = broker.get_jobs_from_queue('spinach', 10)
        assert sorted(job.task_args for job in jobs) == [
            ('order-1',), ('order-2',),
        ]
    finally:
        with psycopg.connect(_dsn(), autocommit=True) as conn:
            conn.execute(
                'DELETE FROM spinach_poc_order WHERE namespace = %s',
                (namespace,),
            )
        broker.flush()
        broker.close()


def test_join_transaction_rollback_drops_the_job():
    _ensure_order_table()
    spin, broker = _spin()
    namespace = broker.namespace

    @spin.task(name='record')
    def record(order_id):
        return order_id

    class RollBack(Exception):
        pass

    try:
        with pytest.raises(RollBack):
            with psycopg.connect(_dsn()) as conn:
                with conn.transaction():
                    conn.execute(
                        'INSERT INTO spinach_poc_order '
                        '(namespace, order_id) VALUES (%s, %s)',
                        (namespace, 'order-1'),
                    )
                    with spin.join_transaction(conn):
                        spin.schedule(record, 'order-1')
                    raise RollBack()
        with psycopg.connect(_dsn(), autocommit=True) as watcher:
            assert _count(watcher, 'spinach_poc_order', namespace) == 0
            assert _count(watcher, 'spinach_queue_job', namespace) == 0
    finally:
        broker.close()


def test_join_transaction_rejects_autocommit():
    spin, broker = _spin()
    try:
        with psycopg.connect(_dsn(), autocommit=True) as conn:
            with pytest.raises(RuntimeError, match='autocommit'):
                with spin.join_transaction(conn):
                    pass
    finally:
        broker.close()


def test_join_transaction_rejects_a_different_table():
    spin, broker = _spin()
    schema = 'earlier_' + uuid.uuid4().hex[:12]
    try:
        with psycopg.connect(_dsn(), autocommit=True) as admin:
            admin.execute(
                psycopg.sql.SQL('CREATE SCHEMA {}').format(
                    psycopg.sql.Identifier(schema)
                )
            )
            admin.execute(
                psycopg.sql.SQL(
                    'CREATE TABLE {}.spinach_queue_job '
                    '(LIKE public.spinach_queue_job INCLUDING ALL)'
                ).format(psycopg.sql.Identifier(schema))
            )
            admin.execute(
                psycopg.sql.SQL(
                    'CREATE TABLE {}.spinach_future_job '
                    '(LIKE public.spinach_future_job INCLUDING ALL)'
                ).format(psycopg.sql.Identifier(schema))
            )
        hidden = psycopg.conninfo.make_conninfo(
            _dsn(),
            options='-c search_path=%s,public' % schema,
        )
        with psycopg.connect(hidden) as conn:
            with pytest.raises(RuntimeError, match='Spinach tables'):
                with spin.join_transaction(conn):
                    pass
            row = conn.execute(
                psycopg.sql.SQL(
                    'SELECT count(*) FROM {}.spinach_queue_job'
                ).format(psycopg.sql.Identifier(schema))
            ).fetchone()
            assert row[0] == 0
    finally:
        with psycopg.connect(_dsn(), autocommit=True) as admin:
            admin.execute(
                psycopg.sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(
                    psycopg.sql.Identifier(schema)
                )
            )
        broker.close()


def test_join_transaction_accepts_an_insert_only_role():
    role = 'spinach_app_' + uuid.uuid4().hex[:12]
    spin, broker = _spin()
    namespace = broker.namespace

    @spin.task(name='record')
    def record(order_id):
        return order_id

    info = psycopg.conninfo.conninfo_to_dict(_dsn())
    app_dsn = psycopg.conninfo.make_conninfo(
        host=info.get('host') or '127.0.0.1',
        port=info.get('port') or 5432,
        dbname=info['dbname'],
        user=role,
    )
    ident = psycopg.sql.Identifier(role)
    try:
        with psycopg.connect(_dsn(), autocommit=True) as admin:
            admin.execute(
                psycopg.sql.SQL('CREATE ROLE {} LOGIN').format(ident)
            )
            admin.execute(
                psycopg.sql.SQL(
                    'GRANT CONNECT ON DATABASE {} TO {}'
                ).format(psycopg.sql.Identifier(info['dbname']), ident)
            )
            admin.execute(
                psycopg.sql.SQL(
                    'GRANT USAGE ON SCHEMA public TO {}'
                ).format(ident)
            )
            admin.execute(
                psycopg.sql.SQL(
                    'GRANT INSERT ON spinach_queue_job, '
                    'spinach_future_job TO {}'
                ).format(ident)
            )
            admin.execute(
                psycopg.sql.SQL(
                    'GRANT USAGE ON SEQUENCE '
                    'spinach_queue_job_position_seq TO {}'
                ).format(ident)
            )
        with psycopg.connect(app_dsn) as conn:
            allowed = conn.execute(
                'SELECT has_table_privilege('
                'current_user, %s, %s)',
                ('spinach_running_job', 'DELETE'),
            ).fetchone()[0]
            assert allowed is False
        later = datetime.now(timezone.utc) + timedelta(days=1)
        with psycopg.connect(app_dsn) as conn:
            with conn.transaction():
                with spin.join_transaction(conn):
                    spin.schedule(record, 'now')
                    spin.schedule_at(record, later, 'later')
        with psycopg.connect(_dsn()) as watcher:
            assert _count(watcher, 'spinach_queue_job', namespace) == 1
            assert _count(watcher, 'spinach_future_job', namespace) == 1
    finally:
        with psycopg.connect(_dsn(), autocommit=True) as admin:
            exists = admin.execute(
                'SELECT 1 FROM pg_roles WHERE rolname = %s', (role,)
            ).fetchone()
            if exists:
                admin.execute(
                    psycopg.sql.SQL(
                        'REVOKE ALL ON SEQUENCE '
                        'spinach_queue_job_position_seq FROM {}'
                    ).format(ident)
                )
                admin.execute(
                    psycopg.sql.SQL(
                        'REVOKE ALL ON TABLE spinach_queue_job, '
                        'spinach_future_job FROM {}'
                    ).format(ident)
                )
                admin.execute(
                    psycopg.sql.SQL(
                        'REVOKE USAGE ON SCHEMA public FROM {}'
                    ).format(ident)
                )
                admin.execute(
                    psycopg.sql.SQL(
                        'REVOKE CONNECT ON DATABASE {} FROM {}'
                    ).format(
                        psycopg.sql.Identifier(info['dbname']), ident,
                    )
                )
                admin.execute(
                    psycopg.sql.SQL('DROP ROLE {}').format(ident)
                )
        broker.flush()
        broker.close()


def test_join_transaction_rejects_a_different_database():
    spin, broker = _spin()
    try:
        with psycopg.connect(_dsn(), autocommit=True) as admin:
            exists = admin.execute(
                'SELECT 1 FROM pg_database '
                "WHERE datname = 'spinach_poctest'"
            ).fetchone()
            if exists is None:
                admin.execute('CREATE DATABASE spinach_poctest')
        other_dsn = psycopg.conninfo.make_conninfo(
            _dsn(), dbname='spinach_poctest'
        )
        with psycopg.connect(other_dsn) as other:
            with pytest.raises(RuntimeError, match='same database'):
                with spin.join_transaction(other):
                    pass
    finally:
        broker.close()


def test_task_keyword_connection_is_not_the_broker_connection():
    spin, broker = _spin()

    @spin.task(name='record')
    def record(connection):
        return connection

    try:
        with psycopg.connect(_dsn()) as conn:
            with conn.transaction():
                with spin.join_transaction(conn):
                    spin.schedule(record, connection='from-the-app')
        job = broker.get_jobs_from_queue('spinach', 1)[0]
        assert job.task_kwargs == {'connection': 'from-the-app'}
    finally:
        broker.flush()
        broker.close()


def test_worker_executes_a_job():
    broker = _open()
    broker.must_stop_periodicity = 0.01
    namespace = 'worker-' + uuid.uuid4().hex
    spin = Engine(broker, namespace=namespace)
    ran = threading.Event()
    seen = {}

    @spin.task(name='touch')
    def touch(value):
        seen['value'] = value
        ran.set()

    try:
        spin.schedule(touch, 7)
        spin.start_workers(number=1, block=False)
        assert ran.wait(10), 'worker did not run the job'
        assert seen['value'] == 7
    finally:
        if spin._workers is not None:
            spin.stop_workers()
        else:
            broker.stop()
        broker.flush()
        broker.close()


def test_concurrency_limit_leaves_the_extra_job_queued(broker):
    broker.set_concurrency_keys([
        Task(print, 'limited', 'q', 2, None, max_concurrency=1),
    ])
    now = datetime.now(timezone.utc)
    broker.enqueue_jobs([
        Job('limited', 'q', now, 2),
        Job('limited', 'q', now, 2),
    ])
    first = broker.get_jobs_from_queue('q', 10)
    assert len(first) == 1
    assert first[0].status is JobStatus.RUNNING
    assert broker.get_jobs_from_queue('q', 10) == []
    assert broker.is_queue_empty('q') is False
    broker.remove_job_from_running(first[0])
    second = broker.get_jobs_from_queue('q', 10)
    assert len(second) == 1
    assert broker.is_queue_empty('q') is True


@patch(
    'spinach.brokers.postgres.generate_idempotency_token',
    return_value='same-token',
)
def test_same_idempotency_token_does_not_enqueue_twice(_, broker):
    now = datetime.now(timezone.utc)
    job_1 = Job('foo_task', 'foo_queue', now, 0)
    job_2 = Job('foo_task', 'foo_queue', now, 0)
    broker.enqueue_jobs([job_1])
    broker.enqueue_jobs([job_2])
    jobs = broker.get_jobs_from_queue('foo_queue', max_jobs=10)
    job_1.status = JobStatus.RUNNING
    assert jobs == [job_1]


def test_failure_requeue_clears_the_running_row(broker):
    broker.set_concurrency_keys([
        Task(print, 'limited', 'q', 2, None, max_concurrency=1),
    ])
    now = datetime.now(timezone.utc)
    job = Job('limited', 'q', now, 2)
    broker.enqueue_jobs([job])
    assert len(broker.get_jobs_from_queue('q', 1)) == 1
    job.status = JobStatus.NOT_SET
    job.retries += 1
    job.at = now + timedelta(days=1)
    broker.enqueue_jobs([job], from_failure=True)
    with psycopg.connect(_dsn()) as conn:
        running = conn.execute(
            'SELECT count(*) FROM spinach_running_job '
            'WHERE namespace = %s AND job_id = %s',
            (broker.namespace, job.id),
        ).fetchone()[0]
        waiting = conn.execute(
            'SELECT count(*) FROM spinach_future_job '
            'WHERE namespace = %s AND job_id = %s',
            (broker.namespace, job.id),
        ).fetchone()[0]
    assert running == 0
    assert waiting == 1
    nxt = Job('limited', 'q', now, 2)
    broker.enqueue_jobs([nxt])
    claimed = broker.get_jobs_from_queue('q', 1)
    assert [item.id for item in claimed] == [nxt.id]


def test_set_concurrency_keys_clears_an_empty_task_list(broker):
    broker.set_concurrency_keys([
        Task(print, 'limited', 'q', 2, None, max_concurrency=1),
    ])
    broker.set_concurrency_keys([])
    now = datetime.now(timezone.utc)
    broker.enqueue_jobs([
        Job('limited', 'q', now, 2),
        Job('limited', 'q', now, 2),
    ])
    assert len(broker.get_jobs_from_queue('q', 10)) == 2


def test_dead_broker_frees_the_slot_of_a_discarded_job():
    namespace = 'slot-' + uuid.uuid4().hex
    first = _open()
    second = _open()
    first.namespace = namespace
    second.namespace = namespace
    try:
        first.set_concurrency_keys([
            Task(print, 'limited', 'q', 2, None, max_concurrency=1),
        ])
        now = datetime.now(timezone.utc)
        stuck = Job('limited', 'q', now, 0)
        first.enqueue_jobs([stuck])
        assert len(first.get_jobs_from_queue('q', 10)) == 1
        num, failed = second.enqueue_jobs_from_dead_broker(first._id)
        assert num == 0
        assert len(failed) == 1
        nxt = Job('limited', 'q', now, 0)
        second.enqueue_jobs([nxt])
        claimed = second.get_jobs_from_queue('q', 10)
        assert [job.id for job in claimed] == [nxt.id]
    finally:
        first.flush()
        first.close()
        second.close()


def test_dead_broker_requeues_only_retryable_jobs():
    namespace = 'dead-' + uuid.uuid4().hex
    first = _open()
    second = _open()
    first.namespace = namespace
    second.namespace = namespace
    try:
        now = datetime.now(timezone.utc)
        retryable = Job('foo_task', 'q', now, 3)
        once = Job('foo_task', 'q', now, 0)
        first.enqueue_jobs([retryable, once])
        assert len(first.get_jobs_from_queue('q', 10)) == 2
        num, failed = second.enqueue_jobs_from_dead_broker(first._id)
        assert num == 1
        assert len(failed) == 1
        assert Job.deserialize(failed[0]).id == once.id
        again = second.get_jobs_from_queue('q', 10)
        assert len(again) == 1
        assert again[0].id == retryable.id
        assert again[0].retries == 1
        assert second.enqueue_jobs_from_dead_broker(first._id) == (0, [])
    finally:
        first.flush()
        first.close()
        second.close()


def test_stop_deregisters_the_broker():
    namespace = 'reg-' + uuid.uuid4().hex
    first = _open()
    second = _open()
    first.namespace = namespace
    second.namespace = namespace
    first.must_stop_periodicity = 0.01
    try:
        first.start()
        first.move_future_jobs()
        assert any(
            item['id'] == str(first._id)
            for item in first.get_all_brokers()
        )
        first.stop()
        assert all(
            item['id'] != str(first._id)
            for item in second.get_all_brokers()
        )
    finally:
        second.flush()
        first.close()
        second.close()


def test_flush_keeps_other_namespaces():
    first = _open()
    second = _open()
    first.namespace = 'flush-' + uuid.uuid4().hex
    second.namespace = 'flush-' + uuid.uuid4().hex
    now = datetime.now(timezone.utc)
    try:
        first.enqueue_jobs([Job('t', 'q', now, 0)])
        second.enqueue_jobs([Job('t', 'q', now, 0)])
        first.flush()
        assert first.get_jobs_from_queue('q', 1) == []
        assert len(second.get_jobs_from_queue('q', 1)) == 1
    finally:
        second.flush()
        first.close()
        second.close()
