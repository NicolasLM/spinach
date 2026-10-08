from contextlib import contextmanager
from unittest.mock import Mock, ANY, patch

import pytest

from spinach import Engine, MemoryBroker, Batch, Tasks
from spinach.brokers.redis import RedisBroker
from spinach.exc import UnknownTask

from .conftest import get_now


@pytest.fixture
def spin():
    s = Engine(MemoryBroker(), namespace='tests')
    s.start_workers(number=1, block=False)
    yield s
    s.stop_workers()


spin_2 = spin


def test_schedule_unknown_task(spin):
    with pytest.raises(UnknownTask):
        spin.schedule('foo_task')


@patch('spinach.engine.logger')
def test_attach_tasks(mock_logger, spin, spin_2):
    tasks = Tasks()
    tasks.add(print, 'foo_task')

    spin.attach_tasks(tasks)
    mock_logger.warning.assert_not_called()
    assert tasks._spin is spin
    assert spin._tasks.tasks == tasks.tasks

    spin.attach_tasks(tasks)
    mock_logger.warning.assert_not_called()
    assert tasks._spin is spin
    assert spin._tasks.tasks == tasks.tasks

    spin_2.attach_tasks(tasks)
    mock_logger.warning.assert_called_once_with(ANY)
    assert tasks._spin is spin_2
    assert spin_2._tasks.tasks == tasks.tasks


def test_schedule_at(patch_now):
    now = get_now()

    tasks = Tasks()
    tasks.add(Mock(), 'bar_task')

    broker = Mock()

    s = Engine(broker, namespace='tests')
    s.attach_tasks(tasks)

    job = s.schedule_at('bar_task', now, three=True)

    bar_job = broker.enqueue_jobs.call_args[0][0][0]
    assert bar_job == job
    assert bar_job.task_name == 'bar_task'
    assert bar_job.at == now
    assert bar_job.task_args == ()
    assert bar_job.task_kwargs == {'three': True}


def test_schedule(patch_now):
    now = get_now()

    tasks = Tasks()
    tasks.add(print, 'foo_task')

    broker = Mock()

    s = Engine(broker, namespace='tests')
    s.attach_tasks(tasks)

    job1 = s.schedule('foo_task', 1, 2)

    foo_job = broker.enqueue_jobs.call_args[0][0][0]
    assert foo_job == job1
    assert foo_job.task_name == 'foo_task'
    assert foo_job.at == now
    assert foo_job.task_args == (1, 2)
    assert foo_job.task_kwargs == {}


def test_schedule_batch(patch_now):
    now = get_now()

    tasks = Tasks()
    tasks.add(Mock(), 'foo_task')
    tasks.add(Mock(), 'bar_task')

    broker = Mock()

    s = Engine(broker, namespace='tests')
    s.attach_tasks(tasks)

    batch = Batch()
    batch.schedule('foo_task', 1, 2)
    batch.schedule_at('bar_task', now, three=True)
    jobs = s.schedule_batch(batch)

    broker.enqueue_jobs.assert_called_once_with([ANY, ANY])

    foo_job = broker.enqueue_jobs.call_args[0][0][0]
    assert foo_job in jobs
    assert foo_job.task_name == 'foo_task'
    assert foo_job.at == now
    assert foo_job.task_args == (1, 2)
    assert foo_job.task_kwargs == {}

    bar_job = broker.enqueue_jobs.call_args[0][0][1]
    assert bar_job in jobs
    assert bar_job.task_name == 'bar_task'
    assert bar_job.at == now
    assert bar_job.task_args == ()
    assert bar_job.task_kwargs == {'three': True}


def test_execute(spin):
    func = Mock()
    tasks = Tasks()
    tasks.add(func, 'foo_task')
    spin.attach_tasks(tasks)

    spin.execute('foo_task')
    func.assert_called_once_with()


def test_start_workers_twice(spin):
    with pytest.raises(RuntimeError):
        spin.start_workers()


class _JoinedBroker:
    supports_join_transaction = True

    def __init__(self):
        self.namespace = None
        self.enqueued = []
        self.tx_enqueued = []
        self._connection = None

    @contextmanager
    def join_transaction(self, connection):
        self._connection = connection
        try:
            yield
        finally:
            self._connection = None

    def joined_connection(self):
        return self._connection

    def enqueue_jobs(self, jobs, from_failure=False):
        self.enqueued.extend(jobs)

    def enqueue_in_transaction(self, jobs, connection):
        self.tx_enqueued.append((connection, list(jobs)))


@pytest.mark.parametrize('broker_cls', [MemoryBroker, RedisBroker])
def test_join_transaction_rejected(broker_cls):
    spin = Engine(broker_cls(), namespace='tests')
    with pytest.raises(
            RuntimeError, match='does not support join_transaction'):
        with spin.join_transaction(object()):
            pass


def test_schedule_joins_caller_transaction():
    broker = _JoinedBroker()
    spin = Engine(broker, namespace='tests')
    tasks = Tasks()

    def record(connection):
        pass

    tasks.add(record, 'record')
    spin.attach_tasks(tasks)
    connection = object()

    with spin.join_transaction(connection):
        job = spin.schedule('record', connection='from-the-app')

    assert broker.enqueued == []
    assert broker.tx_enqueued[0][0] is connection
    scheduled = broker.tx_enqueued[0][1][0]
    assert scheduled is job
    assert scheduled.task_kwargs == {'connection': 'from-the-app'}

    spin.schedule('record', connection='later')
    assert len(broker.enqueued) == 1
    assert broker.enqueued[0].task_kwargs == {'connection': 'later'}


def test_start_workers_blocking():
    spin = Engine(MemoryBroker(), namespace='tests')
    spin.start_workers(number=1, block=True, stop_when_queue_empty=True)
    assert not spin._must_stop.is_set()
