from unittest.mock import Mock, ANY, patch

import pytest

from spinach import Engine, MemoryBroker, Batch, Tasks
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


def test_schedule_batch(patch_now):
    now = get_now()

    tasks = Tasks()
    tasks.add(print, 'foo_task')
    tasks.add(print, 'bar_task')

    broker = Mock()

    s = Engine(broker, namespace='tests')
    s.attach_tasks(tasks)

    batch = Batch()
    batch.schedule('foo_task', 1, 2)
    batch.schedule_at('bar_task', now, three=True)
    s.schedule_batch(batch)

    broker.enqueue_jobs.assert_called_once_with([ANY, ANY])

    foo_job = broker.enqueue_jobs.call_args[0][0][0]
    assert foo_job.task_name == 'foo_task'
    assert foo_job.at == now
    assert foo_job.task_args == (1, 2)
    assert foo_job.task_kwargs == {}

    bar_job = broker.enqueue_jobs.call_args[0][0][1]
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


def test_start_workers_blocking():
    spin = Engine(MemoryBroker(), namespace='tests')
    spin.start_workers(number=1, block=True, stop_when_queue_empty=True)
    assert not spin._must_stop.is_set()


def test_engine_list_queue():
    s = Engine(MemoryBroker(), namespace='tests')
    tasks = Tasks()
    tasks.add(print, 'foo_task', queue='q1')
    s.attach_tasks(tasks)
    tasks.schedule('foo_task')
    [result] = s.list_queue('q1')
    assert result['task_name'] == 'foo_task'
