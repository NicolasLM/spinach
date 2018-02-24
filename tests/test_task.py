import copy
from datetime import timedelta
from unittest import mock

import pytest

from spinach.task import Task, Tasks, RetryException, exponential_backoff
from spinach import const

from .conftest import get_now


@pytest.fixture
def task():
    return Task(print, 'write_to_stdout', 'foo_queue', 0)


def test_task(task):
    assert task.func is print
    assert task.name == 'write_to_stdout'
    assert task.queue == 'foo_queue'
    assert task.max_retries == 0

    assert 'print' in repr(task)
    assert 'write_to_stdout' in repr(task)
    assert 'foo_queue' in repr(task)
    assert '0' in repr(task)


def test_task_eq(task):
    assert task == task
    assert task != print

    task_2 = copy.deepcopy(task)
    task_2.name = 'read_from_stdout'
    assert task != task_2


def test_tasks_add(task):
    tasks = Tasks()
    tasks.add(print, 'write_to_stdout', queue='foo_queue')
    assert tasks.tasks == {
        'write_to_stdout': task
    }

    with pytest.raises(ValueError):
        tasks.add(print, 'write_to_stdout', queue='bar_queue')
    with pytest.raises(ValueError):
        tasks.add(print, queue='bar_queue')
    with pytest.raises(ValueError):
        tasks.add(print, name='internal', queue='_internal_queue')
    assert tasks.tasks == {
        'write_to_stdout': task
    }


def test_tasks_queues_and_max_retries():
    # Constant default
    tasks = Tasks()
    tasks.add(print, 'write_to_stdout')
    assert tasks.tasks['write_to_stdout'].queue == const.DEFAULT_QUEUE
    assert (tasks.tasks['write_to_stdout'].max_retries ==
            const.DEFAULT_MAX_RETRIES)

    # Tasks has a default
    tasks = Tasks(queue='tasks_default', max_retries=10)
    tasks.add(print, 'write_to_stdout')
    assert tasks.tasks['write_to_stdout'].queue == 'tasks_default'
    assert tasks.tasks['write_to_stdout'].max_retries == 10

    # Task added with an explicit value
    tasks = Tasks(queue='tasks_default', max_retries=10)
    tasks.add(print, 'write_to_stdout', queue='task_queue', max_retries=20)
    assert tasks.tasks['write_to_stdout'].queue == 'task_queue'
    assert tasks.tasks['write_to_stdout'].max_retries == 20


def test_tasks_decorator():

    tasks = Tasks(queue='tasks_queue')

    @tasks.task(name='foo')
    def foo():
        pass

    @tasks.task(name='bar', queue='task_queue', max_retries=20)
    def bar():
        pass

    assert 'foo' in str(tasks.tasks['foo'].func)
    assert tasks.tasks['foo'].name == 'foo'
    assert tasks.tasks['foo'].queue == 'tasks_queue'
    assert tasks.tasks['foo'].max_retries == const.DEFAULT_MAX_RETRIES

    assert 'bar' in str(tasks.tasks['bar'].func)
    assert tasks.tasks['bar'].name == 'bar'
    assert tasks.tasks['bar'].queue == 'task_queue'
    assert tasks.tasks['bar'].max_retries == 20


def test_task_function_can_be_called():

    tasks = Tasks()

    @tasks.task(name='foo')
    def foo(a, b=2):
        return a + b

    assert foo(40) == 42
    assert foo(40, 3) == 43


def test_tasks_scheduling(task):
    tasks = Tasks()
    tasks.add(print, 'write_to_stdout', queue='foo_queue')

    with pytest.raises(RuntimeError):
        tasks.schedule('write_to_stdout')
    with pytest.raises(RuntimeError):
        tasks.schedule_at('write_to_stdout', get_now())

    spin = mock.Mock()
    tasks._spin = spin

    tasks.schedule('write_to_stdout')
    spin.schedule.assert_called_once_with('write_to_stdout')

    tasks.schedule_at('write_to_stdout', get_now())
    spin.schedule_at.assert_called_once_with('write_to_stdout', get_now())


def test_exponential_backoff():
    with pytest.raises(ValueError):
        exponential_backoff(0)

    assert (
        timedelta(seconds=3) <= exponential_backoff(1) <= timedelta(seconds=6)
    )
    assert exponential_backoff(10000) <= timedelta(minutes=20)


def test_retry_exception():
    r = RetryException('Foo')
    assert str(r) == 'Foo'
    assert r.at is None

    r = RetryException('Bar', get_now())
    assert str(r) == 'Bar'
    assert r.at is get_now()
