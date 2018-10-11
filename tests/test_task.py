import copy
from datetime import timedelta
from unittest import mock

import pytest

from spinach.task import (Task, Tasks, Batch, RetryException)
from spinach import const, exc

from .conftest import get_now


@pytest.fixture
def task():
    return Task(print, 'write_to_stdout', 'foo_queue', 0, None)


def test_task(task):
    assert task.func is print
    assert task.name == 'write_to_stdout'
    assert task.queue == 'foo_queue'
    assert task.max_retries == 0
    assert task.periodicity is None

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


def test_task_serialize(task):
    expected = (
        '{"max_retries": 0, "name": "write_to_stdout", '
        '"periodicity": null, "queue": "foo_queue"}'
    )
    assert task.serialize() == expected

    task.periodicity = timedelta(minutes=5)
    expected = (
        '{"max_retries": 0, "name": "write_to_stdout", '
        '"periodicity": 300, "queue": "foo_queue"}'
    )
    assert task.serialize() == expected


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
    assert foo.task_name == 'foo'
    assert tasks.tasks['foo'].name == 'foo'
    assert tasks.tasks['foo'].queue == 'tasks_queue'
    assert tasks.tasks['foo'].max_retries == const.DEFAULT_MAX_RETRIES

    assert 'bar' in str(tasks.tasks['bar'].func)
    assert bar.task_name == 'bar'
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


def test_tasks_update():
    tasks_1, tasks_2 = Tasks(), Tasks()

    tasks_1.add(print, 'write_to_stdout', queue='foo_queue')
    tasks_2.update(tasks_1)
    assert tasks_1.tasks == tasks_2.tasks

    tasks_2.add(print, 'bar')
    assert tasks_1.tasks != tasks_2.tasks


def test_tasks_names():
    tasks = Tasks()
    assert tasks.names == []
    tasks.add(print, 'foo')
    tasks.add(print, 'bar')
    assert sorted(tasks.names) == ['bar', 'foo']


def test_tasks_get_by_name():
    tasks = Tasks()
    tasks.add(print, 'foo')

    r = tasks.get('foo')
    assert isinstance(r, Task)
    assert r.name == 'foo'
    assert r.func == print


def test_tasks_get_by_function():
    tasks = Tasks()

    @tasks.task(name='foo')
    def foo():
        pass

    r = tasks.get(foo)
    assert isinstance(r, Task)
    assert r.name == 'foo'
    assert r.func == foo


def test_tasks_get_by_task_object(task):
    tasks = Tasks()
    tasks._tasks[task.name] = task

    r = tasks.get(task)
    assert isinstance(r, Task)
    assert r.name == task.name
    assert r.func == task.func


def test_tasks_get_by_unknown_or_wrong_object():
    tasks = Tasks()
    with pytest.raises(exc.UnknownTask):
        tasks.get('foo')
    with pytest.raises(exc.UnknownTask):
        tasks.get(None)
    with pytest.raises(exc.UnknownTask):
        tasks.get(object())
    with pytest.raises(exc.UnknownTask):
        tasks.get(b'foo')
    with pytest.raises(exc.UnknownTask):
        tasks.get(RuntimeError)


def test_tasks_scheduling(task):
    tasks = Tasks()
    tasks.add(print, 'write_to_stdout', queue='foo_queue')
    batch = Batch()

    with pytest.raises(RuntimeError):
        tasks.schedule('write_to_stdout')
    with pytest.raises(RuntimeError):
        tasks.schedule_at('write_to_stdout', get_now())
    with pytest.raises(RuntimeError):
        tasks.schedule_batch(batch)

    spin = mock.Mock()
    tasks._spin = spin

    tasks.schedule('write_to_stdout')
    spin.schedule.assert_called_once_with('write_to_stdout')

    tasks.schedule_at('write_to_stdout', get_now())
    spin.schedule_at.assert_called_once_with('write_to_stdout', get_now())

    tasks.schedule_batch(batch)
    spin.schedule_batch.assert_called_once_with(batch)


def test_retry_exception():
    r = RetryException('Foo')
    assert str(r) == 'Foo'
    assert r.at is None

    r = RetryException('Bar', get_now())
    assert str(r) == 'Bar'
    assert r.at is get_now()


def test_batch(patch_now):
    now = get_now()
    batch = Batch()
    batch.schedule('foo_task', 1, 2)
    batch.schedule_at('bar_task', now, three=True)

    assert batch.jobs_to_create == [
        ('foo_task', now, (1, 2), {}),
        ('bar_task', now, (), {'three': True})
    ]
