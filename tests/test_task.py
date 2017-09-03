import copy

import pytest

from spinach.task import Task, Tasks
from spinach import const


@pytest.fixture
def task():
    return Task(print, 'write_to_stdout', 'foo_queue')


def test_task(task):
    assert task.func is print
    assert task.name == 'write_to_stdout'
    assert task.queue == 'foo_queue'

    assert 'print' in repr(task)
    assert 'write_to_stdout' in repr(task)
    assert 'foo_queue' in repr(task)


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
    assert tasks.tasks == {
        'write_to_stdout': task
    }


def test_tasks_queues():
    # Constant default queue
    tasks = Tasks()
    tasks.add(print, 'write_to_stdout')
    assert tasks.tasks['write_to_stdout'].queue == const.DEFAULT_QUEUE

    # Tasks has a default queue
    tasks = Tasks(queue='tasks_default')
    tasks.add(print, 'write_to_stdout')
    assert tasks.tasks['write_to_stdout'].queue == 'tasks_default'

    # Task added with an explicit queue
    tasks = Tasks(queue='tasks_default')
    tasks.add(print, 'write_to_stdout', queue='task_queue')
    assert tasks.tasks['write_to_stdout'].queue == 'task_queue'


def test_tasks_decorator():

    tasks = Tasks(queue='tasks_queue')

    @tasks.task(name='foo')
    def foo():
        pass

    @tasks.task(name='bar', queue='task_queue')
    def bar():
        pass

    assert 'foo' in str(tasks.tasks['foo'].func)
    assert tasks.tasks['foo'].name == 'foo'
    assert tasks.tasks['foo'].queue == 'tasks_queue'

    assert 'bar' in str(tasks.tasks['bar'].func)
    assert tasks.tasks['bar'].name == 'bar'
    assert tasks.tasks['bar'].queue == 'task_queue'
