from datetime import datetime, timedelta
import functools
from typing import Optional, Callable
from numbers import Number
import random

from . import const


class Task:

    __slots__ = ['func', 'name', 'queue', 'max_retries']

    def __init__(self, func: Callable, name: str, queue: str,
                 max_retries: Number):
        self.func = func
        self.name = name
        self.queue = queue
        self.max_retries = max_retries

    def __repr__(self):
        return 'Task({}, {}, {}, {})'.format(self.func, self.name,
                                             self.queue, self.max_retries)

    def __eq__(self, other):
        for attr in self.__slots__:
            try:
                if not getattr(self, attr) == getattr(other, attr):
                    return False
            except AttributeError:
                return False
        return True


class Tasks:
    """Hold some tasks to be used by Spinach.

    This class is not thread-safe because it doesn't need to be used
    concurrently.
    """

    def __init__(self, queue: Optional[str]=None,
                 max_retries: Optional[Number]=None):
        self._tasks = {}
        self.queue = queue
        self.max_retries = max_retries
        self._spin = None

    @property
    def tasks(self) -> dict:
        return self._tasks

    def task(self, func: Optional[Callable]=None, name: Optional[str]=None,
             queue: Optional[str]=None, max_retries: Optional[Number]=None):

        if func is None:
            return functools.partial(self.task, name=name, queue=queue,
                                     max_retries=max_retries)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        self.add(func, name=name, queue=queue, max_retries=max_retries)

        return wrapper

    def add(self, func: Callable, name: Optional[str]=None,
            queue: Optional[str]=None, max_retries: Optional[Number]=None):
        if not name:
            raise ValueError('Each Spinach task needs a name')
        if name in self._tasks:
            raise ValueError('A task named {} already exists'.format(name))

        if queue is None:
            if self.queue:
                queue = self.queue
            else:
                queue = const.DEFAULT_QUEUE

        if max_retries is None:
            if self.max_retries:
                max_retries = self.max_retries
            else:
                max_retries = const.DEFAULT_MAX_RETRIES

        if queue and queue.startswith('_'):
            raise ValueError('Queues starting with "_" are reserved by '
                             'Spinach for internal use')

        self._tasks[name] = Task(func, name, queue, max_retries)

    def _require_attached_tasks(self):
        if self._spin is None:
            raise RuntimeError(
                'Cannot execute tasks until the tasks have been attached to '
                'a Spinach instance.'
            )

    def schedule(self, task_name: str, *args, **kwargs):
        self._require_attached_tasks()
        self._spin.schedule(task_name, *args, **kwargs)

    def schedule_at(self, task_name: str, at: datetime, *args, **kwargs):
        self._require_attached_tasks()
        self._spin.schedule_at(task_name, at, *args, **kwargs)


def exponential_backoff(attempt: int) -> timedelta:
    """Calculate a delay to retry using an exponential backoff algorithm.

    It is an exponential backoff with random jitter to prevent all failed tasks
    from being retried at the same time. It is a good fit for most
    applications.
    """
    cap = 1200  # max 20 minutes
    base = 3

    temp = min(base * 2 ** attempt, cap)
    return timedelta(seconds=temp / 2 + random.randint(0, temp / 2))
