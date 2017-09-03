import functools
from typing import Optional, Callable

from . import const


class Task:

    __slots__ = ['func', 'name', 'queue']

    def __init__(self, func: Callable, name: str, queue: str):
        self.func = func
        self.name = name
        self.queue = queue

    def __repr__(self):
        return 'Task({}, {}, {})'.format(self.func, self.name, self.queue)

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

    def __init__(self, queue: Optional[str]=None):
        self._tasks = {}
        self.queue = queue

    @property
    def tasks(self) -> dict:
        return self._tasks

    def task(self, func: Optional[Callable]=None, name: Optional[str]=None,
             queue: Optional[str]=None):

        if func is None:
            return functools.partial(self.task, name=name, queue=queue)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        self.add(func, name=name, queue=queue)

        return wrapper

    def add(self, func: Callable, name: Optional[str]=None,
            queue: Optional[str]=None):
        if not name:
            raise ValueError('Each Spinach task needs a name')
        if name in self._tasks:
            raise ValueError('A task named {} already exists'.format(name))

        if not queue:
            if self.queue:
                queue = self.queue
            else:
                queue = const.DEFAULT_QUEUE

        self._tasks[name] = Task(func, name, queue)
