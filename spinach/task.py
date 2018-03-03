from datetime import datetime, timezone, timedelta
import functools
import json
from typing import Optional, Callable, List
from numbers import Number

from . import const, exc


class Task:

    __slots__ = ['func', 'name', 'queue', 'max_retries', 'periodicity']

    def __init__(self, func: Callable, name: str, queue: str,
                 max_retries: Number, periodicity: Optional[timedelta]):
        self.func = func
        self.name = name
        self.queue = queue
        self.max_retries = max_retries
        self.periodicity = periodicity

    def serialize(self):
        periodicity = (int(self.periodicity.total_seconds())
                       if self.periodicity else None)
        return json.dumps({
            'name': self.name,
            'queue': self.queue,
            'max_retries': self.max_retries,
            'periodicity': periodicity
        }, sort_keys=True)

    def __repr__(self):
        return 'Task({}, {}, {}, {}, {})'.format(
            self.func, self.name, self.queue, self.max_retries,
            self.periodicity
        )

    def __eq__(self, other):
        for attr in self.__slots__:
            try:
                if not getattr(self, attr) == getattr(other, attr):
                    return False
            except AttributeError:
                return False
        return True


class Tasks:
    """Registry for tasks to be used by Spinach.

    :arg queue: default queue for tasks
    :arg max_retries: default retry policy for tasks
    :arg periodicity: for periodic tasks, delay between executions as a
         timedelta
    """
    # This class is not thread-safe because it doesn't need to be used
    # concurrently.

    def __init__(self, queue: Optional[str]=None,
                 max_retries: Optional[Number]=None,
                 periodicity: Optional[timedelta]=None):
        self._tasks = {}
        self.queue = queue
        self.max_retries = max_retries
        self.periodicity = periodicity
        self._spin = None

    def update(self, tasks: 'Tasks'):
        self._tasks.update(tasks.tasks)

    @property
    def names(self) -> List[str]:
        return list(self._tasks.keys())

    @property
    def tasks(self) -> dict:
        return self._tasks

    def get(self, name: str):
        task = self._tasks.get(name)
        if task is not None:
            return task

        raise exc.UnknownTask(
            'Unknown task "{}", known tasks: {}'.format(name, self.names)
        )

    def task(self, func: Optional[Callable]=None, name: Optional[str]=None,
             queue: Optional[str]=None, max_retries: Optional[Number]=None,
             periodicity: Optional[timedelta]=None):
        """Decorator to register a task function.

        :arg name: name of the task, used later to schedule jobs
        :arg queue: queue of the task, the default is used if not provided
        :arg max_retries: maximum number of retries, the default is used if
             not provided
        :arg periodicity: for periodic tasks, delay between executions as a
             timedelta

        >>> tasks = Tasks()
        >>> @tasks.task(name='foo')
        >>> def foo():
        ...    pass
        """
        if func is None:
            return functools.partial(self.task, name=name, queue=queue,
                                     max_retries=max_retries,
                                     periodicity=periodicity)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        self.add(func, name=name, queue=queue, max_retries=max_retries,
                 periodicity=periodicity)

        return wrapper

    def add(self, func: Callable, name: Optional[str]=None,
            queue: Optional[str]=None, max_retries: Optional[Number]=None,
            periodicity: Optional[timedelta]=None):
        """Register a task function.

        :arg func: a callable to be executed
        :arg name: name of the task, used later to schedule jobs
        :arg queue: queue of the task, the default is used if not provided
        :arg max_retries: maximum number of retries, the default is used if
             not provided
        :arg periodicity: for periodic tasks, delay between executions as a
             timedelta

        >>> tasks = Tasks()
        >>> tasks.add(lambda x: x, name='do_nothing')
        """
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

        if periodicity is None:
            periodicity = self.periodicity

        if queue and queue.startswith('_'):
            raise ValueError('Queues starting with "_" are reserved by '
                             'Spinach for internal use')

        self._tasks[name] = Task(func, name, queue, max_retries, periodicity)

    def _require_attached_tasks(self):
        if self._spin is None:
            raise RuntimeError(
                'Cannot execute tasks until the tasks have been attached to '
                'a Spinach Engine.'
            )

    def schedule(self, task_name: str, *args, **kwargs):
        """Schedule a job to be executed as soon as possible.

        :arg task_name: name of the task to execute in the background
        :arg args: args to be passed to the task function
        :arg kwargs: kwargs to be passed to the task function

        This method can only be used once tasks have been attached to a
        Spinach :class:`Engine`.
        """
        self._require_attached_tasks()
        self._spin.schedule(task_name, *args, **kwargs)

    def schedule_at(self, task_name: str, at: datetime, *args, **kwargs):
        """Schedule a job to be executed in the future.

        :arg task_name: name of the task to execute in the background
        :arg at: Date at which the job should start. It is advised to pass a
                 timezone aware datetime to lift any ambiguity. However if a
                 timezone naive datetime if given, it will be assumed to
                 contain UTC time.
        :arg args: args to be passed to the task function
        :arg kwargs: kwargs to be passed to the task function

        This method can only be used once tasks have been attached to a
        Spinach :class:`Engine`.
        """
        self._require_attached_tasks()
        self._spin.schedule_at(task_name, at, *args, **kwargs)

    def schedule_batch(self, batch: 'Batch'):
        """Schedule many jobs at once.

        Scheduling jobs in batches allows to enqueue them fast by avoiding
        round-trips to the broker.

        :arg batch: :class:`Batch` instance containing jobs to schedule
        """
        self._require_attached_tasks()
        self._spin.schedule_batch(batch)


class Batch:
    """Container allowing to schedule many jobs at once.

    Batching the scheduling of jobs allows to avoid doing many round-trips
    to the broker, reducing the overhead and the chance of errors associated
    with doing network calls.

    In this example 100 jobs are sent to Redis in one call:

    >>> batch = Batch()
    >>> for i in range(100):
    ...     batch.schedule('compute', i)
    ...
    >>> spin.schedule_batch(batch)

    Once the :class:`Batch` is passed to the :class:`Engine` it should be
    disposed off and not be reused.
    """

    def __init__(self):
        self.jobs_to_create = list()

    def schedule(self, task_name: str, *args, **kwargs):
        """Add a job to be executed ASAP to the batch.

        :arg task_name: name of the task to execute in the background
        :arg args: args to be passed to the task function
        :arg kwargs: kwargs to be passed to the task function
        """
        at = datetime.now(timezone.utc)
        self.schedule_at(task_name, at, *args, **kwargs)

    def schedule_at(self, task_name: str, at: datetime, *args, **kwargs):
        """Add a job to be executed in the future to the batch.

        :arg task_name: name of the task to execute in the background
        :arg at: Date at which the job should start. It is advised to pass a
                 timezone aware datetime to lift any ambiguity. However if a
                 timezone naive datetime if given, it will be assumed to
                 contain UTC time.
        :arg args: args to be passed to the task function
        :arg kwargs: kwargs to be passed to the task function
        """
        self.jobs_to_create.append((task_name, at, args, kwargs))


class RetryException(Exception):
    """Exception raised in a task to indicate that the job should be retried.

    Even if this exception is raised, the `max_retries` defined in the task
    still applies.

    :arg at: Optional date at which the job should be retried. If it is not
         given the job will be retried after a randomized exponential backoff.
         It is advised to pass a timezone aware datetime to lift any
         ambiguity. However if a timezone naive datetime if given, it will
         be assumed to contain UTC time.
    """

    def __init__(self, message, at: Optional[datetime]=None):
        super().__init__(message)
        self.at = at
