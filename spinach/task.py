from datetime import datetime, timezone, timedelta
import functools
import json
from typing import Iterable, Optional, Callable, List, TYPE_CHECKING, Union
from numbers import Number

from . import const, exc

if TYPE_CHECKING:
    from .job import Job


class Task:

    __slots__ = [
        'func', 'name', 'queue', 'max_retries', 'periodicity',
        'max_concurrency',
    ]

    def __init__(self, func: Callable, name: str, queue: str,
                 max_retries: Number, periodicity: Optional[timedelta],
                 max_concurrency: Optional[int]=None):
        self.func = func
        self.name = name
        self.queue = queue
        self.max_retries = max_retries
        self.periodicity = periodicity
        self.max_concurrency = max_concurrency

        # Prevent initialisation with max_concurrency set and
        # max_retries not set.
        if max_concurrency is not None:
            if max_retries is None or max_retries == 0:
                raise ValueError(
                    "max_retries must be set if max_concurrency is set"
                )
            if max_concurrency < 1:
                raise ValueError("max_concurrency must be greater than zero")

    def serialize(self):
        periodicity = (int(self.periodicity.total_seconds())
                       if self.periodicity else None)
        return json.dumps({
            'name': self.name,
            'queue': self.queue,
            'max_retries': self.max_retries,
            'periodicity': periodicity,
            'max_concurrency': self.max_concurrency or -1,
        }, sort_keys=True)

    @property
    def task_name(self):
        return self.name

    def __repr__(self):
        return 'Task({}, {}, {}, {}, {}, {})'.format(
            self.func, self.name, self.queue, self.max_retries,
            self.periodicity, self.max_concurrency,
        )

    def __eq__(self, other):
        for attr in self.__slots__:
            try:
                if not getattr(self, attr) == getattr(other, attr):
                    return False
            except AttributeError:
                return False
        return True


Schedulable = Union[str, Callable, Task]


class Tasks:
    """Registry for tasks to be used by Spinach.

    :arg queue: default queue for tasks
    :arg max_retries: default retry policy for tasks
    :arg periodicity: for periodic tasks, delay between executions as a
         timedelta
    :arg max_concurrency: maximum number of simultaneous Jobs that can be
        started for this Task. Requires max_retries to be also set.
    """
    # This class is not thread-safe because it doesn't need to be used
    # concurrently.

    def __init__(self, queue: Optional[str]=None,
                 max_retries: Optional[Number]=None,
                 periodicity: Optional[timedelta]=None,
                 max_concurrency: Optional[int]=None):
        self._tasks = {}
        self.queue = queue
        self.max_retries = max_retries
        self.periodicity = periodicity
        self.max_concurrency = max_concurrency
        self._spin = None

    def update(self, tasks: 'Tasks'):
        self._tasks.update(tasks.tasks)

    @property
    def names(self) -> List[str]:
        return list(self._tasks.keys())

    @property
    def tasks(self) -> dict:
        return self._tasks

    def get(self, name: Schedulable) -> Task:
        try:
            task_name = name.task_name
        except AttributeError:
            task_name = name
        task = self._tasks.get(task_name)
        if task is not None:
            return task

        raise exc.UnknownTask(
            'Unknown task "{}", known tasks: {}'.format(name, self.names)
        )

    def task(self, func: Optional[Callable]=None, name: Optional[str]=None,
             queue: Optional[str]=None, max_retries: Optional[Number]=None,
             periodicity: Optional[timedelta]=None,
             max_concurrency: Optional[int]=None):
        """Decorator to register a task function.

        :arg name: name of the task, used later to schedule jobs
        :arg queue: queue of the task, the default is used if not provided
        :arg max_retries: maximum number of retries, the default is used if
             not provided
        :arg periodicity: for periodic tasks, delay between executions as a
             timedelta
        :arg max_concurrency: maximum number of simultaneous Jobs that can be
            started for this Task. Requires max_retries to be also set.

        >>> tasks = Tasks()
        >>> @tasks.task(name='foo')
        >>> def foo():
        ...    pass
        """
        if func is None:
            return functools.partial(self.task, name=name, queue=queue,
                                     max_retries=max_retries,
                                     periodicity=periodicity,
                                     max_concurrency=max_concurrency)

        self.add(func, name=name, queue=queue, max_retries=max_retries,
                 periodicity=periodicity, max_concurrency=max_concurrency)

        # Add an attribute to the function to be able to conveniently use it as
        # spin.schedule(function) instead of spin.schedule('task_name')
        func.task_name = name

        return func

    def add(self, func: Callable, name: Optional[str]=None,
            queue: Optional[str]=None, max_retries: Optional[Number]=None,
            periodicity: Optional[timedelta]=None,
            max_concurrency: Optional[int]=None):
        """Register a task function.

        :arg func: a callable to be executed
        :arg name: name of the task, used later to schedule jobs
        :arg queue: queue of the task, the default is used if not provided
        :arg max_retries: maximum number of retries, the default is used if
             not provided
        :arg periodicity: for periodic tasks, delay between executions as a
             timedelta
        :arg max_concurrency: maximum number of simultaneous Jobs that can be
            started for this Task. Requires max_retries to be also set.

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
        if max_concurrency is None:
            max_concurrency = self.max_concurrency

        if queue and queue.startswith('_'):
            raise ValueError('Queues starting with "_" are reserved by '
                             'Spinach for internal use')

        self._tasks[name] = Task(
            func, name, queue, max_retries, periodicity, max_concurrency
        )

    def _require_attached_tasks(self):
        if self._spin is None:
            raise RuntimeError(
                'Cannot execute tasks until the tasks have been attached to '
                'a Spinach Engine.'
            )

    def schedule(self, task: Schedulable, *args, **kwargs) -> "Job":
        """Schedule a job to be executed as soon as possible.

        :arg task: the task or its name to execute in the background
        :arg args: args to be passed to the task function
        :arg kwargs: kwargs to be passed to the task function

        :return: The Job that was created and scheduled.

        This method can only be used once tasks have been attached to a
        Spinach :class:`Engine`.
        """
        self._require_attached_tasks()
        return self._spin.schedule(task, *args, **kwargs)

    def schedule_at(
        self, task: Schedulable, at: datetime, *args, **kwargs
    ) -> "Job":
        """Schedule a job to be executed in the future.

        :arg task: the task or its name to execute in the background
        :arg at: Date at which the job should start. It is advised to pass a
                 timezone aware datetime to lift any ambiguity. However if a
                 timezone naive datetime if given, it will be assumed to
                 contain UTC time.
        :arg args: args to be passed to the task function
        :arg kwargs: kwargs to be passed to the task function

        :return: The Job that was created and scheduled.

        This method can only be used once tasks have been attached to a
        Spinach :class:`Engine`.
        """
        self._require_attached_tasks()
        return self._spin.schedule_at(task, at, *args, **kwargs)

    def schedule_batch(self, batch: 'Batch') -> Iterable["Job"]:
        """Schedule many jobs at once.

        Scheduling jobs in batches allows to enqueue them fast by avoiding
        round-trips to the broker.

        :arg batch: :class:`Batch` instance containing jobs to schedule

        :return: The Jobs that were created and scheduled.
        """
        self._require_attached_tasks()
        return self._spin.schedule_batch(batch)


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

    def schedule(self, task: Schedulable, *args, **kwargs):
        """Add a job to be executed ASAP to the batch.

        :arg task: the task or its name to execute in the background
        :arg args: args to be passed to the task function
        :arg kwargs: kwargs to be passed to the task function
        """
        at = datetime.now(timezone.utc)
        self.schedule_at(task, at, *args, **kwargs)

    def schedule_at(self, task: Schedulable, at: datetime, *args, **kwargs):
        """Add a job to be executed in the future to the batch.

        :arg task: the task or its name to execute in the background
        :arg at: Date at which the job should start. It is advised to pass a
                 timezone aware datetime to lift any ambiguity. However if a
                 timezone naive datetime if given, it will be assumed to
                 contain UTC time.
        :arg args: args to be passed to the task function
        :arg kwargs: kwargs to be passed to the task function
        """
        self.jobs_to_create.append((task, at, args, kwargs))


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


class AbortException(Exception):
    """Exception raised in a task to indicate that the job should NOT be
    retried.

    If this exception is raised, all retry attempts are stopped immediately.
    """
