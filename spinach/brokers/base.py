from abc import ABC, abstractmethod
from datetime import datetime, timezone
from logging import getLogger
import threading
from typing import Optional, Iterable, List, Tuple
import uuid

from ..job import Job
from ..task import Task
from ..const import WAIT_FOR_EVENT_MAX_SECONDS

logger = getLogger('spinach.broker')


class Broker(ABC):

    def __init__(self):
        self._something_happened = threading.Event()
        self._namespace = None
        self._id = uuid.uuid4()

    def wait_for_event(self):
        next_future_job_delta = self.next_future_job_delta
        if next_future_job_delta is None:
            next_future_job_delta = WAIT_FOR_EVENT_MAX_SECONDS

        next_future_periodic_delta = self.next_future_periodic_delta
        if next_future_periodic_delta is None:
            next_future_periodic_delta = WAIT_FOR_EVENT_MAX_SECONDS

        timeout = min(
            next_future_job_delta,
            next_future_periodic_delta,
            WAIT_FOR_EVENT_MAX_SECONDS
        )
        if self._something_happened.wait(timeout=timeout):
            self._something_happened.clear()

    def start(self):
        """Start the broker.

        Only needed by arbiter.
        """

    def stop(self):
        """Stop the broker.

        Only needed by arbiter.
        """
        self._something_happened.set()

    @property
    def namespace(self) -> str:
        if not self._namespace:
            raise RuntimeError('Namespace must be set before using the broker')
        return self._namespace

    @namespace.setter
    def namespace(self, value: str):
        if self._namespace:
            raise RuntimeError('The namespace can only be set once')
        self._namespace = value

    def _to_namespaced(self, value: str) -> str:
        return '{}/{}'.format(self.namespace, value)

    @abstractmethod
    def register_periodic_tasks(self, tasks: Iterable[Task]):
        """Register tasks that need to be scheduled periodically."""

    @abstractmethod
    def inspect_periodic_tasks(self) -> List[Tuple[int, str]]:
        """Get the next periodic task schedule.

        Used only for debugging and during tests.
        """

    @abstractmethod
    def enqueue_jobs(self, jobs: Iterable[Job]):
        """Enqueue a batch of jobs."""

    @abstractmethod
    def remove_job_from_running(self, job: Job):
        """Remove a job from the list of running ones."""

    @abstractmethod
    def get_jobs_from_queue(self, queue: str, max_jobs: int) -> List[Job]:
        """Get jobs from a queue."""

    @abstractmethod
    def move_future_jobs(self) -> int:
        """Move ready jobs from the future queue to their normal queues.

        :returns the number of jobs moved
        """

    @abstractmethod
    def _get_next_future_job(self)-> Optional[Job]:
        """Get the next future job."""

    @property
    def next_future_job_delta(self) -> Optional[float]:
        """Give the amount of seconds before the next future job is due."""
        job = self._get_next_future_job()
        if not job:
            return None
        return (job.at - datetime.now(timezone.utc)).total_seconds()

    @property
    @abstractmethod
    def next_future_periodic_delta(self) -> Optional[float]:
        """Give the amount of seconds before the next periodic task is due."""

    @abstractmethod
    def flush(self):
        """Delete everything in the namespace."""

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, self._id)
