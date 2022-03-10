from abc import ABC, abstractmethod
from datetime import datetime, timezone
from logging import getLogger
import platform
import threading
import time
from typing import Optional, Iterable, List, Tuple, Dict, Union
import uuid

from ..job import Job
from ..task import Task
from ..const import WAIT_FOR_EVENT_MAX_SECONDS

logger = getLogger('spinach.broker')


class Broker(ABC):

    def __init__(self):
        # Event that is set whenever:
        #   - a job is enqueued in the main queue (to allow to fetch it)
        #   - a job has been finished (to allow to fetch a new one)
        #   - a future job is put in the waiting queue (to move it)
        #   - the broker is stopping
        # It allows the Engine to wait for these things.
        self._something_happened = threading.Event()

        self._namespace = None
        self._id = uuid.uuid4()
        self._broker_info = {
            'id': str(self._id),
            'name': platform.node(),
            'started_at': int(time.time())
        }

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
        self._broker_info['namespace'] = value

    def _to_namespaced(self, value: str) -> str:
        return '{}/{}'.format(self.namespace, value)

    @abstractmethod
    def register_periodic_tasks(self, tasks: Iterable[Task]):
        """Register tasks that need to be scheduled periodically."""

    @abstractmethod
    def set_concurrency_keys(self, tasks: Iterable[Task]):
        """Register concurrency data for Tasks.

        Set up anything in the Broker that is required to track
        concurrency on Tasks, where a Task defines max_concurrency.
        """

    @abstractmethod
    def is_queue_empty(self, queue: str) -> bool:
        """Return True if the provided queue is empty."""

    @abstractmethod
    def inspect_periodic_tasks(self) -> List[Tuple[int, str]]:
        """Get the next periodic task schedule.

        Used only for debugging and during tests.
        """

    @abstractmethod
    def enqueue_jobs(self, jobs: Iterable[Job], from_failure: bool):
        """Enqueue a batch of jobs."""

    @abstractmethod
    def remove_job_from_running(self, job: Job):
        """Remove a job from the list of running ones."""

    @abstractmethod
    def get_jobs_from_queue(self, queue: str, max_jobs: int) -> List[Job]:
        """Get jobs from a queue."""

    @abstractmethod
    def move_future_jobs(self) -> int:
        """Perform periodic management of the broker and the queues.

        This method originally only moved future jobs, but it expanded to
        perform other actions related to maintenance of brokers' data:
        - Moves ready jobs from the future queue to their normal queues
        - Enqueue periodic tasks that are due
        - Perform broker keepalive

        Note: This method may be called very often. In the future it would be
        preferable to decouple it from the retrieval of jobs from the queue.

        :returns the number of jobs moved
        """

    @abstractmethod
    def _get_next_future_job(self) -> Optional[Job]:
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

    @abstractmethod
    def get_all_brokers(self) -> List[Dict[str, Union[None, str, int]]]:
        """Return all registered brokers."""

    @abstractmethod
    def enqueue_jobs_from_dead_broker(
        self, dead_broker_id: uuid.UUID
    ) -> Tuple[int, list]:
        """Re-enqueue the jobs that were running on a broker.

        Only jobs that can be retired are moved back to the queue, the others
        are lost as expected.

        Both the current broker and the dead one must use the same namespace.

        This method is called automatically on brokers that are identified
        as dead by Spinach but it can also be used by user's code.
        If someone has a better system to detect dead processes (monitoring,
        Consul, etcd...) this method can be called with the ID of the dead
        broker to re-enqueue jobs before Spinach notices that the broker is
        actually dead, which takes 30 minutes by default.

        :param dead_broker_id: UUID of the dead broker.
        :return: Number of jobs that were moved back to the queue.
        """

    def _get_broker_info(self) -> Dict[str, Union[None, str, int]]:
        rv = self._broker_info.copy()
        rv['last_seen_at'] = int(time.time())
        return rv

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, self._id)
