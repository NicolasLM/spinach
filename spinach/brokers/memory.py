from datetime import datetime, timezone
from logging import getLogger
from queue import Queue, Empty
import sched
import threading
import time
from typing import Optional, Iterable, List, Tuple, Dict, Union
import uuid

from .base import Broker
from ..job import Job, JobStatus
from ..task import Task

logger = getLogger('spinach.broker')


class MemoryBroker(Broker):

    def __init__(self):
        super().__init__()
        self._lock = threading.RLock()
        self._queues = dict()
        self._future_jobs = list()
        self._running_jobs = list()
        self._scheduler = sched.scheduler()
        self._max_concurrency_keys = dict()
        self._cur_concurrency_keys = dict()

    def _get_queue(self, queue_name: str):
        queue_name = self._to_namespaced(queue_name)
        with self._lock:
            try:
                return self._queues[queue_name]
            except KeyError:
                queue = Queue()
                self._queues[queue_name] = queue
                return queue

    def enqueue_jobs(self, jobs: Iterable[Job], from_failure: bool=False):
        """Enqueue a batch of jobs."""
        for job in jobs:
            with self._lock:
                if from_failure:
                    max_concurrency = self._max_concurrency_keys[
                        job.task_name
                    ]
                    if max_concurrency is not None:
                        self._cur_concurrency_keys[job.task_name] -= 1
            if job.should_start:
                job.status = JobStatus.QUEUED
                queue = self._get_queue(job.queue)
                queue.put(job.serialize())
            else:
                with self._lock:
                    job.status = JobStatus.WAITING
                    self._future_jobs.append(job.serialize())
                    self._future_jobs.sort(key=lambda j: Job.deserialize(j).at)
        self._something_happened.set()

    def move_future_jobs(self) -> int:
        num_jobs_moved = 0
        with self._lock:
            job = self._get_next_future_job()

            while job and job.should_start:
                job.status = JobStatus.QUEUED
                queue = self._get_queue(job.queue)
                queue.put(job.serialize())
                self._future_jobs.pop(0)
                num_jobs_moved += 1

                job = self._get_next_future_job()

            if num_jobs_moved < 0:
                # At least one job got enqueued so the flag must be set
                self._something_happened.set()

        # Create jobs from due periodic tasks
        self._scheduler.run(blocking=False)

        return num_jobs_moved

    def set_concurrency_keys(self, tasks: Iterable[Task]):
        for task in tasks:
            self._max_concurrency_keys[task.name] = task.max_concurrency
            self._cur_concurrency_keys[task.name] = 0

    def register_periodic_tasks(self, tasks: Iterable[Task]):
        """Register tasks that need to be scheduled periodically."""
        for task in tasks:
            self._scheduler.enter(
                int(task.periodicity.total_seconds()),
                0,
                self._schedule_periodic_task,
                argument=(task,)
            )

    def _schedule_periodic_task(self, task: Task):
        at = datetime.now(timezone.utc)
        job = Job(task.name, task.queue, at, task.max_retries)
        self.enqueue_jobs([job])
        self._scheduler.enter(
            int(task.periodicity.total_seconds()),
            0,
            self._schedule_periodic_task,
            argument=(task,)
        )

    @property
    def next_future_periodic_delta(self) -> Optional[float]:
        """Give the amount of seconds before the next periodic task is due."""
        try:
            next_event = self._scheduler.queue[0]
        except IndexError:
            return None

        now = time.monotonic()
        next_event_time = next_event[0]
        if next_event_time < now:
            return 0

        return next_event_time - now

    def inspect_periodic_tasks(self) -> List[Tuple[int, str]]:
        """Get the next periodic task schedule.

        Used only for debugging and during tests.
        """
        return [(int(e[0]), e[3][0].name) for e in self._scheduler.queue]

    def _get_next_future_job(self) -> Optional[Job]:
        with self._lock:
            try:
                return Job.deserialize(self._future_jobs[0])
            except IndexError:
                return None

    def is_queue_empty(self, queue: str):
        return self._get_queue(queue).qsize() == 0

    def get_jobs_from_queue(self, queue: str, max_jobs: int) -> List[Job]:
        """Get jobs from a queue."""
        rv = list()
        jobs_to_re_add = list()
        with self._lock:
            while len(rv) < max_jobs:
                try:
                    job_json_string = self._get_queue(queue).get(block=False)
                except Empty:
                    break

                job = Job.deserialize(job_json_string)
                max_concurrency = self._max_concurrency_keys.get(job.task_name)
                cur_concurrency = self._cur_concurrency_keys.get(job.task_name)
                if (
                    max_concurrency is not None and
                    cur_concurrency >= max_concurrency
                ):
                    jobs_to_re_add.append(job_json_string)

                else:
                    job.status = JobStatus.RUNNING
                    rv.append(job)
                    if max_concurrency is not None:
                        self._cur_concurrency_keys[job.task_name] += 1

            # Re-add jobs that could not be run due to max_concurrency
            # limits. Queue does not have a way to insert at the front, so
            # sadly they go straight to the back again. Given that
            # MemoryBroker is generally only used for testing, this should
            # not be a great hardship.
            logger.debug(
                "Re-adding %s jobs due to concurrency limits",
                len(jobs_to_re_add)
            )
            for job in jobs_to_re_add:
                self._get_queue(queue).put(job)

        return rv

    def flush(self):
        with self._lock:
            self._queues = dict()
            self._future_jobs = list()

    def get_all_brokers(self) -> List[Dict[str, Union[None, str, int]]]:
        # A memory broker is not connected to any other broker
        return [self._get_broker_info()]

    def enqueue_jobs_from_dead_broker(
        self, dead_broker_id: uuid.UUID
    ) -> Tuple[int, list]:
        # A memory broker cannot be dead
        return 0, []

    def remove_job_from_running(self, job: Job):
        """Remove a job from the list of running ones.

        Easy, the memory broker doesn't track running jobs. If the broker dies
        there is nothing we can do.

        We still need to decrement the current_concurrency count,
        however, if it exists.
        """
        with self._lock:
            max_concurrency = self._max_concurrency_keys[job.task_name]
            if max_concurrency is not None:
                self._cur_concurrency_keys[job.task_name] -= 1

        self._something_happened.set()
