from datetime import datetime, timezone
from logging import getLogger
from queue import Queue, Empty
import sched
import threading
import time
from typing import Optional, Iterable, List, Tuple

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

    def _get_queue(self, queue_name: str):
        queue_name = self._to_namespaced(queue_name)
        with self._lock:
            try:
                return self._queues[queue_name]
            except KeyError:
                queue = Queue()
                self._queues[queue_name] = queue
                return queue

    def enqueue_jobs(self, jobs: Iterable[Job]):
        """Enqueue a batch of jobs."""
        for job in jobs:
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
                self._something_happened.set()
                num_jobs_moved += 1

                job = self._get_next_future_job()
        self._scheduler.run(blocking=False)
        return num_jobs_moved

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

    def _get_next_future_job(self)-> Optional[Job]:
        with self._lock:
            try:
                return Job.deserialize(self._future_jobs[0])
            except IndexError:
                return None

    def get_jobs_from_queue(self, queue: str, max_jobs: int) -> List[Job]:
        """Get jobs from a queue."""
        rv = list()
        while len(rv) < max_jobs:
            try:
                job_json_string = self._get_queue(queue).get(block=False)
            except Empty:
                break

            job = Job.deserialize(job_json_string)
            job.status = JobStatus.RUNNING
            rv.append(job)

        return rv

    def flush(self):
        with self._lock:
            self._queues = dict()
            self._future_jobs = list()

    def remove_job_from_running(self, job: Job):
        """Remove a job from the list of running ones.

        Easy, the memory broker doesn't track running jobs. If the broker dies
        there is nothing we can do.
        """
        self._something_happened.set()
