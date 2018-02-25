from logging import getLogger
from queue import Queue, Empty
import threading
from typing import Optional, Iterable, List

from .base import Broker
from ..job import Job, JobStatus

logger = getLogger('spinach.broker')


class MemoryBroker(Broker):

    def __init__(self):
        super().__init__()
        self._lock = threading.RLock()
        self._queues = dict()
        self._future_jobs = list()
        self._running_jobs = list()

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
        return num_jobs_moved

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
