from logging import getLogger
from queue import Queue
import threading
import time
from typing import Callable

from .job import Job
from .utils import human_duration
from . import signals

logger = getLogger(__name__)


class MaxUnfinishedQueue(Queue):
    """Queue considered full when it has too many unfinished tasks.

    This is to make sure that no job is pushed to the queue before a worker is
    actually free to take it.
    """

    def empty(self):
        with self.mutex:
            return not self._qsize() and not self.unfinished_tasks

    def full(self):
        with self.mutex:
            too_many_items = 0 < self.maxsize <= self._qsize()
            too_many_unfinished_tasks = self.maxsize <= self.unfinished_tasks
            return too_many_items or too_many_unfinished_tasks


class Workers:

    def __init__(self, job_finished_callback: Callable,
                 num_workers: int, namespace: str):
        self._queue = MaxUnfinishedQueue(maxsize=num_workers)
        self._namespace = namespace.format(namespace)
        self._job_finished_callback = job_finished_callback
        self._threads = list()

        # The event exists only to stop accepting jobs, workers are terminated
        # via the poison pill
        self._must_stop = threading.Event()
        self._poison_pill = object()

        for i in range(num_workers):
            thread = threading.Thread(
                target=self._worker_func,
                name='{}-worker-{}'.format(self._namespace, i)
            )
            thread.start()
            self._threads.append(thread)

    def _worker_func(self):
        worker_name = threading.current_thread().name
        logger.debug('Worker %s started', worker_name)
        signals.worker_started.send(self._namespace, worker_name=worker_name)

        while True:
            item = self._queue.get()

            if item is self._poison_pill:
                self._queue.task_done()
                self._queue.put(self._poison_pill)
                break

            job = item
            logger.info('Starting execution of %s', job)
            signals.job_started.send(self._namespace, job=job)
            start_time = time.monotonic()
            try:
                job.task_func(*job.task_args, **job.task_kwargs)
            except Exception as e:
                duration = human_duration(time.monotonic() - start_time)
                logger.exception('Error during execution of %s after %s', job,
                                 duration)
                self._job_finished_callback(job, e)
            else:
                duration = human_duration(time.monotonic() - start_time)
                logger.info('Finished execution of %s in %s', job, duration)
                self._job_finished_callback(job, None)
            finally:
                signals.job_finished.send(self._namespace, job=job)
                self._queue.task_done()

        logger.debug('Worker %s terminated', worker_name)
        signals.worker_terminated.send(self._namespace,
                                       worker_name=worker_name)

    def submit_job(self, job: Job):
        if self._must_stop.is_set():
            raise RuntimeError('Cannot submit job: workers are shutting down')
        self._queue.put(job)

    def can_accept_job(self) -> bool:
        """Tells whether a worker is available to take a job.

        Queue.full() is racy, but it should not be a problem here as jobs are
        only submitted by a single thread (the arbiter).
        """
        if self._queue.full():
            return False
        if self._must_stop.is_set():
            return False
        return True

    def stop(self):
        if self._must_stop.is_set():
            logger.warning('Workers are already shutting down')
            return
        logger.info('Stopping workers %s', self._namespace)
        self._must_stop.set()
        self._queue.join()
        self._queue.put(self._poison_pill)
        for thread in self._threads:
            thread.join()
        logger.debug('All workers %s stopped', self._namespace)
