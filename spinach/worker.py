from collections import namedtuple
from logging import getLogger
from queue import Queue
import threading
import time

from .job import Job
from . import signals

logger = getLogger(__name__)
JobResult = namedtuple('JobResult', 'job, duration, error')


class MaxUnfinishedQueue(Queue):
    """Queue considered full when it has too many unfinished tasks.

    This is to make sure that no job is pushed to the queue before a worker is
    actually free to take it.
    """

    def empty(self):
        with self.mutex:
            return self.unfinished_tasks == 0

    def full(self):
        with self.mutex:
            return self.maxsize <= self.unfinished_tasks

    def available_slots(self) -> int:
        with self.mutex:
            return self.maxsize - self.unfinished_tasks


class Workers:

    def __init__(self, num_workers: int, namespace: str):
        self._in_queue = MaxUnfinishedQueue(maxsize=num_workers)
        self.out_queue = Queue()
        self._namespace = namespace.format(namespace)
        self._threads = list()

        # The event exists only to stop accepting jobs, workers are terminated
        # via the poison pill
        self._must_stop = threading.Event()
        self.poison_pill = object()

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
            item = self._in_queue.get()

            if item is self.poison_pill:
                self._in_queue.task_done()
                self._in_queue.put(self.poison_pill)
                break

            job = item
            logger.info('Starting execution of %s', job)
            signals.job_started.send(self._namespace, job=job)
            start_time = time.monotonic()
            try:
                job.task_func(*job.task_args, **job.task_kwargs)
            except Exception as e:
                duration = time.monotonic() - start_time
                self.out_queue.put(JobResult(job, duration, e))
            else:
                duration = time.monotonic() - start_time
                self.out_queue.put(JobResult(job, duration, None))
            finally:
                signals.job_finished.send(self._namespace, job=job)
                self._in_queue.task_done()

        logger.debug('Worker %s terminated', worker_name)
        signals.worker_terminated.send(self._namespace,
                                       worker_name=worker_name)

    def submit_job(self, job: Job):
        if self._must_stop.is_set():
            raise RuntimeError('Cannot submit job: workers are shutting down')
        self._in_queue.put(job)

    @property
    def available_slots(self) -> int:
        """Number of jobs the :class:`Workers` can accept.

        It may be racy, but it should not be a problem here as jobs are
        only submitted by a single thread (the arbiter).
        """
        return self._in_queue.available_slots()

    def can_accept_job(self) -> bool:
        return self.available_slots > 0

    def stop(self):
        if self._must_stop.is_set():
            logger.warning('Workers are already shutting down')
            return
        logger.info('Stopping workers %s', self._namespace)
        self._must_stop.set()
        self._in_queue.join()
        self._in_queue.put(self.poison_pill)
        for thread in self._threads:
            thread.join()
        self.out_queue.put(self.poison_pill)
        logger.debug('All workers %s stopped', self._namespace)
