from abc import ABC
import asyncio
from logging import getLogger
try:
    from queue import SimpleQueue
except ImportError:
    # For Python 3.6
    from queue import Queue as SimpleQueue
import sys
import threading
import time
from typing import List

from . import signals
from .job import Job, advance_job_status
from .queuey import Queuey

logger = getLogger(__name__)


class BaseWorkers(ABC):
    """Base class for Spinach workers.

    The Workers class receives jobs from the Engine via the `submit_job`
    method and sends back results to the Engine via the `out_queue`.

    Children classes are responsible for taking jobs from the in_queue,
    executing them and putting results in the out_queue.
    """

    def __init__(self, num_workers: int, namespace: str):
        if num_workers <= 0:
            raise ValueError('num_workers must be at least 1')

        self._num_workers = num_workers
        self._namespace = namespace.format(namespace)

        # List containing worker threads
        self._threads: List[threading.Thread] = list()

        # in_queue receives Job objects to execute
        # out_queue send Job objects after execution
        self._in_queue = Queuey(maxsize=self._num_workers)
        self.out_queue = SimpleQueue()

        # The event exists only to stop accepting jobs, workers are terminated
        # via the poison pill
        self._must_stop = threading.Event()
        self.poison_pill = object()

    def submit_job(self, job: Job):
        if self._must_stop.is_set():
            raise RuntimeError('Cannot submit job: workers are shutting down')

        self._in_queue.put_sync(job)

    @property
    def available_slots(self) -> int:
        """Number of jobs the :class:`BaseWorkers` can accept.

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
        self._in_queue.put_sync(self.poison_pill)
        for thread in self._threads:
            thread.join()
        self.out_queue.put(self.poison_pill)
        logger.debug('All workers %s stopped', self._namespace)


class ThreadWorkers(BaseWorkers):
    """Thread pool based workers.

    Launches a pool of `num_workers` threads, each executing a single job
    at once.
    """

    def __init__(self, num_workers: int, namespace: str):
        super().__init__(num_workers, namespace)
        for i in range(1, self._num_workers + 1):
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
            item = self._in_queue.get_sync()

            if item is self.poison_pill:
                self._in_queue.task_done()
                self._in_queue.put_sync(self.poison_pill)
                break

            job = item
            logger.info('Starting execution of %s', job)
            signals.job_started.send(self._namespace, job=job)
            start_time = time.monotonic()
            try:
                job.task_func(*job.task_args, **job.task_kwargs)
            except Exception as e:
                duration = time.monotonic() - start_time
                advance_job_status(self._namespace, job, duration, e)
            else:
                duration = time.monotonic() - start_time
                advance_job_status(self._namespace, job, duration, None)
            finally:
                signals.job_finished.send(self._namespace, job=job)
                self.out_queue.put(job)
                self._in_queue.task_done()

        logger.debug('Worker %s terminated', worker_name)
        signals.worker_terminated.send(self._namespace,
                                       worker_name=worker_name)


class AsyncioWorkers(BaseWorkers):
    """Asyncio based workers.

    Launches a single thread that runs `num_workers` asyncio coroutines at
    once. The sync part of Spinach (Engine, Broker...) interfaces with
    the asyncio loop in the worker thread via Janus queues that can be used
    both from sync and async code.
    """

    def __init__(self, num_workers: int, namespace: str):
        # Python 3.6 misses a few asyncio features that make it a pain to
        # support. Projects using asyncio are most likely already using the
        # latest version of Python.
        if sys.version_info < (3, 7):
            raise Exception("Spinach asyncio workers require Python 3.7+")

        super().__init__(num_workers, namespace)
        self._threads.append(threading.Thread(
            target=self._sync_interface_func,
            name='{}-asyncio-worker'.format(self._namespace)
        ))
        self._threads[0].start()

    def _sync_interface_func(self):
        worker_name = threading.current_thread().name
        logger.debug('Worker %s started', worker_name)
        signals.worker_started.send(self._namespace, worker_name=worker_name)

        asyncio.run(self._async_interface_func())

        logger.debug('Worker %s terminated', worker_name)
        signals.worker_terminated.send(self._namespace,
                                       worker_name=worker_name)

    async def _async_interface_func(self):
        worker_futures = list()
        for _ in range(1, self._num_workers + 1):
            worker_futures.append(
                asyncio.ensure_future(self._worker_func())
            )

        await asyncio.gather(*worker_futures)
        loop = asyncio.get_running_loop()
        await loop.shutdown_default_executor()

    async def _worker_func(self):
        while True:
            item = await self._in_queue.get_async()

            if item is self.poison_pill:
                self._in_queue.task_done()
                await self._in_queue.put_async(self.poison_pill)
                break

            job = item
            logger.info('Starting execution of %s', job)
            signals.job_started.send(self._namespace, job=job)
            start_time = time.monotonic()
            try:
                await job.task_func(*job.task_args, **job.task_kwargs)
            except Exception as e:
                duration = time.monotonic() - start_time
                advance_job_status(self._namespace, job, duration, e)
            else:
                duration = time.monotonic() - start_time
                advance_job_status(self._namespace, job, duration, None)
            finally:
                signals.job_finished.send(self._namespace, job=job)
                self.out_queue.put(job)
                self._in_queue.task_done()
