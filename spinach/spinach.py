from datetime import datetime, timezone
from logging import getLogger
import threading
import time
from typing import Optional

from .task import Task, Tasks
from .job import Job, JobStatus
from .brokers.base import Broker
from .const import DEFAULT_QUEUE, DEFAULT_NAMESPACE
from .worker import Workers


logger = getLogger(__name__)


class Spinach:

    def __init__(self, broker: Broker, namespace: str=DEFAULT_NAMESPACE):
        self._broker = broker
        self._broker.namespace = namespace
        self._namespace = namespace
        self._tasks = {}

        self._arbiter = None
        self._workers = None
        self._working_queue = None

        self._must_stop = threading.Event()

    @property
    def namespace(self) -> str:
        return self._namespace

    def attach_tasks(self, tasks: Tasks):
        self._tasks.update(tasks.tasks)
        tasks._spin = self

    def _get_task(self, name) -> Task:
        try:
            return self._tasks[name]
        except KeyError:
            raise ValueError(
                'Unknown task "{}". Known tasks: {}'
                .format(name, list(self._tasks.keys()))
            )

    def execute(self, task_name: str, *args, **kwargs):
        return self._get_task(task_name).func(*args, **kwargs)

    def schedule(self, task_name: str, *args, **kwargs):
        """Schedule a job to be executed as soon as possible.

        :arg task_name: Name to the task to execute
        """
        at = datetime.now(timezone.utc)
        return self.schedule_at(task_name, at, *args, **kwargs)

    def schedule_at(self, task_name: str, at: datetime, *args, **kwargs):
        """Schedule a job to be executed in the future.

        :arg task_name: Name to the task to execute
        :arg at: Date at which the job should start. It is advised to pass a
                 timezone aware datetime to lift any ambiguity. However if a
                 timezone naive datetime if given, it will be assumed to
                 contain UTC time.
        """
        task = self._get_task(task_name)
        job = Job(task.name, task.queue, at, task.max_retries, task_args=args,
                  task_kwargs=kwargs)
        return self._broker.enqueue_job(job)

    def _arbiter_func(self):
        logger.debug('Arbiter started')
        while not self._must_stop.is_set():

            self._broker.move_future_jobs()

            if self._workers.can_accept_job():
                job = self._broker.get_job_from_queue(self._working_queue)

                while job:
                    job.task_func = self._get_task(job.task_name).func
                    self._workers.submit_job(job)

                    if self._workers.can_accept_job():
                        job = self._broker.get_job_from_queue(
                            self._working_queue
                        )
                    else:
                        job = None

            logger.debug('Arbiter waiting for events')
            self._broker.wait_for_event()

        logger.debug('Arbiter terminated')

    def start_workers(self, number: int=5, queue=DEFAULT_QUEUE, block=True):
        if self._arbiter or self._workers:
            raise RuntimeError('Workers can only be started once')
        self._working_queue = queue

        # Start the broker
        self._broker.start()

        # Start workers
        self._workers = Workers(
            self._job_finished_callback,
            num_workers=number,
            namespace=self.namespace,
        )

        # Start the arbiter
        self._arbiter = threading.Thread(
            target=self._arbiter_func,
            name='{}-arbiter'.format(self.namespace)
        )
        self._arbiter.start()

        if block:
            try:
                while True:
                    time.sleep(20000)
            except KeyboardInterrupt:
                self.stop_workers()

    def stop_workers(self):
        self._must_stop.set()
        self._workers.stop()
        self._broker.stop()

    def _job_finished_callback(self, job: Job, err: Optional[Exception]):
        """Function called by a worker when a job is finished."""
        assert job.status is JobStatus.RUNNING
        self._broker.job_ran(job, err)
