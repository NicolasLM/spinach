from datetime import datetime, timezone
from logging import getLogger
import threading
from typing import Iterable, Type

from .task import Tasks, Batch, Schedulable
from .utils import run_forever, handle_sigterm
from .job import Job, JobStatus, advance_job_status
from .brokers.base import Broker
from .const import DEFAULT_QUEUE, DEFAULT_NAMESPACE, DEFAULT_WORKER_NUMBER
from .worker import BaseWorkers, ThreadWorkers
from . import exc


logger = getLogger(__name__)


class Engine:
    """Spinach Engine coordinating a broker with workers.

    This class does the orchestration of all components, it is the one that
    starts and terminates the whole machinery.

    The Engine can be run in two modes:

    - client: synchronously submits jobs.
    - worker: asynchronously executes jobs.

    Submitting jobs is quite easy, so running the Engine in client mode doesn't
    require spawning any thread.

    Executing jobs however is a bit more involved, so running the Engine in
    worker mode ends up spawning a few threads:

    - a few worker threads: they are only responsible for executing the task
      function and advancing the job status once it is finished.
    - a result notifier thread: sends back the result of job executions to the
      Broker backend, acts basically as a client.
    - an arbiter thread: fetches jobs from the Broker and gives them to the
      workers as well as doing some periodic bookkeeping.
    - a Broker subscriber thread: receives notifications from the backend when
      something happens, typically a job is enqueued.
    - the process main thread: starts all the above threads, then does nothing
      waiting for the signal to terminate the threads it started.

    This means that a Spinach worker process has at least 5 threads.

    :arg broker: instance of a :class:`Broker`
    :arg namespace: name of the namespace used by the Engine. When different
     Engines use the same Redis server, they must use different namespaces to
     isolate themselves.
    """

    def __init__(self, broker: Broker, namespace: str=DEFAULT_NAMESPACE):
        self._broker = broker
        self._broker.namespace = namespace
        self._namespace = namespace

        self._tasks = Tasks()
        self.task = self._tasks.task
        self._reset()

    def _reset(self):
        """Initialization that must happen before the arbiter is (re)started"""
        self._arbiter = None
        self._workers = None
        self._working_queue = None
        self._must_stop = threading.Event()

    @property
    def namespace(self) -> str:
        """Namespace the Engine uses."""
        return self._namespace

    def attach_tasks(self, tasks: Tasks):
        """Attach a set of tasks.

        A task cannot be scheduled or executed before it is attached to an
        Engine.

        >>> tasks = Tasks()
        >>> spin.attach_tasks(tasks)
        """
        if tasks._spin is not None and tasks._spin is not self:
            logger.warning('Tasks already attached to a different Engine')
        self._tasks.update(tasks)
        tasks._spin = self

    def execute(self, task: Schedulable, *args, **kwargs):
        return self._tasks.get(task).func(*args, **kwargs)

    def schedule(self, task: Schedulable, *args, **kwargs) -> Job:
        """Schedule a job to be executed as soon as possible.

        :arg task: the task or its name to execute in the background
        :arg args: args to be passed to the task function
        :arg kwargs: kwargs to be passed to the task function

        :return: The Job that was created and scheduled.
        """
        at = datetime.now(timezone.utc)
        return self.schedule_at(task, at, *args, **kwargs)

    def schedule_at(
        self, task: Schedulable, at: datetime, *args, **kwargs
    ) -> Job:
        """Schedule a job to be executed in the future.

        :arg task: the task or its name to execute in the background
        :arg at: date at which the job should start. It is advised to pass a
                 timezone aware datetime to lift any ambiguity. However if a
                 timezone naive datetime if given, it will be assumed to
                 contain UTC time.
        :arg args: args to be passed to the task function
        :arg kwargs: kwargs to be passed to the task function

        :return: The Job that was created and scheduled.
        """
        task = self._tasks.get(task)
        job = Job(task.name, task.queue, at, task.max_retries, task_args=args,
                  task_kwargs=kwargs)
        job.task_func = task.func
        job.check_signature()
        self._broker.enqueue_jobs([job])
        return job

    def schedule_batch(self, batch: Batch) -> Iterable[Job]:
        """Schedule many jobs at once.

        Scheduling jobs in batches allows to enqueue them fast by avoiding
        round-trips to the broker.

        :arg batch: :class:`Batch` instance containing jobs to schedule

        :return: The Jobs that were created and scheduled.
        """
        jobs = list()
        for task, at, args, kwargs in batch.jobs_to_create:
            task = self._tasks.get(task)
            job = Job(
                task.name, task.queue, at, task.max_retries,
                task_args=args, task_kwargs=kwargs
            )
            job.task_func = task.func
            job.check_signature()
            jobs.append(job)

        self._broker.enqueue_jobs(jobs)
        return jobs

    def _arbiter_func(self, stop_when_queue_empty=False):
        logger.debug('Arbiter started')
        self._register_periodic_tasks()
        self._broker.set_concurrency_keys(
            [task for task in self._tasks.tasks.values()]
        )
        while not self._must_stop.is_set():

            self._broker.move_future_jobs()

            received_jobs = 0
            available_slots = self._workers.available_slots
            logger.debug("Available slots: %s", available_slots)
            if available_slots > 0:
                logger.debug("Getting jobs from queue %s", self._working_queue)
                jobs = self._broker.get_jobs_from_queue(
                    self._working_queue, available_slots
                )
                for job in jobs:
                    logger.debug("Received job: %s", job)
                    received_jobs += 1
                    try:
                        job.task_func = self._tasks.get(job.task_name).func
                    except exc.UnknownTask as err:
                        # This is slightly cheating, when a task is unknown
                        # it doesn't go to workers but is still sent to the
                        # workers out_queue so that it is processed by the
                        # notifier.
                        advance_job_status(self.namespace, job, 0.0, err)
                        self._workers.out_queue.put(job)
                    else:
                        self._workers.submit_job(job)

                if (stop_when_queue_empty and available_slots > 0
                        and received_jobs == 0
                        and self._broker.is_queue_empty(self._working_queue)):
                    logger.info("Stopping workers because queue '%s' is empty",
                                self._working_queue)
                    self.stop_workers(_join_arbiter=False)
                    logger.debug('Arbiter terminated')
                    return

            logger.debug('Received %s jobs, now waiting for events',
                         received_jobs)
            self._broker.wait_for_event()

        logger.debug('Arbiter terminated')

    def start_workers(self, number: int = DEFAULT_WORKER_NUMBER,
                      queue: str = DEFAULT_QUEUE, block: bool = True,
                      stop_when_queue_empty=False,
                      workers_class: Type[BaseWorkers] = ThreadWorkers):
        """Start the worker threads.

        :arg number: number of workers to launch, each job running uses one
                     worker.
        :arg queue: name of the queue to consume, see :doc:`queues`.
        :arg block: whether to block the calling thread until a signal arrives
             and workers get terminated.
        :arg stop_when_queue_empty: automatically stop the workers when the
             queue is empty. Useful mostly for one-off scripts and testing.
        :arg worker_class: Class to change the behavior of workers,
             defaults to threaded workers
        """
        if self._arbiter or self._workers:
            raise RuntimeError('Workers are already running')

        self._working_queue = queue

        tasks_names = '\n'.join(
            ['  - ' + task.name for task in self._tasks.tasks.values()
             if task.queue == self._working_queue]
        )
        logger.info('Starting %d workers on queue "%s" with tasks:\n%s',
                    number, self._working_queue, tasks_names)

        # Start the broker
        self._broker.start()

        # Start workers
        self._workers = workers_class(
            num_workers=number,
            namespace=self.namespace,
        )

        # Start the result notifier
        self._result_notifier = threading.Thread(
            target=run_forever,
            args=(self._result_notifier_func, self._must_stop, logger),
            name='{}-result-notifier'.format(self.namespace)
        )
        self._result_notifier.start()

        # Start the arbiter
        self._arbiter = threading.Thread(
            target=run_forever,
            args=(self._arbiter_func, self._must_stop, logger,
                  stop_when_queue_empty),
            name='{}-arbiter'.format(self.namespace)
        )
        self._arbiter.start()

        if block:
            with handle_sigterm():
                try:
                    self._arbiter.join()
                except KeyboardInterrupt:
                    self.stop_workers()
                except AttributeError:
                    # Arbiter thread starts and stops immediately when ran with
                    # `stop_when_queue_empty` and queue is already empty.
                    pass

    def stop_workers(self, _join_arbiter=True):
        """Stop the workers and wait for them to terminate."""
        # _join_arbiter is used internally when the arbiter is shutting down
        # the full engine itself. This is because the arbiter thread cannot
        # join itself.
        self._must_stop.set()
        self._workers.stop()
        self._result_notifier.join()
        self._broker.stop()
        if _join_arbiter:
            self._arbiter.join()
        self._reset()

    def _result_notifier_func(self):
        logger.debug('Result notifier started')

        while True:
            job = self._workers.out_queue.get()
            if job is self._workers.poison_pill:
                break

            if job.status in (JobStatus.SUCCEEDED, JobStatus.FAILED):
                self._broker.remove_job_from_running(job)
            elif job.status is JobStatus.NOT_SET:
                self._broker.enqueue_jobs([job], from_failure=True)
            else:
                raise RuntimeError('Received job with an incorrect status')

        logger.debug('Result notifier terminated')

    def _register_periodic_tasks(self):
        periodic_tasks = [task for task in self._tasks.tasks.values()
                          if task.periodicity]
        self._broker.register_periodic_tasks(periodic_tasks)
