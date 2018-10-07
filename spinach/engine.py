from datetime import datetime, timezone
from logging import getLogger
import threading
from typing import Optional

from .task import Tasks, Batch, RetryException
from .utils import human_duration, run_forever, exponential_backoff
from .job import Job, JobStatus
from .brokers.base import Broker
from .const import DEFAULT_QUEUE, DEFAULT_NAMESPACE
from .worker import Workers
from . import signals, exc


logger = getLogger(__name__)


class Engine:
    """Spinach Engine coordinating a broker with workers.

    :arg broker: instance of a :class:`Broker`
    :arg namespace: name of the namespace used by the Engine. When different
         Engines use the same Redis server, they must use different
         namespaces to isolate themselves.
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

    def execute(self, task_name: str, *args, **kwargs):
        return self._tasks.get(task_name).func(*args, **kwargs)

    def schedule(self, task_name: str, *args, **kwargs):
        """Schedule a job to be executed as soon as possible.

        :arg task_name: name of the task to execute
        :arg args: args to be passed to the task function
        :arg kwargs: kwargs to be passed to the task function
        """
        at = datetime.now(timezone.utc)
        return self.schedule_at(task_name, at, *args, **kwargs)

    def schedule_at(self, task_name: str, at: datetime, *args, **kwargs):
        """Schedule a job to be executed in the future.

        :arg task_name: name of the task to execute
        :arg at: date at which the job should start. It is advised to pass a
                 timezone aware datetime to lift any ambiguity. However if a
                 timezone naive datetime if given, it will be assumed to
                 contain UTC time.
        :arg args: args to be passed to the task function
        :arg kwargs: kwargs to be passed to the task function
        """
        task = self._tasks.get(task_name)
        job = Job(task.name, task.queue, at, task.max_retries, task_args=args,
                  task_kwargs=kwargs)
        return self._broker.enqueue_jobs([job])

    def schedule_batch(self, batch: Batch):
        """Schedule many jobs at once.

        Scheduling jobs in batches allows to enqueue them fast by avoiding
        round-trips to the broker.

        :arg batch: :class:`Batch` instance containing jobs to schedule
        """
        jobs = list()
        for task_name, at, args, kwargs in batch.jobs_to_create:
            task = self._tasks.get(task_name)
            jobs.append(
                Job(task.name, task.queue, at, task.max_retries,
                    task_args=args, task_kwargs=kwargs)
            )
        return self._broker.enqueue_jobs(jobs)

    def _arbiter_func(self, stop_when_queue_empty=False):
        logger.debug('Arbiter started')
        self._register_periodic_tasks()
        while not self._must_stop.is_set():

            self._broker.move_future_jobs()

            received_jobs = 0
            available_slots = self._workers.available_slots
            if available_slots > 0:
                jobs = self._broker.get_jobs_from_queue(
                    self._working_queue, available_slots
                )
                for job in jobs:
                    received_jobs += 1
                    try:
                        job.task_func = self._tasks.get(job.task_name).func
                    except exc.UnknownTask as err:
                        self._job_finished_callback(job, 0.0, err)
                    else:
                        self._workers.submit_job(job)

                if (stop_when_queue_empty and available_slots > 0
                        and received_jobs == 0):
                    logger.info("Stopping workers because queue '%s' is empty",
                                self._working_queue)
                    self.stop_workers(_join_arbiter=False)
                    logger.debug('Arbiter terminated')
                    return

            logger.debug('Received %s jobs, now waiting for events',
                         received_jobs)
            self._broker.wait_for_event()

        logger.debug('Arbiter terminated')

    def start_workers(self, number: int=5, queue=DEFAULT_QUEUE, block=True,
                      stop_when_queue_empty=False):
        """Start the worker threads.

        :arg number: number of worker threads to launch
        :arg queue: name of the queue to consume, see :doc:`queues`
        :arg block: whether to block the calling thread until a signal arrives
             and workers get terminated
        :arg stop_when_queue_empty: automatically stop the workers when the
             queue is empty. Useful mostly for one-off scripts and testing.
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
        self._workers = Workers(
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

        while not self._must_stop.is_set():
            out_object = self._workers.out_queue.get()
            if out_object is self._workers.poison_pill:
                break

            self._job_finished_callback(
                out_object.job, out_object.duration, out_object.error
            )

        logger.debug('Result notifier terminated')

    def _job_finished_callback(self, job: Job, duration: float,
                               err: Optional[Exception]):
        """Function called by a worker when a job is finished."""
        duration = human_duration(duration)
        if not err:
            job.status = JobStatus.SUCCEEDED
            logger.info('Finished execution of %s in %s', job, duration)
            try:
                self._broker.remove_job_from_running(job)
            except Exception as e:
                logger.warning('Could not remove %s from running jobs: %s',
                               job, e)
            return

        if job.should_retry:
            job.retries += 1
            if isinstance(err, RetryException) and err.at is not None:
                job.at = err.at
            else:
                job.at = (datetime.now(timezone.utc) +
                          exponential_backoff(job.retries))

            signals.job_schedule_retry.send(self._namespace, job=job, err=err)
            # No need to remove job from running, enqueue does it
            try:
                self._broker.enqueue_jobs([job])
            except Exception as e:
                logger.warning(
                    'Could not schedule the retry of %s, however it will be '
                    'retried when the broker is found dead: %s', job, e
                )
                return

            log_args = (
                job.retries, job.max_retries + 1, job, duration,
                human_duration(
                    (job.at - datetime.now(tz=timezone.utc)).total_seconds()
                )
            )
            if isinstance(err, RetryException):
                logger.info('Retry requested during execution %d/%d of %s '
                            'after %s, retry in %s', *log_args)
            else:
                logger.warning('Error during execution %d/%d of %s after %s, '
                               'retry in %s', *log_args)

            return

        job.status = JobStatus.FAILED
        signals.job_failed.send(self._namespace, job=job, err=err)
        logger.error(
            'Error during execution %d/%d of %s after %s',
            job.max_retries + 1, job.max_retries + 1, job, duration,
            exc_info=err
        )
        try:
            self._broker.remove_job_from_running(job)
        except Exception as e:
            logger.warning('Could not remove %s from running jobs: %s', job, e)

    def _register_periodic_tasks(self):
        periodic_tasks = [task for task in self._tasks.tasks.values()
                          if task.periodicity]
        self._broker.register_periodic_tasks(periodic_tasks)
