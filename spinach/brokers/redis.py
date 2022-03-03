from datetime import datetime, timezone
import json
from logging import getLogger
import math
from os import path
import socket
import threading
from typing import Optional, Iterable, List, Tuple, Dict, Union
import uuid

from redis import StrictRedis, ConnectionError, TimeoutError
try:  # Redis < 4.1.0
    from redis.client import Script
except ImportError:  # Redis >= 4.1.0
    from redis.commands.core import Script

from ..brokers.base import Broker
from ..job import Job, JobStatus, advance_job_status
from ..task import Task
from ..const import (
    FUTURE_JOBS_KEY, NOTIFICATIONS_KEY, RUNNING_JOBS_KEY,
    PERIODIC_TASKS_HASH_KEY, PERIODIC_TASKS_QUEUE_KEY,
    DEFAULT_ENQUEUE_JOB_RETRIES, ALL_BROKERS_HASH_KEY, ALL_BROKERS_ZSET_KEY,
    MAX_CONCURRENCY_KEY, CURRENT_CONCURRENCY_KEY,
)
from ..utils import run_forever, call_with_retry


logger = getLogger('spinach.broker')
here = path.abspath(path.dirname(__file__))


class RedisBroker(Broker):

    #: How often to check if the subscriber thread must stop, in seconds.
    #: This is a trade off between responsiveness when stopping a worker
    #: and wasted CPU cycles.
    must_stop_periodicity = 1

    #: How long before a broker that did not send a keepalive is
    #: considered dead and its running jobs are automatically re-enqueued.
    #: 30 minutes by default may seem long, but it is a trade off between
    #: reactivity to reschedule jobs and risk of running the same job
    #: twice in parallel in case of network partition.
    broker_dead_threshold_seconds = 1800

    def __init__(self, redis: Optional[StrictRedis]=None,
                 enqueue_job_max_retries: int=DEFAULT_ENQUEUE_JOB_RETRIES):
        super().__init__()
        self._r = redis if redis else StrictRedis(**recommended_socket_opts)
        self.enqueue_job_max_retries = enqueue_job_max_retries

        # Register the lua scripts
        self._idempotency_protected_scripts = list()
        self._deregister = self._load_script('deregister.lua')
        self._enqueue_job = self._load_script('enqueue_job.lua')
        self._enqueue_jobs_from_dead_broker = self._load_script(
            'enqueue_jobs_from_dead_broker.lua'
        )
        self._flush = self._load_script('flush.lua')
        self._get_jobs_from_queue = self._load_script(
            'get_jobs_from_queue.lua'
        )
        self._remove_job_from_running = self._load_script(
            'remove_job_from_running.lua'
        )
        self._move_future_jobs = self._load_script('move_future_jobs.lua')
        self._register_periodic_tasks = self._load_script(
            'register_periodic_tasks.lua'
        )
        self._set_concurrency_keys = self._load_script(
            'set_concurrency_keys.lua'
        )
        self._reset()

    def _reset(self):
        """Initialization that must happen before the broker is (re)started."""
        self._subscriber_thread = None
        self._must_stop = threading.Event()
        self._number_periodic_tasks = 0

    def _load_script(self, filename: str) -> Script:
        """Load a Lua script.

        Read the Lua script file to generate its Script object. If the script
        starts with a magic string, add it to the list of scripts requiring an
        idempotency token to execute.
        """
        with open(path.join(here, 'redis_scripts', filename), mode='rb') as f:
            script_data = f.read()
        rv = self._r.register_script(script_data)
        if script_data.startswith(b'-- idempotency protected script'):
            self._idempotency_protected_scripts.append(rv)
        return rv

    def _run_script(self, script: Script, *args):
        if script not in self._idempotency_protected_scripts:
            return script(args=args)

        # Script is protected by idempotency token, can be retried safely.
        # Insert idempotency token as first argument.
        idempotency_token = generate_idempotency_token()
        args = list(args)
        args.insert(
            0, self._to_namespaced('idempotency_{}'.format(idempotency_token))
        )
        rv = call_with_retry(
            script,
            (ConnectionError, TimeoutError),
            self.enqueue_job_max_retries,
            logger,
            args=args
        )
        if rv == -1:
            logger.info('Script not reprocessed because it was '
                        'already processed once')

        return rv

    def is_queue_empty(self, queue: str) -> bool:
        """Return True if the provided queue is empty."""
        return self._r.llen(self._to_namespaced(queue)) == 0

    def enqueue_jobs(self, jobs: Iterable[Job], from_failure: bool=False):
        """Enqueue a batch of jobs."""
        jobs_to_queue = list()
        for job in jobs:
            if job.should_start:
                job.status = JobStatus.QUEUED
            else:
                job.status = JobStatus.WAITING
            jobs_to_queue.append(job.serialize())

        if jobs_to_queue:
            self._run_script(
                self._enqueue_job,
                self._to_namespaced(NOTIFICATIONS_KEY),
                self._to_namespaced(RUNNING_JOBS_KEY.format(self._id)),
                self.namespace,
                self._to_namespaced(FUTURE_JOBS_KEY),
                self._to_namespaced(MAX_CONCURRENCY_KEY),
                self._to_namespaced(CURRENT_CONCURRENCY_KEY),
                1 if from_failure else 0,
                *jobs_to_queue
            )

    def move_future_jobs(self) -> int:
        num_jobs_moved, dead_brokers_id = self._run_script(
            self._move_future_jobs,
            self.namespace,
            self._to_namespaced(FUTURE_JOBS_KEY),
            self._to_namespaced(NOTIFICATIONS_KEY),
            math.ceil(datetime.now(timezone.utc).timestamp()),
            JobStatus.QUEUED.value,
            self._to_namespaced(PERIODIC_TASKS_HASH_KEY),
            self._to_namespaced(PERIODIC_TASKS_QUEUE_KEY),
            self._to_namespaced(ALL_BROKERS_HASH_KEY),
            self._to_namespaced(ALL_BROKERS_ZSET_KEY),
            json.dumps(self._get_broker_info()),
            self.broker_dead_threshold_seconds,
            *[str(uuid.uuid4()) for _ in range(self._number_periodic_tasks)]
        )

        # The script may return a list of IDs of dead brokers,
        # convert it into the actual info of the dead brokers.
        dead_brokers = list()
        if dead_brokers_id:
            dead_brokers = [
                b for b in self.get_all_brokers()
                if b['id'].encode() in dead_brokers_id
            ]

        # Mark the dead brokers as dead and re-enqueue the jobs that were
        # running on them.
        for dead_broker in dead_brokers:
            logger.debug(
                'Worker %s on %s detected dead, re-enqueuing its jobs',
                dead_broker['id'], dead_broker['name']
            )
            num_jobs_re_enqueued, failed_jobs = (
                self.enqueue_jobs_from_dead_broker(
                    uuid.UUID(dead_broker['id'])
                )
            )
            logger.warning(
                'Worker %s on %s marked as dead, %d jobs were re-enqueued',
                dead_broker['id'], dead_broker['name'], num_jobs_re_enqueued
            )

            # Mark failed jobs appropriately.
            jobs = [Job.deserialize(job) for job in failed_jobs]
            err = Exception(
                "Worker %s died and max_retries exceeded" % dead_broker['name']
            )
            for job in jobs:
                advance_job_status(self.namespace, job, duration=0.0, err=err)

        logger.debug("Redis moved %s job(s) from future to current queues",
                     num_jobs_moved)
        return num_jobs_moved

    def _get_next_future_job(self) -> Optional[Job]:
        job = self._r.zrangebyscore(
            self._to_namespaced(FUTURE_JOBS_KEY), '-inf', '+inf',
            start=0, num=1
        )
        if not job:
            return None
        return Job.deserialize(job[0].decode())

    def get_jobs_from_queue(self, queue: str, max_jobs: int) -> List[Job]:
        """Get jobs from a queue."""
        jobs_json_string = self._run_script(
            self._get_jobs_from_queue,
            self._to_namespaced(queue),
            self._to_namespaced(RUNNING_JOBS_KEY.format(self._id)),
            JobStatus.RUNNING.value,
            max_jobs,
            self._to_namespaced(MAX_CONCURRENCY_KEY),
            self._to_namespaced(CURRENT_CONCURRENCY_KEY),
        )

        jobs = json.loads(jobs_json_string.decode())
        jobs = [Job.deserialize(job) for job in jobs]

        return jobs

    def remove_job_from_running(self, job: Job):
        if job.max_retries > 0:
            self._run_script(
                self._remove_job_from_running,
                self._to_namespaced(RUNNING_JOBS_KEY.format(self._id)),
                self._to_namespaced(MAX_CONCURRENCY_KEY),
                self._to_namespaced(CURRENT_CONCURRENCY_KEY),
                job.serialize(),
            )

        self._something_happened.set()

    def _subscriber_func(self):
        logger.debug('Redis broker subscriber started')
        pub_sub = self._r.pubsub(ignore_subscribe_messages=True)
        channel_name = self._to_namespaced(NOTIFICATIONS_KEY)
        pub_sub.subscribe(channel_name)

        # The subscriber basically waits for two things that can happen
        # independently:
        #   - must stop being set, meaning the thread should terminate
        #   - a message from the pub sub is received
        #
        # Because one of them is a threading.Event and the other one is
        # receiving something from a socket, it is not obvious how to
        # wait for those two things at the same time. Hence the approach
        # taken here which waits only for the pub sub but polls the must
        # stop event every second. Not very elegant.
        while not self._must_stop.is_set():
            if not pub_sub.get_message(timeout=self.must_stop_periodicity):
                continue

            # Consume all messages
            while pub_sub.get_message(timeout=0):
                pass  # pragma: no cover

            logger.debug('Got a message from channel %s', channel_name)
            self._something_happened.set()

        logger.debug('Deregistering broker')
        self._run_script(
            self._deregister,
            str(self._id),
            self._to_namespaced(ALL_BROKERS_HASH_KEY),
            self._to_namespaced(ALL_BROKERS_ZSET_KEY)
        )

        logger.debug('Redis broker subscriber terminated')

    def start(self):
        self._subscriber_thread = threading.Thread(
            target=run_forever,
            args=(self._subscriber_func, self._must_stop, logger),
            name='{}-broker-subscriber'.format(self.namespace)
        )
        self._subscriber_thread.start()

    def stop(self):
        super().stop()
        self._must_stop.set()
        self._subscriber_thread.join()
        self._reset()

    def flush(self):
        self._run_script(self._flush, self.namespace)

    def get_all_brokers(self) -> List[Dict[str, Union[None, str, int]]]:
        rv = self._r.hvals(
            self._to_namespaced(ALL_BROKERS_HASH_KEY),
        )
        return [json.loads(r.decode()) for r in rv]

    def enqueue_jobs_from_dead_broker(
        self, dead_broker_id: uuid.UUID
    ) -> Tuple[int, list]:
        return tuple(self._run_script(
            self._enqueue_jobs_from_dead_broker,
            str(dead_broker_id),
            self._to_namespaced(RUNNING_JOBS_KEY.format(dead_broker_id)),
            self._to_namespaced(ALL_BROKERS_HASH_KEY),
            self._to_namespaced(ALL_BROKERS_ZSET_KEY),
            self.namespace,
            self._to_namespaced(NOTIFICATIONS_KEY),
            self._to_namespaced(MAX_CONCURRENCY_KEY),
            self._to_namespaced(CURRENT_CONCURRENCY_KEY),
        ))

    def register_periodic_tasks(self, tasks: Iterable[Task]):
        """Register tasks that need to be scheduled periodically."""
        _tasks = [task.serialize() for task in tasks]
        self._number_periodic_tasks = len(_tasks)
        self._run_script(
            self._register_periodic_tasks,
            math.ceil(datetime.now(timezone.utc).timestamp()),
            self._to_namespaced(PERIODIC_TASKS_HASH_KEY),
            self._to_namespaced(PERIODIC_TASKS_QUEUE_KEY),
            *_tasks
        )

    def set_concurrency_keys(self, tasks: Iterable[Task]):
        """For each Task, set up its concurrency keys.

        The Lua script handles the logic of:
            - removing dead keys where a Task was removed
            - only setting keys where max_concurrency > 0
        """
        _tasks = [task.serialize() for task in tasks]
        if not _tasks:
            return
        self._run_script(
            self._set_concurrency_keys,
            self._to_namespaced(MAX_CONCURRENCY_KEY),
            self._to_namespaced(CURRENT_CONCURRENCY_KEY),
            *_tasks,
        )

    def inspect_periodic_tasks(self) -> List[Tuple[int, str]]:
        """Get the next periodic task schedule.

        Used only for debugging and during tests.
        """
        rv = self._r.zrangebyscore(
            self._to_namespaced(PERIODIC_TASKS_QUEUE_KEY),
            '-inf', '+inf', withscores=True
        )
        return [(int(r[1]), r[0].decode()) for r in rv]

    @property
    def next_future_periodic_delta(self) -> Optional[float]:
        """Give the amount of seconds before the next periodic task is due."""
        rv = self._r.zrangebyscore(
            self._to_namespaced(PERIODIC_TASKS_QUEUE_KEY),
            '-inf', '+inf', start=0, num=1, withscores=True,
            score_cast_func=int
        )
        if not rv:
            return None

        now = datetime.now(timezone.utc).timestamp()
        next_event_time = rv[0][1]
        if next_event_time < now:
            return 0

        return next_event_time - now


def generate_idempotency_token():
    return str(uuid.uuid4())


recommended_socket_opts = {
    'socket_timeout': 60,
    'socket_connect_timeout': 15
}
try:
    # These are the values used by Redis itself by default
    recommended_socket_opts['socket_keepalive_options'] = {
        socket.TCP_KEEPIDLE: 300,   # Send probes after 300s of inactivity
        socket.TCP_KEEPINTVL: 100,  # Send probes every 100s
        socket.TCP_KEEPCNT: 3       # Send 3 probes before closing
    }
    recommended_socket_opts['socket_keepalive'] = True
except AttributeError:  # pragma: no cover
    # Some non-Linux OS do not have the proper attribute in the socket module
    # for TCP Keepalive
    pass
