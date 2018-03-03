from datetime import datetime, timezone
import json
from logging import getLogger
import math
from os import path
import socket
import threading
from typing import Optional, Iterable, List, Tuple
import uuid

from redis import StrictRedis
from redis.client import Script

from ..brokers.base import Broker
from ..job import Job, JobStatus
from ..task import Task
from ..const import (
    FUTURE_JOBS_KEY, NOTIFICATIONS_KEY, RUNNING_JOBS_KEY,
    PERIODIC_TASKS_HASH_KEY, PERIODIC_TASKS_QUEUE_KEY
)
from ..utils import run_forever


logger = getLogger('spinach.broker')
here = path.abspath(path.dirname(__file__))


# Todo: make move_future_jobs into a more generic task not always executed at
# arbiter loop (like once per minute) that:
# - moves future jobs
# - registers the broker
# - moves running jobs from stale brokers

class RedisBroker(Broker):

    def __init__(self, redis: Optional[StrictRedis]=None):
        super().__init__()
        self._r = redis if redis else StrictRedis(**recommended_socket_opts)

        # Register the lua scripts
        self._move_future_jobs = self._load_script('move_future_jobs.lua')
        self._enqueue_job = self._load_script('enqueue_job.lua')
        self._enqueue_future_job = self._load_script('enqueue_future_job.lua')
        self._flush = self._load_script('flush.lua')
        self._get_jobs_from_queue = self._load_script(
            'get_jobs_from_queue.lua'
        )
        self._register_periodic_tasks = self._load_script(
            'register_periodic_tasks.lua'
        )

        self._subscriber_thread = None
        self._must_stop = threading.Event()

    def _load_script(self, filename: str) -> Script:
        with open(path.join(here, 'redis_scripts', filename), mode='rb') as f:
            script_data = f.read()
        return self._r.register_script(script_data)

    def _run_script(self, script: Script, *args):
        args = [str(self._id)] + list(args)
        return script(args=args)

    def enqueue_jobs(self, jobs: Iterable[Job]):
        """Enqueue a batch of jobs."""
        jobs_to_queue = list()
        future_jobs = list()
        for job in jobs:
            if job.should_start:
                job.status = JobStatus.QUEUED
                jobs_to_queue.append(job.serialize())
            else:
                job.status = JobStatus.WAITING
                future_jobs.append(job.serialize())

        if jobs_to_queue:
            self._run_script(
                self._enqueue_job,
                self._to_namespaced(NOTIFICATIONS_KEY),
                self._to_namespaced(RUNNING_JOBS_KEY.format(self._id)),
                self.namespace,
                *jobs_to_queue
            )

        if future_jobs:
            self._run_script(
                self._enqueue_future_job,
                self._to_namespaced(NOTIFICATIONS_KEY),
                self._to_namespaced(RUNNING_JOBS_KEY.format(self._id)),
                self._to_namespaced(FUTURE_JOBS_KEY),
                *future_jobs
            )

    def move_future_jobs(self) -> int:
        num_jobs_moved = self._run_script(
            self._move_future_jobs,
            self.namespace,
            self._to_namespaced(FUTURE_JOBS_KEY),
            self._to_namespaced(NOTIFICATIONS_KEY),
            math.ceil(datetime.now(timezone.utc).timestamp()),
            JobStatus.QUEUED.value,
            self._to_namespaced(PERIODIC_TASKS_HASH_KEY),
            self._to_namespaced(PERIODIC_TASKS_QUEUE_KEY),
            *[str(uuid.uuid4()) for _ in range(10)]
        )
        logger.debug("Redis moved %s job(s) from future to current queues",
                     num_jobs_moved)
        return num_jobs_moved

    def _get_next_future_job(self)-> Optional[Job]:
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
            max_jobs
        )

        jobs = json.loads(jobs_json_string.decode())
        jobs = [Job.deserialize(job) for job in jobs]

        return jobs

    def remove_job_from_running(self, job: Job):
        if job.max_retries > 0:
            self._r.hdel(
                self._to_namespaced(RUNNING_JOBS_KEY.format(self._id)),
                str(job.id)
            )
        self._something_happened.set()

    def _subscriber_func(self):
        logger.debug('Redis broker subscriber started')
        pub_sub = self._r.pubsub(ignore_subscribe_messages=True)
        channel_name = self._to_namespaced(NOTIFICATIONS_KEY)
        pub_sub.subscribe(channel_name)

        while not self._must_stop.is_set():
            if not pub_sub.get_message(timeout=1):
                continue

            # Consume all messages
            while pub_sub.get_message(timeout=0):
                pass  # pragma: no cover

            logger.debug('Got a message from channel %s', channel_name)
            self._something_happened.set()
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

    def flush(self):
        self._run_script(self._flush, self.namespace)

    def register_periodic_tasks(self, tasks: Iterable[Task]):
        """Register tasks that need to be scheduled periodically."""
        tasks = [task.serialize() for task in tasks]
        self._run_script(
            self._register_periodic_tasks,
            math.ceil(datetime.now(timezone.utc).timestamp()),
            self._to_namespaced(PERIODIC_TASKS_HASH_KEY),
            self._to_namespaced(PERIODIC_TASKS_QUEUE_KEY),
            *tasks
        )

    def inspect_periodic_tasks(self) -> List[Tuple[int, str]]:
        """Get the next periodic task schedule.

        Used only for debugging and during tests.
        """
        rv = self._r.zrangebyscore(
            self._to_namespaced(PERIODIC_TASKS_QUEUE_KEY),
            '-inf',  '+inf', withscores=True
        )
        return [(int(r[1]), r[0].decode()) for r in rv]

    @property
    def next_future_periodic_delta(self) -> Optional[float]:
        """Give the amount of seconds before the next periodic task is due."""
        rv = self._r.zrangebyscore(
            self._to_namespaced(PERIODIC_TASKS_QUEUE_KEY),
            '-inf',  '+inf', start=0, num=1, withscores=True,
            score_cast_func=int
        )
        if not rv:
            return None

        now = datetime.now(timezone.utc).timestamp()
        next_event_time = rv[0][1]
        if next_event_time < now:
            return 0

        return next_event_time - now


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
