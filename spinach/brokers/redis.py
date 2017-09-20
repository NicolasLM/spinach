from datetime import datetime, timezone
from logging import getLogger
import math
from os import path
import threading
from typing import Optional

from redis import StrictRedis
from redis.client import Script

from ..brokers.base import Broker
from ..job import Job, JobStatus
from ..const import FUTURE_JOBS_KEY, NOTIFICATIONS_KEY

logger = getLogger('spinach.broker')
here = path.abspath(path.dirname(__file__))


class RedisBroker(Broker):

    def __init__(self, redis: Optional[StrictRedis]=None):
        super().__init__()
        self._r = redis if redis else StrictRedis()

        # Register the lua scripts
        self._move_future_jobs = self._load_script('move_future_jobs.lua')
        self._enqueue_job = self._load_script('enqueue_job.lua')
        self._enqueue_future_job = self._load_script('enqueue_future_job.lua')
        self._flush = self._load_script('flush.lua')

        self._subscriber_thread = None
        self._must_stop = threading.Event()

    def _load_script(self, filename: str) -> Script:
        with open(path.join(here, 'redis_scripts', filename), mode='rb') as f:
            script_data = f.read()
        return self._r.register_script(script_data)

    def enqueue_job(self, job: Job):
        """Add a job to a queue"""
        if job.should_start:
            job.status = JobStatus.QUEUED
            self._enqueue_job(args=[
                self._to_namespaced(job.queue),
                self._to_namespaced(NOTIFICATIONS_KEY),
                job.serialize()
            ])
        else:
            job.status = JobStatus.WAITING
            self._enqueue_future_job(args=[
                self._to_namespaced(FUTURE_JOBS_KEY),
                self._to_namespaced(NOTIFICATIONS_KEY),
                job.at_timestamp,
                job.serialize()
            ])

    def move_future_jobs(self) -> int:
        num_jobs_moved = self._move_future_jobs(args=[
            self.namespace,
            self._to_namespaced(FUTURE_JOBS_KEY),
            self._to_namespaced(NOTIFICATIONS_KEY),
            math.ceil(datetime.now(timezone.utc).timestamp()),
            JobStatus.QUEUED.value
        ])
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

    def get_job_from_queue(self, queue: str) -> Optional[Job]:
        job_json_string = self._r.lpop(self._to_namespaced(queue))
        if not job_json_string:
            return None

        job = Job.deserialize(job_json_string.decode())
        job.status = JobStatus.RUNNING
        return job

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
                pass

            logger.debug('Got a message from channel %s', channel_name)
            self._something_happened.set()
        logger.debug('Redis broker subscriber terminated')

    def start(self):
        self._subscriber_thread = threading.Thread(
            target=self._subscriber_func,
            name='{}-broker-subscriber'.format(self.namespace)
        )
        self._subscriber_thread.start()

    def stop(self):
        super().stop()
        self._must_stop.set()

    def flush(self):
        self._flush(args=[self.namespace])
