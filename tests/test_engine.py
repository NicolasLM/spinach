from datetime import datetime, timezone
from unittest.mock import patch
import time

import pytest

from spinach import Engine, MemoryBroker, RetryException
from spinach.job import Job, JobStatus
from spinach.exc import UnknownTask


@pytest.fixture
def spin():
    s = Engine(MemoryBroker(), namespace='tests')
    s.start_workers(number=1, block=False)
    yield s
    s.stop_workers()


def test_job_finished_callback(spin):
    now = datetime.now(timezone.utc)
    job = Job('foo_task', 'foo_queue', now, 0,
              task_args=(1, 2), task_kwargs={'foo': 'bar'})

    job.status = JobStatus.RUNNING
    spin._job_finished_callback(job, 1.0, None)
    assert job.status is JobStatus.SUCCEEDED

    job.status = JobStatus.RUNNING
    spin._job_finished_callback(job, 1.0, RuntimeError('Error'))
    assert job.status is JobStatus.FAILED

    job.status = JobStatus.RUNNING
    job.max_retries = 10
    spin._job_finished_callback(job, 1.0, RuntimeError('Error'))
    assert job.status is JobStatus.WAITING
    assert job.at > now

    job.status = JobStatus.RUNNING
    job.max_retries = 10
    spin._job_finished_callback(job, 1.0, RetryException('Must retry', at=now))
    assert job.status is JobStatus.QUEUED
    assert job.at == now

    job.status = JobStatus.RUNNING
    job.max_retries = 0
    spin._job_finished_callback(job, 1.0, RetryException('Must retry', at=now))
    assert job.status is JobStatus.FAILED


def test_schedule_unknown_task(spin):
    with pytest.raises(UnknownTask):
        spin.schedule('foo_task')
