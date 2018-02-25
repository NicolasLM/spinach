from datetime import datetime, timezone
from unittest.mock import Mock, ANY

import pytest

from spinach import Engine, MemoryBroker, RetryException, Batch, Tasks
from spinach.job import Job, JobStatus
from spinach.exc import UnknownTask

from .conftest import get_now


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


def test_schedule_batch(patch_now):
    now = get_now()

    tasks = Tasks()
    tasks.add(print, 'foo_task')
    tasks.add(print, 'bar_task')

    broker = Mock()

    s = Engine(broker, namespace='tests')
    s.attach_tasks(tasks)

    batch = Batch()
    batch.schedule('foo_task', 1, 2)
    batch.schedule_at('bar_task', now, three=True)
    s.schedule_batch(batch)

    broker.enqueue_jobs.assert_called_once_with([ANY, ANY])

    foo_job = broker.enqueue_jobs.call_args[0][0][0]
    assert foo_job.task_name == 'foo_task'
    assert foo_job.at == now
    assert foo_job.task_args == (1, 2)
    assert foo_job.task_kwargs == {}

    bar_job = broker.enqueue_jobs.call_args[0][0][1]
    assert bar_job.task_name == 'bar_task'
    assert bar_job.at == now
    assert bar_job.task_args == ()
    assert bar_job.task_kwargs == {'three': True}
