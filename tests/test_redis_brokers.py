from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from spinach.brokers.redis import RedisBroker, RUNNING_JOBS_KEY
from spinach.job import Job, JobStatus


@pytest.fixture
def broker():
    broker = RedisBroker()
    broker.namespace = 'tests'
    broker.flush()
    broker.start()
    yield broker
    broker.stop()
    broker.flush()


def test_redis_flush(broker):
    broker._r.set('tests/foo', b'1')
    broker._r.set('tests2/foo', b'2')
    broker.flush()
    assert broker._r.get('tests/foo') is None
    assert broker._r.get('tests2/foo') == b'2'
    broker._r.delete('tests2/foo')


@patch('spinach.brokers.base.exponential_backoff')
def test_running_job(mock_eb, broker):
    mock_eb.return_value = timedelta()
    running_jobs_key = broker._to_namespaced(
        RUNNING_JOBS_KEY.format(broker._id)
    )

    # Non-idempotent job
    job = Job('foo_task', 'foo_queue', datetime.now(timezone.utc), 0)
    broker.enqueue_job(job)
    assert broker._r.hget(running_jobs_key, str(job.id)) is None
    broker.get_job_from_queue('foo_queue')
    assert broker._r.hget(running_jobs_key, str(job.id)) is None

    # Idempotent job - get from queue
    job = Job('foo_task', 'foo_queue', datetime.now(timezone.utc), 10)
    broker.enqueue_job(job)
    assert broker._r.hget(running_jobs_key, str(job.id)) is None
    broker.get_job_from_queue('foo_queue')
    job.status = JobStatus.RUNNING
    assert (
        Job.deserialize(broker._r.hget(running_jobs_key, str(job.id)).decode())
        == job
    )

    # Idempotent job - re-enqueue after job ran with error
    broker.job_ran(job, err=ZeroDivisionError())
    assert broker._r.hget(running_jobs_key, str(job.id)) is None
    broker.get_job_from_queue('foo_queue')
    job.status = JobStatus.RUNNING
    assert (
        Job.deserialize(broker._r.hget(running_jobs_key, str(job.id)).decode())
        == job
    )

    # Idempotent job - job succeeded
    broker.job_ran(job, err=None)
    assert broker._r.hget(running_jobs_key, str(job.id)) is None
    assert broker.get_job_from_queue('foo_queue') is None

    # Idempotent job - job failed
    job.status = JobStatus.NOT_SET
    job.retries = 999
    broker.enqueue_job(job)
    job = broker.get_job_from_queue('foo_queue')
    assert (
        Job.deserialize(broker._r.hget(running_jobs_key, str(job.id)).decode())
        == job
    )
    broker.job_ran(job, err=ZeroDivisionError())
    assert broker._r.hget(running_jobs_key, str(job.id)) is None
    assert job.status == JobStatus.FAILED
