from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from spinach import const
from spinach.brokers.memory import MemoryBroker
from spinach.brokers.redis import RedisBroker
from spinach.job import Job, JobStatus
from .conftest import get_now, set_now


@pytest.fixture(params=[MemoryBroker, RedisBroker])
def broker(request):
    broker = request.param()
    broker.namespace = 'tests'
    broker.flush()
    broker.start()
    yield broker
    broker.stop()
    broker.flush()


def test_normal_job(broker):
    job = Job('foo_task', 'foo_queue', datetime.now(timezone.utc), 0,
              task_args=(1, 2), task_kwargs={'foo': 'bar'})
    broker.enqueue_job(job)
    assert job.status == JobStatus.QUEUED

    job.status = JobStatus.RUNNING
    assert broker.get_job_from_queue('foo_queue') == job
    assert broker.get_job_from_queue('foo_queue') is None


def test_future_job(broker, patch_now):
    assert broker.next_future_job_delta is None
    assert broker.move_future_jobs() == 0

    job = Job('foo_task', 'foo_queue', get_now() + timedelta(minutes=10), 0,
              task_args=(1, 2), task_kwargs={'foo': 'bar'})

    broker.enqueue_job(job)
    assert job.status == JobStatus.WAITING
    assert broker.get_job_from_queue('foo_queue') is None
    assert broker.next_future_job_delta == 600
    assert broker.move_future_jobs() == 0

    set_now(datetime(2017, 9, 2, 9, 00, 56, 482169))
    assert broker.next_future_job_delta == 0
    assert broker.move_future_jobs() == 1

    job.status = JobStatus.RUNNING
    assert broker.get_job_from_queue('foo_queue') == job
    assert broker.next_future_job_delta is None


def test_wait_for_events_no_future_job(broker):
    with patch.object(broker, '_something_happened') as mock_sh:
        mock_sh.wait.return_value = False
        broker.wait_for_event()
        mock_sh.wait.assert_called_once_with(
            timeout=const.WAIT_FOR_EVENT_MAX_SECONDS
        )
        mock_sh.clear.assert_not_called()

        mock_sh.wait.return_value = True
        broker.wait_for_event()
        mock_sh.clear.called_once()


@pytest.mark.parametrize('delta,timeout', [
    (timedelta(weeks=10), const.WAIT_FOR_EVENT_MAX_SECONDS),
    (timedelta(seconds=5), 5)
])
def test_wait_for_events_with_future_job(broker, patch_now, delta, timeout):
    broker.enqueue_job(
        Job('foo_task', 'foo_queue', get_now() + delta, 0)
    )
    with patch.object(broker, '_something_happened') as mock_sh:
        broker.wait_for_event()
        mock_sh.wait.assert_called_once_with(timeout=timeout)


def test_flush(broker):
    broker.enqueue_job(Job('t1', 'q1', get_now(), 0))
    broker.enqueue_job(Job('t2', 'q2', get_now() + timedelta(seconds=10), 0))
    broker.flush()
    assert broker.get_job_from_queue('q1') is None
    assert broker.next_future_job_delta is None


def test_job_ran(broker):
    now = datetime.now(timezone.utc)
    job = Job('foo_task', 'foo_queue', now, 0,
              task_args=(1, 2), task_kwargs={'foo': 'bar'})

    job.status = JobStatus.RUNNING
    broker.job_ran(job, None)
    assert job.status is JobStatus.SUCCEEDED

    job.status = JobStatus.RUNNING
    broker.job_ran(job, RuntimeError('Error'))
    assert job.status is JobStatus.FAILED

    job.status = JobStatus.RUNNING
    job.max_retries = 10
    broker.job_ran(job, RuntimeError('Error'))
    assert job.status is JobStatus.WAITING
    assert job.at > now
