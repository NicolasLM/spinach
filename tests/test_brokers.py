from datetime import datetime, timedelta, timezone
from unittest.mock import patch
import uuid

import pytest

from spinach import const
from spinach.brokers.memory import MemoryBroker
from spinach.brokers.redis import RedisBroker
from spinach.job import Job, JobStatus
from spinach.task import Task
from .conftest import get_now, set_now


@pytest.fixture(params=[MemoryBroker, RedisBroker])
def broker(request):
    broker = request.param()
    broker.namespace = 'tests'
    broker.must_stop_periodicity = 0.01
    broker.flush()
    broker.start()
    yield broker
    broker.stop()
    broker.flush()


def test_normal_job(broker):
    job = Job('foo_task', 'foo_queue', datetime.now(timezone.utc), 0,
              task_args=(1, 2), task_kwargs={'foo': 'bar'})
    broker.enqueue_jobs([job])
    assert job.status == JobStatus.QUEUED

    job.status = JobStatus.RUNNING
    assert broker.get_jobs_from_queue('foo_queue', 5) == [job]
    assert broker.get_jobs_from_queue('foo_queue', 1) == []


def test_future_job(broker, patch_now):
    assert broker.next_future_job_delta is None
    assert broker.move_future_jobs() == 0

    job = Job('foo_task', 'foo_queue', get_now() + timedelta(minutes=10), 0,
              task_args=(1, 2), task_kwargs={'foo': 'bar'})

    broker.enqueue_jobs([job])
    assert job.status == JobStatus.WAITING
    assert broker.get_jobs_from_queue('foo_queue', 5) == []
    assert broker.next_future_job_delta == 600
    assert broker.move_future_jobs() == 0

    set_now(datetime(2017, 9, 2, 9, 00, 56, 482169))
    assert broker.next_future_job_delta == 0
    assert broker.move_future_jobs() == 1

    job.status = JobStatus.RUNNING
    assert broker.get_jobs_from_queue('foo_queue', 5) == [job]


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
    broker.enqueue_jobs(
        [Job('foo_task', 'foo_queue', get_now() + delta, 0)]
    )
    with patch.object(broker, '_something_happened') as mock_sh:
        broker.wait_for_event()
        mock_sh.wait.assert_called_once_with(timeout=timeout)


def test_flush(broker):
    broker.enqueue_jobs([
        Job('t1', 'q1', get_now(), 0),
        Job('t2', 'q2', get_now() + timedelta(seconds=10), 0)
    ])
    broker.flush()
    assert broker.get_jobs_from_queue('q1', 1) == []
    assert broker.next_future_job_delta is None


def test_enqueue_jobs_from_dead_broker(broker):
    # Marking a broker that doesn't exist as dead
    broker_id = uuid.UUID('62664577-cf89-4f6a-ab16-4e20ec8fe4c2')
    assert broker.enqueue_jobs_from_dead_broker(broker_id) == 0


def test_repr(broker):
    assert broker.__class__.__name__ in repr(broker)
    assert str(broker._id) in repr(broker)


def test_no_periodic_tasks(broker):
    broker.register_periodic_tasks([])
    assert broker.inspect_periodic_tasks() == []


def test_periodic_tasks(broker):
    tasks = [
        Task(print, 'foo', 'q1', 0, timedelta(seconds=5)),
        Task(print, 'bar', 'q1', 0, timedelta(seconds=10))
    ]
    broker.register_periodic_tasks(tasks)

    r = broker.inspect_periodic_tasks()
    assert r[0][1] == 'foo'
    assert r[1][1] == 'bar'
    assert r[0][0] == r[1][0] - 5
