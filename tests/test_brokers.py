from datetime import datetime, timedelta

import pytest

from spinach.brokers.memory import MemoryBroker
from spinach.brokers.redis.redis import RedisBroker
from spinach.job import Job

from .conftest import get_now, set_now


@pytest.fixture(params=[MemoryBroker, RedisBroker])
def broker(request):
    broker = request.param()
    broker.namespace = 'tests'
    broker.start()
    yield broker
    broker.stop()


def test_normal_job(broker):
    job = Job('foo_task', 'foo_queue', datetime.utcnow(),
              task_args=(1, 2), task_kwargs={'foo': 'bar'})
    broker.enqueue_job(job)
    assert broker.get_job_from_queue('foo_queue') == job
    assert broker.get_job_from_queue('foo_queue') is None


def test_future_job(broker, patch_now):
    assert broker.next_future_job_delta is None
    assert broker.move_future_jobs() == 0

    job = Job('foo_task', 'foo_queue', get_now() + timedelta(minutes=10),
              task_args=(1, 2), task_kwargs={'foo': 'bar'})

    broker.enqueue_job(job)
    assert broker.get_job_from_queue('foo_queue') is None
    assert broker.next_future_job_delta == 600
    assert broker.move_future_jobs() == 0

    set_now(datetime(2017, 9, 2, 9, 00, 56, 482169))
    assert broker.next_future_job_delta == 0
    assert broker.move_future_jobs() == 1
    assert broker.get_job_from_queue('foo_queue') == job
    assert broker.next_future_job_delta is None
