import copy
from datetime import timedelta

import pytest

from spinach.job import Job

from .conftest import get_utcnow, set_utcnow


@pytest.fixture
def job(patch_utcnow):
    job = Job('foo_task', 'foo_queue', get_utcnow(),
              task_args=(1, 2), task_kwargs={'foo': 'bar'})
    return job


def test_serialization(job):
    job_json = job.serialize()
    assert Job.deserialize(job_json) == job


def test_at_timestamp(job):
    assert job.at_timestamp == 1504335057


def test_should_start(job):
    # Exact moment job should start
    assert job.should_start

    # A bit later
    set_utcnow(get_utcnow() + timedelta(minutes=1))
    assert job.should_start

    # A bit earlier
    set_utcnow(get_utcnow() - timedelta(minutes=2))
    assert not job.should_start


def test_repr(job):
    assert str(job.id) in repr(job)
    assert job.task_name in repr(job)


def test_task_func(job):
    assert job.task_func is None
    job.task_func = print
    assert job.task_func is print


def test_eq(job):
    assert job == job
    assert job != print

    job_2 = copy.deepcopy(job)
    job_2.task_name = 'bar_task'
    assert job != job_2
