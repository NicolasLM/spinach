import copy
from datetime import timedelta

import pytest

from spinach.job import Job

from .conftest import get_now, set_now


@pytest.fixture
def job(patch_now):
    job = Job('foo_task', 'foo_queue', get_now(),
              task_args=(1, 2), task_kwargs={'foo': 'bar'})
    return job


def test_serialization(job):
    job_json = job.serialize()
    assert Job.deserialize(job_json) == job


def test_at_timestamp(job):
    assert job.at_timestamp == 1504342257


def test_should_start(job):
    # Exact moment job should start
    assert job.should_start

    # A bit later
    set_now(get_now() + timedelta(minutes=1))
    assert job.should_start

    # A bit earlier
    set_now(get_now() - timedelta(minutes=2))
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
