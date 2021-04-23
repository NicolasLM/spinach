from datetime import datetime, timedelta, timezone
import json
import operator
import random
import time
from unittest.mock import patch, Mock

import pytest

from spinach.brokers.redis import (
    CURRENT_CONCURRENCY_KEY,
    MAX_CONCURRENCY_KEY,
    PERIODIC_TASKS_HASH_KEY,
    RedisBroker,
    RUNNING_JOBS_KEY,
)
from spinach.job import Job, JobStatus
from spinach.task import Task


CONCURRENT_TASK_NAME = 'i_am_concurrent'
CONCURRENT_TASK_MAX_CONCURRENCY = 1


def make_broker():
    broker = RedisBroker()
    broker.namespace = 'tests'
    broker.must_stop_periodicity = 0.01
    broker.flush()
    return broker


@pytest.fixture
def broker():
    broker = make_broker()
    broker.start()
    yield broker
    broker.stop()
    broker.flush()


def set_concurrency_keys(broker, current_concurrency=None):
    if current_concurrency is None:
        current_concurrency = 0
    # Set up dummy concurrency keys for a dummy task.
    broker._r.hset(
        broker._to_namespaced(MAX_CONCURRENCY_KEY),
        CONCURRENT_TASK_NAME,
        CONCURRENT_TASK_MAX_CONCURRENCY,
    )
    broker._r.hset(
        broker._to_namespaced(CURRENT_CONCURRENCY_KEY),
        CONCURRENT_TASK_NAME,
        current_concurrency,
    )


# Fixture to get a second broker
broker_2 = broker


def test_redis_flush(broker):
    broker._r.set('tests/foo', b'1')
    broker._r.set('tests2/foo', b'2')
    broker.flush()
    assert broker._r.get('tests/foo') is None
    assert broker._r.get('tests2/foo') == b'2'
    broker._r.delete('tests2/foo')


def test_running_job(broker):
    running_jobs_key = broker._to_namespaced(
        RUNNING_JOBS_KEY.format(broker._id)
    )

    # Non-idempotent job
    job = Job('foo_task', 'foo_queue', datetime.now(timezone.utc), 0)
    broker.enqueue_jobs([job])
    assert broker._r.hget(running_jobs_key, str(job.id)) is None
    broker.get_jobs_from_queue('foo_queue', 1)
    assert broker._r.hget(running_jobs_key, str(job.id)) is None
    # Try to remove it, even if it doesn't exist in running
    broker.remove_job_from_running(job)

    # Idempotent job - get from queue
    job = Job('foo_task', 'foo_queue', datetime.now(timezone.utc), 10)
    broker.enqueue_jobs([job])
    assert broker._r.hget(running_jobs_key, str(job.id)) is None
    broker.get_jobs_from_queue('foo_queue', 1)
    job.status = JobStatus.RUNNING
    assert (
        Job.deserialize(broker._r.hget(running_jobs_key, str(job.id)).decode())
        == job
    )

    # Idempotent job - re-enqueue after job ran with error
    job.retries += 1
    broker.enqueue_jobs([job])
    assert broker._r.hget(running_jobs_key, str(job.id)) is None
    broker.get_jobs_from_queue('foo_queue', 1)
    job.status = JobStatus.RUNNING
    assert (
        Job.deserialize(broker._r.hget(running_jobs_key, str(job.id)).decode())
        == job
    )

    # Idempotent job - job succeeded
    broker.remove_job_from_running(job)
    assert broker._r.hget(running_jobs_key, str(job.id)) is None
    assert broker.get_jobs_from_queue('foo_queue', 1) == []


def test_decrements_concurrency_count_when_job_fails(broker):
    set_concurrency_keys(broker)
    job = Job(
        CONCURRENT_TASK_NAME, 'foo_queue', datetime.now(timezone.utc),
        max_retries=10,
    )
    broker.enqueue_jobs([job])
    broker.get_jobs_from_queue('foo_queue', 1)
    current = broker._r.hget(
        broker._to_namespaced(CURRENT_CONCURRENCY_KEY), CONCURRENT_TASK_NAME
    )
    assert current == b'1'
    job.status = JobStatus.NOT_SET
    job.retries += 1
    broker.enqueue_jobs([job], from_failure=True)
    current = broker._r.hget(
        broker._to_namespaced(CURRENT_CONCURRENCY_KEY), CONCURRENT_TASK_NAME
    )
    assert current == b'0'


def test_decrements_concurrency_count_when_job_ends(broker):
    set_concurrency_keys(broker)
    job = Job(
        CONCURRENT_TASK_NAME, 'foo_queue', datetime.now(timezone.utc), 1,
        # kwargs help with debugging but are not part of the test.
        task_kwargs=dict(name='job1'),
    )
    broker.enqueue_jobs([job])
    returned_jobs = broker.get_jobs_from_queue('foo_queue', 2)
    assert len(returned_jobs) == CONCURRENT_TASK_MAX_CONCURRENCY
    current = broker._r.hget(
        broker._to_namespaced(CURRENT_CONCURRENCY_KEY), CONCURRENT_TASK_NAME
    )
    assert current == b'1'
    broker.get_jobs_from_queue('foo_queue', 1)
    job.status = JobStatus.RUNNING
    broker.remove_job_from_running(job)
    current = broker._r.hget(
        broker._to_namespaced(CURRENT_CONCURRENCY_KEY), CONCURRENT_TASK_NAME
    )
    assert current == b'0'


def test_cant_exceed_max_concurrency(broker):
    set_concurrency_keys(broker)
    # An idempotent job is required.
    job1 = Job(
        CONCURRENT_TASK_NAME, 'foo_queue', datetime.now(timezone.utc), 1,
        # kwargs help with debugging but are not part of the test.
        task_kwargs=dict(name='job1'),
    )
    job2 = Job(
        CONCURRENT_TASK_NAME, 'foo_queue', datetime.now(timezone.utc), 1,
        task_kwargs=dict(name='job2'),
    )
    broker.enqueue_jobs([job1, job2])
    returned_jobs = broker.get_jobs_from_queue('foo_queue', 2)
    assert len(returned_jobs) == CONCURRENT_TASK_MAX_CONCURRENCY
    assert returned_jobs[0].task_kwargs == dict(name='job1')

    # Check the current concurrency was set properly.
    current = broker._r.hget(
        broker._to_namespaced(CURRENT_CONCURRENCY_KEY), CONCURRENT_TASK_NAME
    )
    assert current == b'1'

    # Make sure job2 was left alone in the queue.
    queued = broker._r.lpop(broker._to_namespaced('foo_queue'))
    assert json.loads(queued.decode())['id'] == str(job2.id)


def test_get_jobs_from_queue_returns_all_requested(broker):
    # If a job is not returned because it was over concurrency limits,
    # make sure the number of jobs requested is filled from other jobs
    # and that the over-limit one is left alone in the queue.
    set_concurrency_keys(broker)
    jobs = [
        Job(CONCURRENT_TASK_NAME, 'foo_queue', datetime.now(timezone.utc), 1),
        Job(CONCURRENT_TASK_NAME, 'foo_queue', datetime.now(timezone.utc), 1),
        Job('foo_task', 'foo_queue', datetime.now(timezone.utc), 1),
    ]
    broker.enqueue_jobs(jobs)
    returned_jobs = broker.get_jobs_from_queue('foo_queue', 2)
    assert len(returned_jobs) == CONCURRENT_TASK_MAX_CONCURRENCY + 1


def test_set_concurrency_keys_sets_new_keys_on_idempotent_tasks(broker):
    max_c = random.randint(1, 10)
    tasks = [
        Task(
            print, 'foo', 'q1', 10, timedelta(seconds=5),
            max_concurrency=max_c),
    ]
    broker.set_concurrency_keys(tasks)
    assert int(broker._r.hget(
        broker._to_namespaced(MAX_CONCURRENCY_KEY),
        tasks[0].name
    )) == max_c
    assert int(broker._r.hget(
        broker._to_namespaced(CURRENT_CONCURRENCY_KEY),
        tasks[0].name
    )) == 0


def test_set_concurrency_keys_ignores_tasks_without_concurrency(broker):
    tasks = [
        Task(
            print, 'foo', 'q1', 10, timedelta(seconds=5),
            max_concurrency=None),
    ]
    broker.set_concurrency_keys(tasks)
    assert broker._r.hget(
        broker._to_namespaced(MAX_CONCURRENCY_KEY), tasks[0].name
    ) is None


def test_set_concurrency_keys_doesnt_overwrite_existing_concurrency(broker):
    current_c = random.randint(1, 10)
    set_concurrency_keys(broker, current_concurrency=current_c)
    tasks = [
        Task(
            print, CONCURRENT_TASK_NAME, 'q1', 10, timedelta(seconds=10),
            max_concurrency=1
        ),
    ]
    broker.set_concurrency_keys(tasks)
    assert int(broker._r.hget(
        broker._to_namespaced(CURRENT_CONCURRENCY_KEY),
        tasks[0].name
    )) == current_c


def test_set_concurrency_keys_removes_unused_keys_on_deleted_tasks(broker):
    set_concurrency_keys(broker)
    # Define different Tasks to CONCURRENT_TASK_NAME with a
    # max_concurrency setting.
    task = Task(
        print, 'foo', 'q1', 10, timedelta(seconds=10), max_concurrency=1
    )
    broker.set_concurrency_keys([task])
    # Check that the testing default concurrency keys were removed.
    assert broker._r.hexists(
        broker._to_namespaced(MAX_CONCURRENCY_KEY), CONCURRENT_TASK_NAME
    ) is False
    assert broker._r.hexists(
        broker._to_namespaced(CURRENT_CONCURRENCY_KEY), CONCURRENT_TASK_NAME
    ) is False


def test_enqueue_jobs_from_dead_broker(broker, broker_2):
    set_concurrency_keys(broker)
    # Enqueue one idempotent job, one non-idempotent job, and another
    # that has a max_concurrency.
    job_1 = Job('foo_task', 'foo_queue', datetime.now(timezone.utc), 0)
    job_2 = Job('foo_task', 'foo_queue', datetime.now(timezone.utc), 10)
    job_3 = Job(
        CONCURRENT_TASK_NAME, 'foo_queue', datetime.now(timezone.utc), 10
    )
    broker.enqueue_jobs([job_1, job_2, job_3])

    # Simulate broker starting the jobs
    broker.get_jobs_from_queue('foo_queue', 100)

    # Check the current_concurrency
    current = broker._r.hget(
        broker._to_namespaced(CURRENT_CONCURRENCY_KEY), CONCURRENT_TASK_NAME
    )
    assert current == b'1'

    # Mark broker as dead, should re-enqueue only the idempotent jobs.
    assert broker_2.enqueue_jobs_from_dead_broker(broker._id) == 2

    # Check that the current_concurrency was decremented for job_3.
    current = broker._r.hget(
        broker._to_namespaced(CURRENT_CONCURRENCY_KEY), CONCURRENT_TASK_NAME
    )
    assert current == b'0'

    # Simulate broker 2 getting jobs from the queue
    job_2.status = job_3.status = JobStatus.RUNNING
    job_2.retries = job_3.retries = 1
    result = sorted(
        broker_2.get_jobs_from_queue('foo_queue', 100),
        key=operator.attrgetter('id')
    )
    expected = sorted([job_2, job_3], key=operator.attrgetter('id'))
    assert result == expected

    # Check that a broker can be marked as dead multiple times
    # without duplicating jobs
    assert broker_2.enqueue_jobs_from_dead_broker(broker._id) == 0


def test_detect_dead_broker(broker, broker_2):
    broker_2.enqueue_jobs_from_dead_broker = Mock(return_value=10)

    # Register the first broker
    broker.move_future_jobs()

    # Set the 2nd broker to detect dead brokers after 2 seconds of inactivity
    broker_2.broker_dead_threshold_seconds = 2
    time.sleep(2.1)

    # Detect dead brokers
    broker_2.move_future_jobs()
    broker_2.enqueue_jobs_from_dead_broker.assert_called_once_with(
        broker._id
    )


def test_not_detect_deregistered_broker_as_dead(broker, broker_2):
    broker_2.enqueue_jobs_from_dead_broker = Mock(return_value=10)

    # Register and de-register the first broker
    broker.move_future_jobs()
    broker.stop()

    # Set the 2nd broker to detect dead brokers after 2 seconds of inactivity
    broker_2.broker_dead_threshold_seconds = 2
    time.sleep(2.1)

    # Detect dead brokers
    broker_2.move_future_jobs()
    broker_2.enqueue_jobs_from_dead_broker.assert_not_called()

    # Just so that the fixture can terminate properly
    broker.stop = Mock()


def test_old_periodic_tasks(broker):
    periodic_tasks_hash_key = broker._to_namespaced(PERIODIC_TASKS_HASH_KEY)
    tasks = [
        Task(print, 'foo', 'q1', 0, timedelta(seconds=5)),
        Task(print, 'bar', 'q1', 0, timedelta(seconds=10))
    ]

    broker.register_periodic_tasks(tasks)
    assert broker._number_periodic_tasks == 2
    assert broker._r.hgetall(periodic_tasks_hash_key) == {
        b'foo': b'{"max_concurrency": -1, "max_retries": 0, "name": "foo", '
                b'"periodicity": 5, "queue": "q1"}',
        b'bar': b'{"max_concurrency": -1, "max_retries": 0, "name": "bar", '
                b'"periodicity": 10, "queue": "q1"}'
    }

    broker.register_periodic_tasks([tasks[1]])
    assert broker._number_periodic_tasks == 1
    assert broker._r.hgetall(periodic_tasks_hash_key) == {
        b'bar': b'{"max_concurrency": -1, "max_retries": 0, "name": "bar", '
                b'"periodicity": 10, "queue": "q1"}'
    }


@patch('spinach.brokers.redis.generate_idempotency_token', return_value='42')
def test_idempotency_token(_, broker):
    job_1 = Job('foo_task', 'foo_queue', datetime.now(timezone.utc), 0)
    job_2 = Job('foo_task', 'foo_queue', datetime.now(timezone.utc), 0)
    broker.enqueue_jobs([job_1])
    broker.enqueue_jobs([job_2])

    jobs = broker.get_jobs_from_queue('foo_queue', max_jobs=10)
    job_1.status = JobStatus.RUNNING
    assert jobs == [job_1]
