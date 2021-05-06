from datetime import datetime, timezone
import pytest

from spinach.brokers.memory import MemoryBroker
from spinach.job import Job, JobStatus
from spinach.task import Task


@pytest.fixture
def broker():
    broker = MemoryBroker()
    broker.namespace = 'tests'
    broker.start()
    yield broker
    broker.stop()


def test_namespace():
    broker = MemoryBroker()

    with pytest.raises(RuntimeError):
        assert broker.namespace

    broker.namespace = 'tests'
    assert broker.namespace == 'tests'
    assert broker._to_namespaced('bar') == 'tests/bar'

    with pytest.raises(RuntimeError):
        broker.namespace = 'foo'


def test_get_queues(broker):
    assert broker._queues == {}

    queue = broker._get_queue('bar')
    assert broker._queues == {
        'tests/bar': queue
    }

    assert broker._get_queue('bar') is queue


def test_get_jobs_from_queue_limits_concurrency(broker):
    task = Task(print, 'foo', 'q1', 10, None, max_concurrency=1)
    broker.set_concurrency_keys([task])
    job1 = Job('foo', 'q1', datetime.now(timezone.utc), 10)
    job2 = Job('foo', 'q1', datetime.now(timezone.utc), 10)
    broker.enqueue_jobs([job1, job2])

    # Try to get one more than it allows.
    jobs = broker.get_jobs_from_queue('q1', 2)
    assert len(jobs) == 1


def test_get_jobs_from_queue_re_adds_jobs_if_over_limit(broker):
    task = Task(print, 'foo', 'q1', 10, None, max_concurrency=1)
    broker.set_concurrency_keys([task])
    job1 = Job('foo', 'q1', datetime.now(timezone.utc), 10)
    job2 = Job('foo', 'q1', datetime.now(timezone.utc), 10)
    broker.enqueue_jobs([job1, job2])

    # Try to get one more than it allows.
    [running_job] = broker.get_jobs_from_queue('q1', 2)

    # Pop what's left in the broker's Queue and inspect it.
    job_json_string = broker._get_queue('q1').get(block=False)
    queued_job = Job.deserialize(job_json_string)
    assert queued_job != running_job


def test_decrements_concurrency_count_when_job_ends(broker):
    task = Task(print, 'foo', 'q1', 10, None, max_concurrency=1)
    broker.set_concurrency_keys([task])
    job1 = Job('foo', 'q1', datetime.now(timezone.utc), 10)
    job2 = Job('foo', 'q1', datetime.now(timezone.utc), 10)
    broker.enqueue_jobs([job1, job2])

    # Start the first job.
    running_jobs = broker.get_jobs_from_queue('q1', 2)
    assert 1 == len(running_jobs)

    # No more can start.
    assert 0 == len(broker.get_jobs_from_queue('q1', 2))

    # Complete the first job.
    broker.remove_job_from_running(running_jobs[0])

    # Start second job now first has finished.
    assert 1 == len(broker.get_jobs_from_queue('q1', 2))
