from datetime import datetime, timezone
from unittest.mock import Mock, ANY
import time
import sys

import pytest

from spinach import signals
from spinach.worker import ThreadWorkers, AsyncioWorkers
from spinach.job import Job


# Spinach does not support AsyncIO on Python 3.6
workers_to_test = [ThreadWorkers]
if sys.version_info >= (3, 7):
    workers_to_test.append(AsyncioWorkers)


@pytest.fixture(params=workers_to_test)
def workers(request):
    workers = request.param(2, 'tests')
    yield workers
    workers.stop()


@pytest.fixture
def job():
    task_func = Mock()
    job = Job('foo_task', 'foo_queue', datetime.now(timezone.utc), 10,
              task_args=(1, 2), task_kwargs={'foo': 'bar'})
    job.task_func = task_func

    return job, task_func


def wait_for_queue_empty(workers: ThreadWorkers, timeout=10):
    for _ in range(timeout * 10):
        if workers._in_queue.empty():
            return

        time.sleep(0.1)

    raise RuntimeError('Queue did not get empty after {}s'.format(timeout))


def test_job_execution(workers, job):
    job, task_func = job
    assert workers.can_accept_job()

    workers.submit_job(job)
    wait_for_queue_empty(workers)

    # Executed function raised no error
    task_func.assert_called_once_with(*job.task_args, **job.task_kwargs)
    assert workers.out_queue.get() is job
    assert workers.can_accept_job()


def test_job_execution_exception(workers, job):
    job, task_func = job

    # Executed function raised an error
    error = RuntimeError('Error')
    task_func.side_effect = error

    workers.submit_job(job)
    wait_for_queue_empty(workers)

    task_func.assert_called_once_with(*job.task_args, **job.task_kwargs)
    assert workers.out_queue.get() is job


def test_submit_job_shutdown_workers(workers, job):
    job, task_func = job
    workers.stop()
    with pytest.raises(RuntimeError):
        workers.submit_job(job)


def test_start_0_workers():
    with pytest.raises(ValueError):
        ThreadWorkers(0, 'tests')


@pytest.mark.parametrize('number', [1, 5])
def test_start_stop_n_workers(number):
    workers = ThreadWorkers(number, 'tests')
    assert workers._in_queue.maxsize == number
    assert len(workers._threads) == number
    for thread in workers._threads:
        assert 'tests-worker-' in thread.name

    workers.stop()


def test_worker_signals(job):
    job, task_func = job

    mock_job_started_receiver = Mock(spec={})
    signals.job_started.connect(mock_job_started_receiver)

    mock_job_finished_receiver = Mock(spec={})
    signals.job_finished.connect(mock_job_finished_receiver)

    mock_worker_started_receiver = Mock(spec={})
    signals.worker_started.connect(mock_worker_started_receiver)

    mock_worker_terminated_receiver = Mock(spec={})
    signals.worker_terminated.connect(mock_worker_terminated_receiver)

    ns = 'tests'
    workers = ThreadWorkers(1, ns)
    workers.submit_job(job)
    wait_for_queue_empty(workers)
    workers.stop()

    mock_job_started_receiver.assert_called_once_with(ns, job=ANY)
    mock_job_finished_receiver.assert_called_once_with(ns, job=ANY)
    mock_worker_started_receiver.assert_called_once_with(
        ns, worker_name='tests-worker-1'
    )
    mock_worker_terminated_receiver.assert_called_once_with(
        ns, worker_name='tests-worker-1'
    )


def test_can_accept_job(workers, job):
    job, _ = job
    assert workers.available_slots == 2

    workers.submit_job(job)
    workers.submit_job(job)
    assert workers.available_slots == 0

    wait_for_queue_empty(workers)
    assert workers.available_slots == 2
