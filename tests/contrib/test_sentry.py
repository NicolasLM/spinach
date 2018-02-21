from unittest.mock import Mock
import time

import pytest

from spinach import Spinach, MemoryBroker, Tasks
from spinach.contrib.sentry import register_sentry


@pytest.fixture
def spin():
    tasks = Tasks()

    @tasks.task(name='fail')
    def fail():
        raise RuntimeError('failing task')

    @tasks.task(name='success')
    def success():
        return

    s = Spinach(MemoryBroker(), namespace='tests')
    s.attach_tasks(tasks)
    s.start_workers(number=1, block=False)
    yield s
    s.stop_workers()


def test_sentry(spin):
    raven_client = Mock()
    register_sentry(raven_client, spin.namespace)

    spin.schedule('success')
    time.sleep(1)
    raven_client.context.activate.assert_called_once_with()
    raven_client.transaction.push.assert_called_once_with('success')
    raven_client.transaction.pop.assert_called_once_with('success')
    raven_client.context.clear.assert_called_once_with()
    raven_client.captureException.assert_not_called()

    raven_client.reset_mock()
    spin.schedule('fail')
    time.sleep(1)
    raven_client.context.activate.assert_called_once_with()
    raven_client.transaction.push.assert_called_once_with('fail')
    raven_client.transaction.pop.assert_called_once_with('fail')
    raven_client.context.clear.assert_called_once_with()
    assert raven_client.captureException.call_count == 1
