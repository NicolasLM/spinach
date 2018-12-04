from unittest.mock import Mock
import time

import pytest

from spinach import Engine, MemoryBroker, Tasks
from spinach.contrib.datadog import register_datadog


@pytest.fixture
def spin():
    tasks = Tasks()

    @tasks.task(name='success')
    def success():
        return

    @tasks.task(name='fail')
    def fail():
        raise RuntimeError('failing task')

    s = Engine(MemoryBroker(), namespace='tests-datadog')
    s.attach_tasks(tasks)
    s.start_workers(number=1, block=False)
    yield s
    s.stop_workers()


def test_datadog(spin):
    mock_tracer = Mock()
    mock_span = Mock()
    mock_tracer.current_root_span.return_value = mock_span

    register_datadog(tracer=mock_tracer, namespace='tests-datadog')

    spin.schedule('success')
    time.sleep(0.1)
    mock_tracer.trace.assert_called_once_with(
        'spinach.task', service='spinach', span_type='worker',
        resource='success'
    )
    mock_span.finish.assert_called_once_with()
    mock_span.set_traceback.assert_not_called()

    spin.schedule('fail')
    time.sleep(0.1)
    mock_span.set_traceback.assert_called_once_with()
