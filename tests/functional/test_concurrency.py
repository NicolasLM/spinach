import pytest
import time

from spinach.brokers.memory import MemoryBroker
from spinach.brokers.redis import RedisBroker
from spinach.engine import Engine


@pytest.fixture(params=[MemoryBroker, RedisBroker])
def spin(request):
    broker = request.param
    spin = Engine(broker(), namespace='tests')
    yield spin


def test_concurrency_limit(spin):
    count = 0

    @spin.task(name='do_something', max_retries=10, max_concurrency=1)
    def do_something(index):
        nonlocal count
        assert index == count
        count += 1

    for i in range(0, 5):
        spin.schedule(do_something, i)

    # Start two workers; test that only one job runs at once as per the
    # Task definition.
    spin.start_workers(number=2, block=True, stop_when_queue_empty=True)
    assert count == 5
