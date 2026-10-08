import os

import pytest

from spinach.brokers.memory import MemoryBroker
from spinach.brokers.redis import RedisBroker
from spinach.engine import Engine


def _postgres_broker():
    from spinach.brokers.postgres import PostgresBroker
    return PostgresBroker(
        os.environ['SPINACH_TEST_POSTGRES_DSN'],
        require_ssl=False,
    )


def _broker_params():
    params = [
        pytest.param(MemoryBroker, id='memory'),
        pytest.param(RedisBroker, id='redis'),
    ]
    if os.environ.get('SPINACH_TEST_POSTGRES_DSN'):
        params.append(pytest.param(_postgres_broker, id='postgres'))
    return params


@pytest.fixture(params=_broker_params())
def spin(request):
    broker = request.param()
    engine = Engine(broker, namespace='tests')
    broker.flush()
    try:
        yield engine
    finally:
        if engine._workers is not None:
            engine.stop_workers()
        broker.flush()
        close = getattr(broker, 'close', None)
        if close is not None:
            close()


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
