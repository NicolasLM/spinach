import pytest

from spinach.brokers.memory import MemoryBroker


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
