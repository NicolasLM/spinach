import pytest

from spinach.brokers.redis.redis import RedisBroker


@pytest.fixture
def broker():
    broker = RedisBroker()
    broker.namespace = 'tests'
    broker.flush()
    broker.start()
    yield broker
    broker.stop()
    broker.flush()


def test_redis_flush(broker):
    broker._r.set('tests/foo', b'1')
    broker._r.set('tests2/foo', b'2')
    broker.flush()
    assert broker._r.get('tests/foo') is None
    assert broker._r.get('tests2/foo') == b'2'
    broker._r.delete('tests2/foo')
