import asyncio

import pytest

from spinach.queuey import Queuey


def test_sync():
    q = Queuey(2)
    q.put_sync(1)
    q.put_sync(2)
    assert len(q._items) == 2
    assert len(q._putters) == 0
    assert len(q._getters) == 0

    assert q.get_sync() == 1
    assert q.get_sync() == 2
    assert len(q._items) == 0
    assert len(q._putters) == 0
    assert len(q._getters) == 0


def test_async():
    q = Queuey(2)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(q.put_async(1))
    loop.run_until_complete(q.put_async(2))
    assert len(q._items) == 2
    assert len(q._putters) == 0
    assert len(q._getters) == 0

    assert loop.run_until_complete(q.get_async()) == 1
    assert loop.run_until_complete(q.get_async()) == 2
    assert len(q._items) == 0
    assert len(q._putters) == 0
    assert len(q._getters) == 0


def test_noblock():
    q = Queuey(1)
    item, future_get = q._get_noblock()
    assert item is None
    assert future_get is not None
    assert future_get.done() is False

    future_put = q._put_noblock(1)
    assert future_put is None
    assert future_get.done() is True
    assert future_get.result() == 1

    future_put = q._put_noblock(2)
    assert future_put is None

    future_put = q._put_noblock(3)
    assert future_put is not None
    assert future_put.done() is False

    item, future_get = q._get_noblock()
    assert item == 2
    assert future_get is None
    assert future_put.done() is True

    item, future_get = q._get_noblock()
    assert item == 3
    assert future_get is None


def test_max_unfinished_queue():
    q = Queuey(maxsize=2)
    assert q.empty()
    assert q.available_slots() == 2

    q.put_sync(None)
    assert not q.full()
    assert not q.empty()
    assert q.available_slots() == 1

    q.put_sync(None)
    assert q.full()
    assert q.available_slots() == 0

    q.get_sync()
    assert q.full()
    assert q.available_slots() == 0

    q.task_done()
    assert not q.full()
    assert q.available_slots() == 1

    q.get_sync()
    assert not q.empty()
    assert q.available_slots() == 1

    q.task_done()
    assert q.empty()
    assert q.available_slots() == 2


def test_too_many_task_done():
    q = Queuey(10)
    with pytest.raises(ValueError):
        q.task_done()
