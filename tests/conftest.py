from datetime import datetime

import pytest


_utcnow = datetime(2017, 9, 2, 8, 50, 56, 482169)


@pytest.fixture
def patch_utcnow(monkeypatch):
    """Patch datetime.datetime.

    It is not possible to patch it like a normal Python object, so the
    reference is replaced completely by a custom class.
    The test function can get and set the fake time with get_utcnow() and
    set_utcnow().
    """
    global _utcnow

    # Reset the time before each test
    _utcnow = datetime(2017, 9, 2, 8, 50, 56, 482169)

    class MyDatetime:
        @classmethod
        def utcnow(cls):
            return _utcnow

        @classmethod
        def fromtimestamp(cls, *args, **kwargs):
            return datetime.fromtimestamp(*args, **kwargs)

    monkeypatch.setattr('spinach.brokers.base.datetime', MyDatetime)
    monkeypatch.setattr('spinach.brokers.redis.redis.datetime', MyDatetime)
    monkeypatch.setattr('spinach.job.datetime', MyDatetime)


def get_utcnow() -> datetime:
    return _utcnow


def set_utcnow(utcnow: datetime):
    global _utcnow
    _utcnow = utcnow
