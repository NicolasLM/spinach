import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest


_now = datetime(2017, 9, 2, 8, 50, 56, 482169, timezone.utc)


@pytest.fixture
def patch_now(monkeypatch):
    """Patch datetime.datetime.

    It is not possible to patch it like a normal Python object, so the
    reference is replaced completely by a custom class.
    The test function can get and set the fake time with get_now() and
    set_now().
    """
    global _now

    # Reset the time before each test
    _now = datetime(2017, 9, 2, 8, 50, 56, 482169, timezone.utc)

    class MyDatetime:
        @classmethod
        def now(cls, tz=None):
            # All code within Spinach shall create TZ aware datetime
            assert tz == timezone.utc
            return _now

        @classmethod
        def fromtimestamp(cls, *args, **kwargs):
            return datetime.fromtimestamp(*args, **kwargs)

    monkeypatch.setattr('spinach.brokers.base.datetime', MyDatetime)
    monkeypatch.setattr('spinach.brokers.postgres.datetime', MyDatetime)
    monkeypatch.setattr('spinach.job.datetime', MyDatetime)
    monkeypatch.setattr('spinach.engine.datetime', MyDatetime)
    monkeypatch.setattr('spinach.task.datetime', MyDatetime)


def get_now() -> datetime:
    return _now


def set_now(now: datetime):
    global _now
    if now.tzinfo is None:
        # Make it a TZ aware datetime here for convenience to avoid over
        # verbose tests
        now = now.replace(tzinfo=timezone.utc)
    _now = now


CONFIG_YAML = Path(__file__).parent / 'docker-compose.yml'
COMPOSE = ('docker', 'compose', '--file', str(CONFIG_YAML))


def pytest_collection_finish(session):
    if not session.config.option.collectonly:
        # --wait implies --detach, but explicit is better than implicit...
        subprocess.run(COMPOSE + ('up', '--detach', '--wait'), check=True)


def pytest_sessionfinish(session, exitstatus):
    if not session.config.option.collectonly:
        subprocess.run(COMPOSE + ('down',), check=True)
