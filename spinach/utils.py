from logging import Logger
from threading import Event
import time
from typing import Callable

from redis import ConnectionError, TimeoutError


def human_duration(duration_seconds: float) -> str:
    """Convert a duration in seconds into a human friendly string."""
    if duration_seconds < 0.001:
        return '0 ms'
    if duration_seconds < 1:
        return '{} ms'.format(int(duration_seconds * 1000))
    return '{} s'.format(int(duration_seconds))


def run_forever(func: Callable, must_stop: Event, logger: Logger):
    while not must_stop.is_set():
        try:
            func()
        except (ConnectionError, TimeoutError) as e:
            logger.warning('Connection issue: %s', e)
            time.sleep(10)
        except Exception:
            logger.exception('Unexpected error')
            time.sleep(10)
