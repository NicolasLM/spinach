from datetime import timedelta
from logging import Logger
import random
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
    attempt = 0
    while not must_stop.is_set():

        start = time.monotonic()
        try:
            func()
        except Exception as e:

            # Reset the attempt counter if `func` ran for 10 minutes without
            # an error
            if int(time.monotonic() - start) > 600:
                attempt = 1
            else:
                attempt += 1

            delay = exponential_backoff(attempt, cap=120)
            if isinstance(e, (ConnectionError, TimeoutError)):
                logger.warning('Connection issue: %s. Retrying in %s', e,
                               delay)
            else:
                logger.exception('Unexpected error. Retrying in %s', delay)

            must_stop.wait(delay.total_seconds())


def exponential_backoff(attempt: int, cap: int=1200) -> timedelta:
    """Calculate a delay to retry using an exponential backoff algorithm.

    It is an exponential backoff with random jitter to prevent failures
    from being retried at the same time. It is a good fit for most
    applications.

    :arg attempt: the number of attempts made
    :arg cap: maximum delay, defaults to 20 minutes
    """
    base = 3

    temp = min(base * 2 ** attempt, cap)
    return timedelta(seconds=temp / 2 + random.randint(0, temp / 2))
