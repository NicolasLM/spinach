import contextlib
from datetime import timedelta
from logging import Logger
import random
import signal
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


def run_forever(func: Callable, must_stop: Event, logger: Logger,
                *args, **kwargs):
    attempt = 0
    while not must_stop.is_set():

        start = time.monotonic()
        try:
            func(*args, **kwargs)
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


def call_with_retry(func: Callable, exceptions, max_retries: int,
                    logger: Logger, *args, **kwargs):
    """Call a function and retry it on failure."""
    attempt = 0
    while True:
        try:
            return func(*args, **kwargs)
        except exceptions as e:
            attempt += 1
            if attempt >= max_retries:
                raise

            delay = exponential_backoff(attempt, cap=60)
            logger.warning('%s: retrying in %s', e, delay)
            time.sleep(delay.total_seconds())


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


@contextlib.contextmanager
def handle_sigterm():
    """Handle SIGTERM like a normal SIGINT (KeyboardInterrupt).

    By default Docker sends a SIGTERM for stopping containers, giving them
    time to terminate before getting killed. If a process does not catch this
    signal and does nothing, it just gets killed.

    Handling SIGTERM like SIGINT allows to gracefully terminate both
    interactively with ^C and with `docker stop`.

    This context manager restores the default SIGTERM behavior when exiting.
    """
    original_sigterm_handler = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, original_sigterm_handler)
