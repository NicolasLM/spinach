from logging import getLogger

import blinker

logger = getLogger(__name__)

__all__ = [
    'job_started', 'job_finished', 'job_schedule_retry', 'job_failed',
    'worker_started', 'worker_terminated'
]


class SafeNamedSignal(blinker.NamedSignal):
    """Named signal for misbehaving receivers."""

    def send(self, *sender, **kwargs):
        """Emit this signal on behalf of `sender`, passing on kwargs.

        This is an extension of `Signal.send` that changes one thing:
        Exceptions raised in calling the receiver are logged but do not fail
        """
        if len(sender) == 0:
            sender = None
        elif len(sender) > 1:
            raise TypeError('send() accepts only one positional argument, '
                            '%s given' % len(sender))
        else:
            sender = sender[0]

        if not self.receivers:
            return []

        rv = list()
        for receiver in self.receivers_for(sender):
            try:
                rv.append((receiver, receiver(sender, **kwargs)))
            except Exception:
                logger.exception('Error while dispatching signal "{}" '
                                 'to receiver'.format(self.name))
        return rv

    def __repr__(self):
        return 'SafeNamedSignal "{}"'.format(self.name)


# Added signals but also be documented in doc/user/signals.rst
job_started = SafeNamedSignal('job_started', doc='''\
Sent by a worker when a job starts being executed.

Signal handlers receive:

- `namespace` Spinach namespace
- `job` :class:`Job` being executed
''')

job_finished = SafeNamedSignal('job_finished', doc='''\
Sent by a worker when a job finishes execution.

The signal is sent no matter the outcome, even if the job fails or gets
rescheduled for retry.

Signal handlers receive:

- `namespace` Spinach namespace
- `job` :class:`Job` being executed
''')

job_schedule_retry = SafeNamedSignal('job_schedule_retry', doc='''\
Sent by a worker when a job gets rescheduled for retry.

Signal handlers receive:

- `namespace` Spinach namespace
- `job` :class:`Job` being executed
- `err` exception that made the job retry
''')

job_failed = SafeNamedSignal('job_failed', doc='''\
Sent by a worker when a job failed.

A failed job will not be retried.

Signal handlers receive:

- `namespace` Spinach namespace
- `job` :class:`Job` being executed
- `err` exception that made the job fail
''')

worker_started = SafeNamedSignal('worker_started', doc='''\
Sent by a worker when it starts.

Signal handlers receive:

- `namespace` Spinach namespace
- `worker_name` name of the worker starting
''')

worker_terminated = SafeNamedSignal('worker_terminated', doc='''\
Sent by a worker when it shutdowns.

Signal handlers receive:

- `namespace` Spinach namespace
- `worker_name` name of the worker shutting down
''')
