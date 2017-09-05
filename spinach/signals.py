import copy
from logging import getLogger

import blinker

logger = getLogger(__name__)

__all__ = ['job_started', 'job_finished', 'worker_started',
           'worker_terminated']


class SafeNamedSignal(blinker.NamedSignal):
    """Named signal for misbehaving receivers."""

    def send(self, *sender, **kwargs):
        """Emit this signal on behalf of `sender`, passing on kwargs.

        This is an extension of `Signal.send` that adds two things:
        - Exceptions raised in calling the receiver are logged but do not fail
        - kwargs passed to receivers are deep-copied to avoid safety issues
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
                receiver_kwargs = copy.deepcopy(kwargs)
                rv.append((receiver, receiver(sender, **receiver_kwargs)))
            except Exception:
                logger.exception('Error while dispatching signal "{}" '
                                 'to receiver'.format(self.name))
        return rv

job_started = SafeNamedSignal('job_started')
job_finished = SafeNamedSignal('job_finished')
worker_started = SafeNamedSignal('worker_started')
worker_terminated = SafeNamedSignal('worker_terminated')
