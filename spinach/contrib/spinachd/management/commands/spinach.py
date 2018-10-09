import signal

from django.core.management.base import BaseCommand
try:
    from raven.contrib.django.models import client as raven_client
except ImportError:
    raven_client = None

from spinach.const import DEFAULT_QUEUE
from spinach.contrib.sentry import register_sentry

from ...apps import spin
from ...settings import SPINACH_WORKER_NUMBER


class Command(BaseCommand):
    help = 'Run spinach workers'

    def add_arguments(self, parser):
        parser.add_argument(
            '--queue',
            dest='queue',
            default=DEFAULT_QUEUE,
            help='Queue to consume'
        )
        parser.add_argument(
            '--stop-when-queue-empty',
            dest='stop_when_queue_empty',
            default=False,
            action='store_true'
        )

    def handle(self, *args, **options):

        if raven_client is not None:
            register_sentry(raven_client, spin.namespace)

        def handle_sigterm(*args):
            raise KeyboardInterrupt()

        signal.signal(signal.SIGTERM, handle_sigterm)

        spin.start_workers(
            number=SPINACH_WORKER_NUMBER,
            queue=options['queue'],
            stop_when_queue_empty=options['stop_when_queue_empty']
        )
