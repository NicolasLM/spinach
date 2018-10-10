from django.core.management.base import BaseCommand
try:
    from raven.contrib.django.models import client as raven_client
except ImportError:
    raven_client = None

from spinach.const import DEFAULT_QUEUE, DEFAULT_WORKER_NUMBER
from spinach.contrib.sentry import register_sentry

from ...apps import spin


class Command(BaseCommand):
    help = 'Run Spinach workers'

    def add_arguments(self, parser):
        parser.add_argument(
            '--threads',
            dest='threads',
            type=int,
            default=DEFAULT_WORKER_NUMBER,
            help='Number of worker threads to launch'
        )
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
            action='store_true',
            help='Stop workers once the queue is empty'
        )

    def handle(self, *args, **options):

        if raven_client is not None:
            register_sentry(raven_client, spin.namespace)

        spin.start_workers(
            number=options['threads'],
            queue=options['queue'],
            stop_when_queue_empty=options['stop_when_queue_empty']
        )
