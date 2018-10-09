import click
from flask import current_app, _app_ctx_stack
import spinach
from spinach import signals
from spinach.const import DEFAULT_WORKER_NUMBER, DEFAULT_QUEUE


class Spinach:

    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        app.config.setdefault('SPINACH_BROKER', spinach.RedisBroker())
        app.config.setdefault('SPINACH_NAMESPACE', app.name)
        app.config.setdefault('SPINACH_WORKER_NUMBER', DEFAULT_WORKER_NUMBER)

        app.extensions['spinach'] = spinach.Engine(
            broker=app.config['SPINACH_BROKER'],
            namespace=app.config['SPINACH_NAMESPACE']
        )
        namespace = app.extensions['spinach'].namespace

        @app.cli.command(name='spinach')
        @click.option('--stop-when-queue-empty', is_flag=True, default=False)
        @click.argument('queue', default=DEFAULT_QUEUE)
        def spinach_run_workers(queue, stop_when_queue_empty):

            # If the application uses the Flask/Raven extension, use its
            # raven client for the integration Spinach/Raven
            if 'sentry' in app.extensions:
                from spinach.contrib.sentry import register_sentry
                register_sentry(app.extensions['sentry'].client)

            self.spin.start_workers(
                number=app.config['SPINACH_WORKER_NUMBER'],
                queue=queue,
                stop_when_queue_empty=stop_when_queue_empty
            )

        @signals.job_started.connect_via(namespace)
        def job_started(*args, job=None, **kwargs):
            app.app_context().push()

        @signals.job_finished.connect_via(namespace)
        def job_finished(*args, job=None, **kwargs):
            _app_ctx_stack.pop()

    @property
    def spin(self):
        if self.app is not None:
            return self.app.extensions['spinach']

        try:
            return current_app.extensions['spinach']
        except (AttributeError, TypeError, KeyError):
            raise RuntimeError('Spinach extension not initialized. '
                               'Did you forget to call init_app?')

    def register_tasks(self, app, tasks):
        try:
            app.extensions['spinach'].attach_tasks(tasks)
        except KeyError:
            raise RuntimeError('Spinach extension not initialized. '
                               'Did you forget to call init_app?')

    # Convenience access to common Engine attributes and methods

    @property
    def task(self):
        return self.spin.task

    @property
    def execute(self):
        return self.spin.execute

    @property
    def schedule(self):
        return self.spin.schedule

    @property
    def schedule_at(self):
        return self.spin.schedule_at

    @property
    def schedule_batch(self):
        return self.spin.schedule_batch
