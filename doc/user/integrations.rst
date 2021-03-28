.. _integrations:

Integrations
============

Integration with third-party libraries and frameworks.

Logging
-------

Spinach uses the standard Python `logging package
<https://docs.python.org/3/library/logging.html>`_. Its logger prefix is
``spinach``. Spinach does nothing else besides creating its loggers and
emitting log records. The user is responsible for configuring logging before
starting workers.

For simple applications it is enough to use::

    import logging

    logging.basicConfig(
        format='%(asctime)s - %(threadName)s %(levelname)s: %(message)s',
        level=logging.DEBUG
    )

More complex applications will probably use `dictConfig
<https://docs.python.org/3/library/logging.config.html>`_.

Flask
-----

The Flask integration follows the spirit of Flask very closely, it provides two
ways of getting started: a single module approach for minial applications and
an application factory approach for more scalable code.

The Spinach extension for Flask pushes an application context for the duration
of the tasks, which means that it plays well with other extensions like
Flask-SQLAlchemy and doesn't require extra precautions.

Single Module
~~~~~~~~~~~~~

.. literalinclude:: ../../examples/flaskapp.py

Application Factory
~~~~~~~~~~~~~~~~~~~

This more complex layout includes an Application Factory ``create_app`` and an
imaginary ``auth`` Blueprint containing routes and tasks.

``app.py``::

    from flask import Flask
    from spinach import RedisBroker
    from spinach.contrib.flask_spinach import Spinach

    spinach = Spinach()


    def create_app():
        app = Flask(__name__)
        app.config['SPINACH_BROKER'] = RedisBroker()
        spinach.init_app(app)

        from . import auth
        app.register_blueprint(auth.blueprint)
        spinach.register_tasks(app, auth.tasks)

        return app

``auth.py``::

    from flask import Blueprint, jsonify
    from spinach import Tasks

    from .app import spinach


    blueprint = Blueprint('auth', __name__)
    tasks = Tasks()


    @blueprint.route('/')
    def create_user():
        spinach.schedule('send_welcome_email')
        return jsonify({'user_id': 42})


    @tasks.task(name='send_welcome_email')
    def send_welcome_email():
        print('Sending email...')

Running workers
~~~~~~~~~~~~~~~

Workers can be launched from the Flask CLI::

    $ FLASK_APP=examples.flaskapp flask spinach

The working queue and the number of threads can be changed with::

    $ FLASK_APP=examples.flaskapp flask spinach --queue high-priority --threads 20

.. note::
    When in development mode, Flask uses its reloader to automatically restart
    the process when the code changes. When having periodic tasks defined,
    using the MemoryBroker and Flask reloader users may see their periodic
    tasks scheduled each time the code changes. If this is a problem, users
    are encouraged to switch to the RedisBroker for development.

Configuration
~~~~~~~~~~~~~

- ``SPINACH_BROKER``, default ``spinach.RedisBroker()``
- ``SPINACH_NAMESPACE``, defaults to the Flask app name

Django
------

A Django application is available for integrating Spinach into Django
projects.

To get started, add the application ``spinach.contrib.spinachd`` to
``settings.py``::

    INSTALLED_APPS = (
        ...
        'spinach.contrib.spinachd',
    )

On startup, Spinach will look for a ``tasks.py`` module in all installed
applications. For instance ``polls/tasks.py``::

    from spinach import Tasks

    from .models import Question

    tasks = Tasks()


    @tasks.task(name='polls:close_poll')
    def close_poll(question_id: int):
        Question.objects.get(pk=question_id).delete()

Tasks can be easily scheduled from views::

    from .models import Question
    from .tasks import tasks

    def close_poll_view(request, question_id):
        question = get_object_or_404(Question, pk=question_id)
        tasks.schedule('polls:close_poll', question.id)

Users of the Django Datadog app get their jobs reported to Datadog APM
automatically in task workers.

Running workers
~~~~~~~~~~~~~~~

Workers can be launched from ``manage.py``::

    $ python manage.py spinach

The working queue and the number of threads can be changed with::

    $ python manage.py spinach --queue high-priority --threads 20

Sending emails in the background
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Spinach app provides an ``EMAIL_BACKEND`` allowing to send emails as
background tasks. To use it simply add it to ``settings.py``::

    EMAIL_BACKEND = 'spinach.contrib.spinachd.mail.BackgroundEmailBackend'
    SPINACH_ACTUAL_EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'

Emails can then be sent using regular Django functions::

    from django.core.mail import send_mail

    send_mail('Subject', 'Content', 'sender@example.com', ['receiver@example.com'])

Periodically clearing expired sessions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Projects using ``django.contrib.sessions`` must remove expired session from the
database from time to time. Django comes with a management command to do that
manually, but this can be automated.

Spinach provides a periodic task, disabled by default, to do that. To enable it
give it a periodicity in ``settings.py``. For instance to clear sessions once
per week::

    from datetime import timedelta

    SPINACH_CLEAR_SESSIONS_PERIODICITY = timedelta(weeks=1)

Configuration
~~~~~~~~~~~~~

- ``SPINACH_BROKER``, default ``spinach.RedisBroker()``
- ``SPINACH_NAMESPACE``, default ``spinach``
- ``SPINACH_ACTUAL_EMAIL_BACKEND``, default
  ``django.core.mail.backends.smtp.EmailBackend``
- ``SPINACH_CLEAR_SESSIONS_PERIODICITY``, default ``None`` (disabled)

Sentry
------

With the Sentry integration, failing jobs can be automatically reported to
`Sentry <https://sentry.io>`_ with full traceback, log breadcrumbs and job
information.

The Sentry integration requires `Sentry SDK
<https://pypi.org/project/sentry-sdk/>`_::

    pip install sentry_sdk

It then just needs to be registered before starting workers::

    import sentry_sdk

    from spinach.contrib.sentry_sdk_spinach import SpinachIntegration

    sentry_sdk.init(
        dsn="https://sentry_dsn/42",
        integrations=[SpinachIntegration(send_retries=False)]
    )


Datadog
-------

With the Datadog integration, all jobs are automatically reported to
Datadog APM.

The integration requires `ddtrace <https://pypi.python.org/pypi/ddtrace>`_, the
Datadog APM client for Python::

    pip install ddtrace

The integration just needs to be registered before starting workers::

    from spinach.contrib.datadog import register_datadog

    register_datadog()

    spin = Engine(MemoryBroker())
    spin.start_workers()

This only installs the integration with Spinach, other libraries still need to
be patched by ddtrace. It is recommended to run your application patched as
explained in the ddtrace documentation.

.. autofunction:: spinach.contrib.datadog.register_datadog
