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

In both cases the Spinach workers can be run with the Flask CLI::

    $ FLASK_APP=examples.flaskapp flask spinach

The Spinach extension for Flask pushes an application context for the duration
of the tasks, which means that it plays well with other extensions like
Flask-SQLAlchemy and doesn't require extra precautions.

Users of the Flask-Sentry extension get their errors sent to Sentry
automatically in task workers.

Configuration
~~~~~~~~~~~~~

- ``SPINACH_BROKER``, default ``spinach.RedisBroker()``
- ``SPINACH_NAMESPACE``, default ``app.name``
- ``SPINACH_WORKER_NUMBER``, default 5 threads

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

Sentry
------

With the Sentry integration, failing jobs can be automatically reported to
`Sentry <https://sentry.io>`_ with full traceback, log breadcrumbs and job
information.

The integration requires `Raven <https://pypi.python.org/pypi/raven>`_, the
Sentry client for Python::

    pip install raven

The integration just needs to be registered before starting workers::

    from raven import Client
    from spinach.contrib.sentry import register_sentry

    raven_client = Client('https://sentry_dsn/42')
    register_sentry(raven_client)

    spin = Engine(MemoryBroker())
    spin.start_workers()

.. autofunction:: spinach.contrib.sentry.register_sentry
