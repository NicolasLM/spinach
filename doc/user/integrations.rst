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
        format='%(asctime)s - %(levelname)s: %(message)s',
        level=logging.DEBUG
    )

More complex applications will probably use `dictConfig
<https://docs.python.org/3/library/logging.config.html>`_.

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

    spin = Spinach(MemoryBroker())
    spin.start_workers()

