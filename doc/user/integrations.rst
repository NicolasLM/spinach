.. _integrations:

Sentry
======

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

