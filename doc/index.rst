Spinach
=======

Release v\ |version|. (:ref:`Installation <install>`)

Spinach is a Redis task queue for Python 3 heavily inspired by Celery and RQ.

Distinctive features:

- At-least-once or at-most-once delivery per task
- Periodic tasks without an additional process
- Scheduling of tasks in batch
- Embeddable workers for easier testing
- Integrations with :ref:`Flask, Logging and Sentry <integrations>`
- Python 3, threaded, explicit... see :ref:`design choices <design>` for more
  details

Installation::

   pip install spinach

Quickstart

.. literalinclude:: ../examples/quickstart.py

Getting started with spinach:

.. toctree::
    :maxdepth: 1

    user/install
    user/tasks
    user/jobs
    user/engine
    user/queues
    user/integrations
    user/signals
    user/production
    user/design
    user/faq

Hacking guide:

.. toctree::
    :maxdepth: 1

    hacking/contributing
    hacking/internals
