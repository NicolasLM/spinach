Spinach
=======

Release v\ |version|. (:ref:`Installation <install>`)

Spinach is a Redis task queue for Python 3 heavily inspired by Celery and RQ.

Distinctive features:

- Threaded and asyncio workers
- At-least-once or at-most-once delivery per task
- Periodic tasks without an additional process
- Concurrency limits on queued jobs
- Scheduling of tasks in batch
- Embeddable workers for easier testing
- Integrations with :ref:`Flask, Django, Logging, Sentry and Datadog
  <integrations>`
- See :ref:`design choices <design>` for more details

Installation::

   pip install spinach

Quickstart

.. literalinclude:: ../examples/quickstart.py

The :class:`Engine` is the central part of Spinach, it allows to define tasks, schedule jobs to
execute in the background and start background workers. :ref:`More details <engine>`.

The Broker is the backend that background workers use to retrieve jobs to execute. Spinach provides
two brokers: MemoryBroker for development and RedisBroker for production.

The :meth:`Engine.task` decorator is used to register tasks. It requires at least a `name` to
identify the task, but other options can be given to customize how the task behaves. :ref:`More
details <tasks>`.

Background jobs can then be scheduled by using either the task name or the task function::

    spin.schedule('compute', 5, 3)  # identify a task by its name
    spin.schedule(compute, 5, 3)    # identify a task by its function

Getting started with spinach:

.. toctree::
    :maxdepth: 1

    user/install
    user/tasks
    user/jobs
    user/engine
    user/queues
    user/asyncio
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
