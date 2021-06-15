Spinach
=======

.. image:: https://github.com/NicolasLM/spinach/workflows/Run%20tests/badge.svg?branch=master
    :target: https://github.com/NicolasLM/spinach/actions
.. image:: https://coveralls.io/repos/github/NicolasLM/spinach/badge.svg?branch=master
    :target: https://coveralls.io/github/NicolasLM/spinach?branch=master
.. image:: https://readthedocs.org/projects/spinach/badge/?version=latest
    :target: http://spinach.readthedocs.io/en/latest/?badge=latest
.. image:: https://img.shields.io/badge/IRC-irc.libera.chat-1e72ff.svg?style=flat
    :target: https://kiwiirc.com/nextclient/irc.libera.chat:+6697/#spinach

Redis task queue for Python 3 heavily inspired by Celery and RQ.

Distinctive features:

- Threaded and asyncio workers
- At-least-once or at-most-once delivery per task
- Periodic tasks without an additional process
- Concurrency limits on queued jobs
- Scheduling of tasks in batch
- Integrations with `Flask, Django, Logging, Sentry and Datadog
  <https://spinach.readthedocs.io/en/stable/user/integrations.html>`_
- Embeddable workers for easier testing
- See `design choices
  <https://spinach.readthedocs.io/en/stable/user/design.html>`_ for more
  details

Quickstart
----------

Install Spinach with pip::

   pip install spinach

Create a task and schedule a job to be executed now:

.. code:: python

    from spinach import Engine, MemoryBroker

    spin = Engine(MemoryBroker())


    @spin.task(name='compute')
    def compute(a, b):
        print('Computed {} + {} = {}'.format(a, b, a + b))


    # Schedule a job to be executed ASAP
    spin.schedule(compute, 5, 3)

    print('Starting workers, ^C to quit')
    spin.start_workers()

Documentation
-------------

The documentation is at `https://spinach.readthedocs.io
<https://spinach.readthedocs.io/en/stable/index.html>`_.

IRC channel for online discussions **#spinach** on `irc.libera.chat
<https://kiwiirc.com/nextclient/irc.libera.chat:+6697/#spinach>`_.

License
-------

BSD 2-clause

