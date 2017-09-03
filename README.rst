Spinach
=======

.. image:: https://travis-ci.org/NicolasLM/spinach.svg?branch=master
    :target: https://travis-ci.org/NicolasLM/spinach
.. image:: https://coveralls.io/repos/github/NicolasLM/spinach/badge.svg?branch=master
    :target: https://coveralls.io/github/NicolasLM/spinach?branch=master
.. image:: https://readthedocs.org/projects/spinach/badge/?version=latest
    :target: http://spinach.readthedocs.io/en/latest/?badge=latest

Redis task queue for Python 3 heavily inspired by Celery and RQ.

Distinctive features:

- Threads first, workers are lightweight
- Explicit, very little black magic that can bite you
- Modern code in Python 3 and Lua
- Workers are embeddable in regular processes for easy testing
- See `Design choices
  <https://spinach.readthedocs.io/en/latest/user/design.html>`_ for more
  details

Quickstart
----------

Install Spinach with pip::

   pip install spinach

Create task and schedule two jobs, one executed now and one later:

.. code:: python

    from datetime import datetime, timedelta, timezone

    from spinach import Tasks, MemoryBroker, Spinach

    tasks = Tasks()


    @tasks.task(name='compute')
    def compute(a, b):
        print('Computed {} + {} = {}'.format(a, b, a + b))


    spin = Spinach(MemoryBroker())
    spin.attach_tasks(tasks)

    # Schedule a job to be executed ASAP
    spin.schedule('compute', 5, 3)

    # Schedule a job to be executed in 10 seconds
    in_10_seconds = datetime.now(timezone.utc) + timedelta(seconds=10)
    spin.schedule_at('compute', in_10_seconds, 20, 10)

    print('Starting workers, ^C to quit')
    spin.start_workers()

Documentation
-------------

The documentation is at `https://spinach.readthedocs.io
<https://spinach.readthedocs.io/en/latest/index.html>`_.

License
-------

BSD 2-clause

