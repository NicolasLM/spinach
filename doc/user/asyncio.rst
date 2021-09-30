.. _asyncio:

Asyncio
=======

Spinach allows to define and run tasks as asyncio coroutines. In this mode the worker is a single
thread that runs all tasks asynchronously. This allows for greater concurrency as well as
compatibility with the asyncio ecosystem.

Creating async tasks
--------------------

To define an asynchronous task, just prefix its definition with the ``async`` keyword::

    @spin.task(name='compute')
    async def compute(a, b):
        await asyncio.sleep(1)
        print('Computed {} + {} = {}'.format(a, b, a + b))

To run the workers in asynchronous mode, pass the ``AsyncioWorkers`` class to ``start_workers``::

    from spinach import AsyncioWorkers

    spin.start_workers(number=256, workers_class=AsyncioWorkers)

When using the asyncio workers, the ``number`` argument can be set quite high because each worker
is just a coroutine, consuming a negligible amount of resources.

Scheduling jobs
---------------

Because internally only workers are asyncio aware, jobs are still sent to Redis using a blocking
socket. This means that to schedule jobs from asynchronous code, care must be taken to send jobs
from outside the event loop. This can be achieve using `asyncio.to_thread
<https://docs.python.org/3/library/asyncio-task.html#asyncio.to_thread>`_::

    await asyncio.to_thread(spin.schedule, compute, 2, 4)

Code scheduling a lot of jobs should use :ref:`batches <batch>` to improve performance.

Example
-------

.. literalinclude:: ../../examples/asyncio_workers.py


.. note:: If an application defines both sync and async tasks, each kind of task should go in its
   own :ref:`queue <queues>` so that sync tasks are picked by threaded workers and async tasks by
   asyncio workers.

.. note:: Not all contrib :ref:`integrations <integrations>` may work with asynchronous workers.
