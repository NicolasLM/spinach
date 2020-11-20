.. _tasks:

Tasks
=====

A tasks is a unit of code, usually a function, to be executed in the background
on remote workers.

To define a task::

    from spinach import Tasks

    tasks = Tasks()

    @tasks.task(name='add')
    def add(a, b):
        print('Computed {} + {} = {}'.format(a, b, a + b))

.. note:: The `args` and `kwargs` of a task must be JSON serializable.

Retries
-------

Spinach knows two kinds of tasks: the ones that can be retried safely
(idempotent tasks) and the ones that cannot be retried safely (non-idempotent
tasks). Since Spinach cannot guess if a task code is safe to be retried
multiple times, it must be annotated when the task is created.

Non-Retriable Tasks
~~~~~~~~~~~~~~~~~~~

Spinach assumes that by default tasks are not safe to be retried (tasks are
assumed to have side effects).

These tasks are defined with `max_retries=0` (the default)::

    @tasks.task(name='foo')
    def foo(a, b):
        pass

- use at-most-once delivery, the job may never even start
- jobs are not automatically retried in case of errors

Retriable Tasks
~~~~~~~~~~~~~~~

Idempotent tasks can be executed multiple times without changing the result
beyond the initial application. It is a nice property to have and most tasks
should try to be idempotent to gracefully recover from errors.

Retriable tasks are defined with a positive `max_retries` value::

    @tasks.task(name='foo', max_retries=10)
    def foo(a, b):
        pass

- use at-least-once delivery, the job may be executed more than once
- jobs are automatically retried, up to `max_retries` times, in case of errors

Retrying
~~~~~~~~

When a retriable task is being executed it will be retried when it encounters
an unexpected exception::

    @tasks.task(name='foo', max_retries=10)
    def foo(a, b):
        l = [0, 1, 2]
        print(l[100])  # Raises IndexError

To allow the system to recover gracefully, a default backoff strategy is
applied.

.. autofunction:: spinach.utils.exponential_backoff

To be more explicit, a task can also raise a :class:`RetryException` which
allows to precisely control when it should be retried::

    from spinach import RetryException

    @tasks.task(name='foo', max_retries=10)
    def foo(a, b):
        if status_code == 429:
            raise RetryException(
                'Should retry in 10 minutes',
                at=datetime.now(tz=timezone.utc) + timedelta(minutes=10)
            )


.. autoclass:: spinach.task.RetryException

A task can also raise a :class:`AbortException` for short-circuit behavior:

.. autoclass:: spinach.task.AbortException

Periodic tasks
--------------

Tasks marked as periodic get automatically scheduled. To run a task every 5
seconds:

.. literalinclude:: ../../examples/periodic.py

Periodic tasks get scheduled by the workers themselves, there is no need to
run an additional process only for that. Of course having multiple workers on
multiple machine is fine and will not result in duplicated tasks.

Periodic tasks run at most every `period`. If the system scheduling periodic
tasks gets delayed, nothing compensates for the time lost. This has the added
benefit of periodic tasks not being scheduled if all the workers are down for
a prolonged amount of time. When they get back online, workers won't have a
storm of periodic tasks to execute.

Tasks Registry
--------------

Before being attached to a Spinach :class:`Engine`, tasks are created inside
a :class:`Tasks` registry.

Attaching tasks to a :class:`Tasks` registry instead of directly to the
:class:`Engine` allows to compose large applications in smaller units
independent from each other, the same way a Django project is composed of many
small Django apps.

This may seem cumbersome for trivial applications, like the examples in this
documentation or some single-module projects, so those can create tasks
directly on the :class:`Engine` using::


    spin = Engine(MemoryBroker())

    @spin.task(name='fast')
    def fast():
        time.sleep(1)

.. note:: Creating tasks directly in the :class:`Engine` is a bit like creating
          a Flask app globally instead of using an `app factory`: it works
          until a change introduces a circular import. Its usage should really
          be limited to tiny projects.

.. autoclass:: spinach.task.Tasks
    :members:

Batch
-----

.. autoclass:: spinach.task.Batch
    :members:
