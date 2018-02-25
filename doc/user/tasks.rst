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

.. autofunction:: spinach.task.exponential_backoff

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

Tasks Registry
--------------

Before being attached to a Spinach :class:`Engine`, tasks are created inside
a :class:`Tasks` registry.

This may seem cumbersome for trivial applications, like the examples in this
documentation, but there is a good reason not to directly attach tasks to an
:class:`Engine`.

Attaching tasks to a dumb :class:`Tasks` registry instead allows to compose
large applications in smaller units independent from each other, the same way a
Django project is composed of many small Django apps.

.. autoclass:: spinach.task.Tasks
    :members:

Batch
-----

.. autoclass:: spinach.task.Batch
    :members:
