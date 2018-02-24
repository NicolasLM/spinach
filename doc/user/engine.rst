.. _engine:

Engine
======

The Spinach :class:`Engine` is what connects tasks, jobs, brokers and workers
together.

It is possible, but unusual, to have multiple Engines running in the same
Python interpreter.

.. autoclass:: spinach.engine.Engine
    :members:

Namespace
---------

When multiple Spinach Engines use the same Redis server, for example when
production and staging share the same database, different namespaces are used
to make sure they do not step on each other's feet.

The production application would contain::

    spin = Engine(RedisBroker(), namespace='prod')

While the staging application would contain::

    spin = Engine(RedisBroker(), namespace='stg')

.. note:: Using different Redis database numbers (0, 1, 2...) for different
          environments is not enough as Redis pubsubs are shared among
          databases. Namespaces solve this problem.
