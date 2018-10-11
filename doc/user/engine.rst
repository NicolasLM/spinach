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

Namespaces allow to identify and isolate multiple Spinach engines running on
the same Python interpreter and/or sharing the same Redis server.

Having multiple engines on the same interpreter is rare but can happen when
using the Flask integration with an app factory. In this case using different
namespaces is important to avoid signals sent from one engine to be received by
another engine.

When multiple Spinach Engines use the same Redis server, for example when
production and staging share the same database, different namespaces must be
used to make sure they do not step on each other's feet.

The production application would contain::

    spin = Engine(RedisBroker(), namespace='prod')

While the staging application would contain::

    spin = Engine(RedisBroker(), namespace='stg')

.. note:: Using different Redis database numbers (0, 1, 2...) for different
          environments is not enough as Redis pubsubs are shared among
          databases. Namespaces solve this problem.
