.. _queues:

Queues
======

Queues are an optional feature that allows directing a set of tasks to specific workers.

Queues are useful when different tasks have different usage patterns, for instance one task being
fast and high priority while another task is slow and low-priority. To prevent the slow task from
blocking the execution of the fast one, each task can be attached to its own queue:

.. literalinclude:: ../../examples/queues.py

The task decorator accepts an optional queue name that binds the task to a specific queue.
Likewise, passing a queue name to `start_workers` restricts workers to executing only tasks of this
particular queue.

.. note:: By default all tasks and all workers use the ``spinach`` queue

.. note:: Namespaces and queues are different concepts. While queues share the same Spinach
   :class:`Engine`, namespaces make two Spinach Engines invisible to each other while still using
   the same broker.
