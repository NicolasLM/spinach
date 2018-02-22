.. _signals:

Signals
=======

Signals are events broadcasted when something happens in Spinach, like a job
starting or a worker shutting down.

Subscribing to signals allows your code to react to internal events in a
composable and reusable way.

Subscribing to signals
----------------------

Subscribing to a signal is done via its ``connect`` decorator::

    from spinach import signals

    @signals.job_started.connect
    def job_started(namespace, job, **kwargs):
        print('Job {} started'.format(job))

The first argument given to your function is always the namespace of your
Spinach, the following arguments depend on the signal itself.

Subscribing to signals of a specific Spinach
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As your application gets bigger you may end up running multiple ``Spinach`` in
the same interpreter. The ``connect_via`` decorator allows to subscribe to the
signals sent by a specific ``Spinach``::

    from spinach import Spinach, MemoryBroker, signals

    foo_spin = Spinach(MemoryBroker(), namespace='foo')
    bar_spin = Spinach(MemoryBroker(), namespace='bar')

    @signals.job_started.connect_via(foo_spin.namespace)
    def job_started(namespace, job, **kwargs):
        print('Job {} started on Foo'.format(job))

In this example only signals sent by the `foo` ``Spinach`` will be received.

Available signals
-----------------

.. todo:: Add the documentation of individual signals here

Tips
----

Received objects
~~~~~~~~~~~~~~~~

Objects received via signals should not be modified in handlers as it could
break something in Spinach internals.

Exceptions
~~~~~~~~~~

If your receiving function raises an exception while processing a signal, this
exception will be logged in the ``spinach.signals`` logger.

Going further
~~~~~~~~~~~~~

Have a look at the `blinker documentation <http://pythonhosted.org/blinker/>`_
for other ways using signals.
