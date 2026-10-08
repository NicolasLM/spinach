.. _postgres:

Postgres
========

:class:`spinach.brokers.postgres.PostgresBroker` stores jobs in Postgres.
Queues, future jobs, periodic tasks, concurrency limits, and namespaces are
rows in that database. A job can be inserted in the same transaction as the
application's own rows, so both commit or both roll back.

Use
---

Install the Postgres extra::

    pip install spinach[postgres]

Read the connection string from the environment or a secrets manager. Do not
put a password in source::

    import os

    from spinach import Engine
    from spinach.brokers.postgres import PostgresBroker

    dsn = os.environ['SPINACH_POSTGRES_DSN']
    spin = Engine(PostgresBroker(dsn), namespace='prod')

Define a task, schedule it, and start workers::

    @spin.task(name='process_order')
    def process_order(order_id):
        print(order_id)

    spin.schedule(process_order, order_id)
    spin.start_workers()

To co-commit, open a transaction on the same database and schedule inside
:meth:`Engine.join_transaction`. The broker does not commit or roll back
that connection. Workers cannot see the job until the caller commits.
``NOTIFY`` is sent at commit and only wakes a worker. The job row is what
is durable.

.. code:: python

    import psycopg

    with psycopg.connect(dsn) as conn:
        with conn.transaction():
            conn.execute(
                'INSERT INTO orders (id) VALUES (%s)',
                (order_id,),
            )
            with spin.join_transaction(conn):
                spin.schedule(process_order, order_id)

A runnable copy is ``examples/postgres_atomic.py``.

Django, inside ``transaction.atomic()``::

    from django.db import transaction
    from spinach.brokers.postgres import django_connection

    with transaction.atomic():
        Order.objects.create(id=order_id)
        with spin.join_transaction(django_connection()):
            spin.schedule(process_order, order_id)

SQLAlchemy, inside ``session.begin()``::

    from spinach.brokers.postgres import sqlalchemy_connection

    with session.begin():
        session.add(order)
        with spin.join_transaction(sqlalchemy_connection(session)):
            spin.schedule(process_order, order.id)

The Flask extension exposes the same ``join_transaction`` method.

Rules for the joined connection:

- It must be the same database cluster and database as the broker.
  The broker compares the cluster system identifier and
  ``current_database()``. ``spinach_queue_job`` and
  ``spinach_future_job`` must be the same tables the broker resolved.
  A ``search_path`` that finds a different copy is rejected.
- Autocommit must be off.
- A task argument named ``connection`` is still passed to the task. It is
  not the broker connection.
- A retried request that commits twice inserts two jobs. Make the
  application row unique if the business action must happen once.

Deploy
------

Application tables and Spinach tables must live in one database when jobs
are co-committed. Namespaces still isolate applications that share that
database. Give each environment its own namespace.

``PostgresBroker(dsn)`` creates tables on startup (``ensure_schema=True``).
That is fine for development. In production construct the broker with
``ensure_schema=False`` and apply ``spinach/brokers/postgres_schema.sql``
with a migration role. Startup resolves each Spinach table on
``search_path`` and checks column types, nullability, primary keys,
``spinach_queue_job.position``'s sequence default,
``spinach_idempotency.created_at``'s ``now()`` default, and
``spinach_schema`` version 1. A missing table, a stub earlier on the
path, a wrong type, a missing default, or another version raises
``RuntimeError``.

The worker role is not a superuser and does not own the tables. Grant it
``SELECT``, ``INSERT``, ``UPDATE``, and ``DELETE`` on the Spinach tables,
and ``USAGE`` on ``spinach_queue_job_position_seq``. An application role
that only dispatches jobs needs ``INSERT`` on ``spinach_queue_job`` and
``spinach_future_job``, plus ``USAGE`` on that sequence.

Production connections use a password and SCRAM-SHA-256. Set
``password_encryption = scram-sha-256`` on the server. Do not enable MD5
password authentication. ``require_ssl`` defaults to True. The session
must use TLS with ``sslmode=verify-full``, including a connection passed
to ``join_transaction``. An encrypted session that does not verify the
server certificate is rejected. Set ``require_ssl`` to False only for a
local server that has no TLS. Put ``sslmode=verify-full`` and
``sslrootcert`` in the DSN. The CA file comes from the environment. Do
not embed certificate data in Python source.

Before trusting the server certificate, inspect it::

    openssl x509 -text -noout -in server.crt

It must be inside its validity dates, use at least an RSA 2048-bit key or
an ECDSA P-256 key, and be signed with SHA-256 or stronger. A self-signed
certificate is only for local tests where trust is configured on purpose.

Do not accept Postgres connections from the internet. Keep server clocks
synchronized with ntp, because scheduling uses the system clock. Start
workers from an init system that restarts them after a crash or a reboot.

Control
-------

``start_workers`` launches the arbiter and the worker threads. The defaults
are five workers, the queue named ``spinach``, and a blocking call that
runs until a signal arrives::

    spin.start_workers(number=5, queue='spinach', block=True)

``stop_when_queue_empty=True`` exits once that queue has no waiting job.
Use it for a one-off script. Attach a task to another queue with the task
decorator, then pass that queue name here. See :doc:`queues`.

Stop a blocking worker with ``SIGINT`` or ``SIGTERM``, or call
``spin.stop_workers()``. In-flight jobs finish. A hard kill does not. Jobs
with ``max_retries`` set to ``0`` are not put back. Retryable jobs return
after ``broker_dead_threshold_seconds`` (30 minutes). A process supervisor
that kills the process sooner than that, such as a 10 second container
stop, drops non-retryable work.

``broker.flush()`` deletes one namespace. It does not drop tables. Use it
to reset tests. ``stop()`` leaves the connection pool open so ``flush()``
still works. ``broker.close()`` stops the listener, if it is running, and
closes the pool. Call it after workers have stopped.

Broker logs use the logger name ``spinach.broker``. The connection string
is not written to the log. See :doc:`production` for the rest of the
production checklist, and :doc:`integrations` for logging.
