.. _production:

Running in Production
=====================

Advices to read before deploying an application using Spinach to production.

Spinach
-------

Since Spinach relies heavily on threads the user's code MUST be thread-safe.
This is usually quite easy to achieve on a traditional web application because
frameworks like Flask or Django make that obvious.

Tasks should not store state in the process between invocations. Instead all
state must be stored in an external system, like a database or a cache. This
advice also applies to `views` in a web application.

Redis
-----

.. note:: TODO: Document advices for Redis configuration

System
------

If the application is deployed on multiple servers it is important that their
clocks be approximately synchronized. This is because Spinach uses the system
time to know when a job should start. Running an `ntp` daemon is highly
recommended.

Workers should be started by an init system that will restart them if they get
killed or if the host reboots.

Production Checklist
--------------------

Spinach:

- Tasks that are NOT safe to be retried have their `max_retries` set to `0`
- Tasks that are safe to be retried have their `max_retries` set to a positive
  number
- Retries happen after an exponential delay with randomized jitter (the
  default)
- Task `args` and `kwargs` are JSON serializable and small in size
- Jobs are sent in :class:`Batch` to the broker when multiple jobs are to be
  scheduled at once
- The user's code is thread-safe
- Tasks do not store state in the process between invocations
- Logging is configured and exceptions are sent to Sentry, see
  :doc:`integrations`
- Different queues are used if tasks have different usage pattens, see
  :doc:`queues`
- Different namespaces are used if multiple Spinach applications share the same
  Redis server, see :doc:`engine`

Redis:

- Connections are secured by a long password
- Connections are encrypted if they go through the public Internet

System:

- Servers have their clock synchronized by ntp
- Workers get restarted by an init system if they get killed
