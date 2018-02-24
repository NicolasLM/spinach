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
- Task `args` and `kwargs` are JSON serializable
- The user's code is thread-safe
- Tasks do not store state in the process between invocations
- Configure logging and send exceptions to Sentry, see :doc:`integrations`
- Use different queues if tasks have different usage pattens, see :doc:`queues`
- Use different namespaces if multiple Spinach applications share the same
  Redis, see :doc:`engine`

Redis:

- Connections are secured by a long password
- Connections are encrypted if they go through the public Internet

System:

- Servers have their clock synchronized by ntp
- Workers get restarted by an init system if they get killed
