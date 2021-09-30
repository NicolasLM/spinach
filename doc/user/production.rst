.. _production:

Running in Production
=====================

Advices to read before deploying an application using Spinach to production.

Spinach
-------

Since by default Spinach executes jobs in a separate threads, the user's code must be thread-safe.
This is usually quite easy to achieve on a traditional web application because frameworks like
Flask or Django make that straightforward.

Tasks should not store state in the process between invocations. Instead all state must be stored
in an external system, like a database or a cache. This advice also applies to `views` in a web
application.

Redis
-----

Most Spinach features are implemented as Lua scripts running inside Redis. Having a solid
installation of Redis is the key to Spinach reliability.

To ensure that no tasks are lost or duplicated, Redis must be configured with persistence enabled.
It is recommended to use AOF persistence (``appendonly yes``) instead of periodic RDB dumps. The
default of fsync every second (``appendfsync everysec``) is a good trade-off between performance
and security against sudden power failures.

Using Redis as a task queue is very different from using it as a cache. If an application uses
Redis for both task queue and cache, it is recommended to have two separated Redis servers. One
would be configured with persistence and without eviction while the other would have no persistence
but would evict keys when running low on memory.

Finally standard security practices apply: Redis should not accept connections from the Internet
and it should require a password even when connecting locally.

System
------

If the application is deployed on multiple servers it is important that their clocks be
approximately synchronized. This is because Spinach uses the system time to know when a job should
start. Running an `ntp` daemon is highly recommended.

Workers should be started by an init system that will restart them if they get killed or if the
host reboots.

To gracefully shutdown a worker, it is recommended to send it a `SIGINT` or a `SIGTERM` and let it
finish its running jobs. If the worker gets killed before it terminates gracefully, non-retryable
jobs will be lost and retryable jobs will be rescheduled automatically after the worker is
identified as dead, which takes 30 minutes by default. This is important if Spinach workers run in
docker containers because docker gives 10 seconds to a container to finish before killing it.

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
- The user's code is thread-safe when using the default threaded workers
- Tasks do not store state in the process between invocations
- Logging is configured and exceptions are sent to Sentry, see
  :doc:`integrations`
- Different queues are used if tasks have different usage pattens, see
  :doc:`queues`
- Different namespaces are used if multiple Spinach applications share the same
  Redis server, see :doc:`engine`

Redis:

- Redis uses AOF persistence
- Redis does not evict keys when running low on memory
- The Redis server used by Spinach is not also used as a cache
- Connections are secured by a long password
- Connections are encrypted if they go through the public Internet

System:

- Servers have their clock synchronized by ntp
- Workers get restarted by an init system if they get killed
