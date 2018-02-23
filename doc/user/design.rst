.. _design:

Design choices
==============

I have used the Celery task queue for a long time and while it is a rock solid
piece of software, there are some design decisions that just drive me crazy.

This page presents and explains the key design decisions behind Spinach. It can
be summed up as: explicit is better than implicit. Spinach makes sure that it
does not provide any convenient feature that can backfire in more complex
usages.

Threaded workers
----------------

Spinach workers are threaded while other task queues like Celery or RQ rely on
processes by default.

Threaded workers work best with IO bound tasks: tasks that make requests to
other services, query a database or read files. If your task are CPU bound,
meaning that you do heavy computations in Python, a process based worker will
be more efficient.

Tasks in a typical web application are more often than not IO bound. The choice
of threads as unit of concurrency is a sensible one.

Threads also have the advantage of being lighter than processes, a system can
handle more threads than processes before resources get exhausted.

Thread safety
~~~~~~~~~~~~~

As Spinach workers are threads, care must be taken to make sure that the
application is thread-safe. The good news is that your application is probably
already thread-safe: web frameworks are often run threaded as well, so they
take care of most of the heavy work for you.

You can read an article I wrote for an `introduction to thread-safety
<https://lemanchet.fr/articles/learning-python-3-threading-module.html>`_.

Fork
~~~~

Another reason why Spinach does not use processes for concurrency is because
the ``fork`` system call used to create the workers is a very special one. It
has Copy-On-Write semantics that are unfamiliar to many Python developers.

On the other hand thread-safety is a more understood problem in Python, the
standard library providing most of the solutions to write thread-safe programs.

Not relying on ``fork`` also makes Spinach compatible with Windows.

Embeddable workers
------------------

As workers are just threads they are easily embeddable in any other Python
process. This opens the door to two nice usages:

During automated tests a worker can be launched processing jobs exactly like a
normal worker would do in production. What is more by using an in-memory
broker there is no need for having a Redis server running during tests.

For small web projects, the task workers can be launched from the same process
as the web application. As the application gets bigger the workers can be moved
to a separate process very easily.

Logging
-------

One issue I have with Celery is the way it handles logging: the framework tries
to be too smart, resulting in great pain when the logging setup gets more
complex.

That is why Spinach keeps it simple: as a well behaved library it uses the
standard `logging module <https://docs.python.org/3/library/logging.html>`_ and
writes logs in its own loggers.

The choice of what to do with theses log records is up to the final user.

Jobs scheduled for the future
-----------------------------

Spinach has full support for jobs that need to be executed in the future. These
jobs go to a special queue until the are ready to be launched. At that time
they are moved to a normal queue where they are picked by a worker.

Celery emulates this behavior by immediately sending the task to a worker and
waiting there until the time has come to execute it. It means tasks cannot be
scheduled much in advance without wasting resources in the worker.

Periodic jobs
-------------

One annoying thing with Celery is that you can launch as many distributed
workers as you want but there must be one and only one Celery beat process
running in the cluster at a time.

This approach does not work well with containerized applications that run in a
cluster that often redeploys and move containers around.

All Spinach workers are part of the system that schedules periodic jobs, there
is no need to have a pet in the cattle farm.

.. todo:: Periodic jobs are not yet available in Spinach

Only two brokers
----------------

Spinach lets the user pick between the in-memory broker for local development
and the Redis broker for production. Both support exactly the same set of
features.

Redis was chosen because it is an incredibly versatile database. With Lua
scripting it becomes possible to develop entirely new patterns which are
essential to create a useful and reliable task queue.

Other services like Google PubSub, Amazon SQS or AMQP are very opinionated and
not as versatile as Redis, making them difficult to use within Spinach without
cutting down on features.

Namespace
---------

Multiple Spinach applications (production, staging...) can use the same Redis
database without interfering with each other.

Likewise, a single interpreter can run multiple Spinach applications without
them interfering with each other.

Minimize import side-effects
----------------------------

Spinach encourages users to write applications that have minimal side-effects
when imported. There is no global state that gets created or modified when
importing or using Spinach.

The user is free to use Spinach in a scoped fashion or declaring everything
globally.

This makes it possible for a single interpreter to run multiple Spinach
applications without them interfering with each other, which is particularly
useful for running automated tests.

No worker entrypoint
--------------------

Celery has this ``celery worker`` entrypoint that can be launched from the
command line to load an application and spawn the workers.

The problem I often face is that I never know if a setting should be defined in
my code as part of the app setup or as a flag of this command line.

Moreover command line flags and application settings often have slightly
different names, making things more confusing.

Spinach thus makes it foolproof, you are responsible for configuring the
Spinach app though your Python code. You can read settings from environment
variables, from a file or anything else possible in Python.

It is then easy to use it to create your own entrypoint to launch the workers.
