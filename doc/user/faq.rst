.. _faq:

Frequently Asked Questions
==========================

What is the licence?
--------------------

Spinach is released under the :download:`BSD license <../../LICENSE>`.

Should I use Spinach?
---------------------

Spinach is a very young software, if your business depends heavily on
background tasks you should probably go with Celery or RQ.

That being said, Spinach relies on proven technologies (Redis, Python queues,
thread pools...) and is heavily tested.

Threads are not enough, can I use Processes?
--------------------------------------------

Threading is the only concurrency primitive however it is possible to run
many processes each containing one worker thread. This will open more
connections to Redis, but Redis is known to support thousands of concurrent
connections so this should not be a problem.

The best approach to achieve this is to rely on an init system like systemd,
supervisord or docker. The init system will be responsible for spawning the
correct number of processes and making sure they are properly restarted if they
terminate prematurely.

Writing this init system yourself in Python using the multiprocessing module
is possible but it must not import your actual application using Spinach. This
is because mixing threads and forks in a single interpreter is a minefield.
Anyway you are probably better off using a battle tested init system.
