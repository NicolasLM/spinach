.. _faq:

FAQ
===

Should I use Spinach?
---------------------

Spinach was designed from the ground up to be reliable. It is built using proven
technologies (Redis, Python queues, thread pools...), is heavily tested and in
my experience just works.

The project has been around for long enough that I am now confident it is a good
option among task frameworks. If after reading this documentation you feel like
giving it a try, go for it!

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

What is the licence?
--------------------

Spinach is released under the :download:`BSD license <../../LICENSE>`.

