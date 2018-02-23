Spinach
=======

Release v\ |version|. (:ref:`Installation <install>`)

Spinach is a Redis task queue for Python 3 heavily inspired by Celery and RQ.

Distinctive features:

- Threads first, workers are lightweight
- Explicit, very little black magic that can bite you
- Modern code in Python 3 and Lua
- Workers are embeddable in regular processes for easy testing
- See :ref:`Design choices <design>` for more details

Installation::

   pip install spinach

Quickstart

.. literalinclude:: ../examples/quickstart.py

Getting started with spinach:

.. toctree::
    :maxdepth: 1

    user/install
    user/tasks
    user/jobs
    user/queues
    user/integrations
    user/signals
    user/production
    user/design
    user/faq

Hacking guide:

.. toctree::
    :maxdepth: 1

    hacking/contributing
    hacking/internals
