.. _contributing:

Contributing
============

This page contains the few guidelines and conventions used in the code base.

Pull requests
-------------

The development of Spinach happens on GitHub, the main repository is
`https://github.com/NicolasLM/spinach <https://github.com/NicolasLM/spinach>`_. To contribute to
Spinach:

* Fork ``NicolasLM/spinach``
* Clone your fork
* Create a feature branch ``git checkout -b my_feature``
* Commit your changes
* Push your changes to your fork ``git push origin my_feature``
* Create a GitHub pull request against ``NicolasLM/spinach``'s master branch

.. note:: Avoid including multiple commits in your pull request, unless it adds value to a future
   reader. If you need to modify a commit, ``git commit --amend`` is your friend. Write
   a meaningful commit message, see `How to write a commit message
   <http://chris.beams.io/posts/git-commit/>`_.

Python sources
--------------

The code base follows `pep8 <https://www.python.org/dev/peps/pep-0008/>`_ guidelines with lines
wrapping at the 79th character. You can verify that the code follows the conventions with::

    $ uv run pycodestyle --ignore=E252,W503,W504 spinach tests

Running tests is an invaluable help when adding a new feature or when refactoring. Try to add the
proper test cases in ``tests/`` together with your patch. The test suite can be run with pytest::

    $ uv run pytest tests

Spinach uses `uv <https://docs.astral.sh/uv/>`_ to manage the development environment. The first
``uv run`` command creates a virtualenv and installs Spinach and its development dependencies. The
PostgreSQL broker tests additionally require a database DSN, for example::

    $ SPINACH_TEST_POSTGRES_DSN=postgresql://postgres@127.0.0.1:5432/spinach uv run pytest tests

Because the Redis and PostgreSQL broker tests require running servers, pytest starts them in
containers via docker-compose before the tests and tears them down when done.

Compatibility
-------------

Spinach runs on all versions of Python starting from 3.6. Tests are run via GitHub actions to
ensure that.

Documentation sources
---------------------

Documentation is located in the ``doc`` directory of the repository. It is written in
`reStructuredText <http://docutils.sourceforge.net/docs/ref/rst/restructuredtext.html>`_ and built
with `Sphinx <http://www.sphinx-doc.org/en/stable/index.html>`_.

If you modify the docs, make sure it builds without errors::

    $ cd doc/
    $ make html

The generated HTML pages should land in ``doc/_build/html``.
