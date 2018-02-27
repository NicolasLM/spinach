.. _contributing:

Contributing
============

This page contains the few guidelines and conventions used in the code base.

Pull requests
-------------

The development of Spinach happens on GitHub, the main repository is
`https://github.com/NicolasLM/spinach <https://github.com/NicolasLM/spinach>`_.
To contribute to Spinach:

* Fork ``NicolasLM/spinach``
* Clone your fork
* Create a feature branch ``git checkout -b my_feature``
* Commit your changes
* Push your changes to your fork ``git push origin my_feature``
* Create a GitHub pull request against ``NicolasLM/spinach``'s master branch

.. note:: Avoid including multiple commits in your pull request, unless it adds
          value to a future reader. If you need to modify a commit,
          ``git commit --amend`` is your friend. Write a meaningful commit
          message, see `How to write a commit message
          <http://chris.beams.io/posts/git-commit/>`_.

Python sources
--------------

The code base follows `pep8 <https://www.python.org/dev/peps/pep-0008/>`_
guidelines with lines wrapping at the 79th character. You can verify that the
code follows the conventions with::

    $ pep8 spinach tests

Running tests is an invaluable help when adding a new feature or when
refactoring. Try to add the proper test cases in ``tests/`` together with your
patch. The test suite can be run with pytest::

    $ pytest tests

Compatibility
-------------

Spinach runs on all versions of Python starting from 3.5. Tests are run on
Travis to ensure that.

Documentation sources
---------------------

Documentation is located in the ``doc`` directory of the repository. It is
written in `reStructuredText
<http://docutils.sourceforge.net/docs/ref/rst/restructuredtext.html>`_ and
built with `Sphinx <http://www.sphinx-doc.org/en/stable/index.html>`_.

If you modify the docs, make sure it builds without errors::

    $ cd doc/
    $ make html

The generated HTML pages should land in ``doc/_build/html``.
