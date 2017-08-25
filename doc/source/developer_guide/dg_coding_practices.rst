.. _Miniconda: http://pythontesting.net/strategy/why-most-unit-testing-is-waste/
.. _Docs on ReadTheDocs: https://github.com/CCI-Tools/cate-core/wiki/Docs-on-ReadTheDocs
.. _Why Most Unit Testing is Waste: http://pythontesting.net/strategy/why-most-unit-testing-is-waste/

Coding practices
================

Environment
-----------

We use Python 3.6+ and exploit its features. Developers are encouraged
to use the Miniconda_ 64-bit
environment. You can create a new development environment by using the
``environment.yml`` file located in the project root, type
``conda env create --file environment.yml``. This is also used by RDT to
generate our docs, see `Docs on ReadTheDocs`_.

Don't use any platform-specific features of Python!

Testing
-------

-  Write good tests. Good tests are ones that test expected core
   behaviour. Bad tests make it hard to refactor production code towards
   better architecture. See article `Why Most Unit Testing is Waste`_.
   *Thanks, Ralf!*
-  Target at 100% code coverage to make sure we don't access inexistent
   attributes, at least for a given test context. But remember that 100%
   code coverage does not imply 100% coverage of the possible
   configuration permutations (which can be close to infinity).
   Therefore it is still the quality of tests that provide value to the
   software and that result in high code coverage.
-  Use ``pytest`` for testing, to run test with coverage type
   ``pytest --cov=cate test``

Git
---

-  Not push any code to ``master`` that isn't backed by one or more
   unit-tests.
-  Keep ``master`` unbroken, only push if all test are green.
-  Always create new branches for new experimental API or API revisions.
   Don't do that on ``master``. Merge when branch is ready and reviewed
   and accepted by the team. Then delete your (remote) branch.
-  When working in the official repository, there is a guideline for branch
   names. Use ``issuenr-initials-description``.
