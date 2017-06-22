Coding conventions
==================

Code style
----------

Like most Python projects, we try to adhere to
`PEP-8 <https://www.python.org/dev/peps/pep-0008/>`__ (Style Guide for
Python Code) and `PEP-257 <https://www.python.org/dev/peps/pep-0257/>`__
(Docstring Conventions) with any modifications documented here. Be sure
to read those documents if you intend to contribute code to Cate
project.

Spaces or tabs, etc
-------------------

-  According to PEP-8, we use 4 spaces for indention.
-  Lines shall be no longer than 120 characters.
-  Put a 2-line space between global classes, functions, variable
   declarations. Put a 1-line space between class methods.

Private components and properties
---------------------------------

-  According to PEP-8, we use a leading underscore to make components
   private.
-  In most cases, class instance variables should be private. Use the
   ``@property`` annotation on a getter method to export them in a
   controlled way. Think twice if you want write access.

Docstrings
----------

-  Use double quotes for docstrings ``"""``.
-  Use single docstrings ``"""bla bla bla."""`` if you have no docstring
   attributes and if the text fits into one line. Otherwise, add a line
   break after the opening ``"""`` and before the closing ``"""``.
-  Use the Sphinx-style docstring attributes, e.g. ``:param <name>:``,
   ``:return:``, etc and references, e.g.
   ``:py:class:\``\ \`\ ``or``:py:meth:\`\`\` etc. **(This is TBC, there
   is an alternative for Sphinx that is nicer for IPython, see
   `numpydoc <https://pypi.python.org/pypi/numpydoc>`__)**
-  Use the Sphinx ``#:`` syntax to document variables.

-  All modules must have a docstring that explains a module's intend,
   content, usage, and requirements that lead to its design.
-  All public (API) classes must have a docstring that explains a class'
   (single!) purpose and its constructor parameters passed to the
   ``__init__`` method.
-  All public (API) functions or methods must have a docstring that
   explains a function's or method's (single!) behaviour, parameter
   values + types and return value + type (if any).
-  All public (API) variables must have a docstring that explains a
   variable's purpose, value and type.

See also `PEP-257 <https://www.python.org/dev/peps/pep-0257/>`__ and
`PEP-258 <https://www.python.org/dev/peps/pep-0258/>`__.

Type annotations
----------------

Use type annotations when it makes sense. It makes sense, when it helps
the IDE to point you to coding errors. It makes sense to help other
people understand our code. When it makes sense, use type types from the
new Python 3.5 ``typing`` module. However, if you allow a certain
function argument to be of multiple types, don't try to construct wild
type annotation expressions, because this will reduce the readability of
the code again. In this case it is better to provide a reasonable
docstring.

TODO comments
-------------

Feel free to use TODO comments in the code on your personal branches,
but avoid them on ``master``. If you need one, use following format

::

    # TODO (forman, 20160613): bla bla bla

Ideally, TODO comments are backed by a GitHub issue providing more
background info

::

    # TODO (forman, 20160613): bla bla bla, see https://github.com/CCI-Tools/cate-core/issues/39
