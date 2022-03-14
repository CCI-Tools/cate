.. _docstrings: https://en.wikipedia.org/wiki/Docstring
.. _verifying unit-tests: https://github.com/CCI-Tools/cate/tree/master/test

.. _detailed-design:

===============
Detailed Design
===============

This chapter provides the CCI Toolbox detailed design documentation. Its is generated from the docstrings_
that are extensively used throughout the Python code.

The documentation is generated for individual modules. Note that this modularisation reflects the effective, internal
(and physical) structure of the Python code. This is not the official *API*, which comprises a relatively stable
subset of the components, types, interfaces, and variables describes here and is described in chapter :doc:`api_reference`.

Each top level module documentation in the following sections provides a sub-section *Description* that provides
the module's purpose, contents, and possibly its usage. Module descriptions may link into :doc:`op_specs` for further
explanation and traceability of the detailed design. An optional sub-section *Technical Requirements* provides a
mapping from URD requirements to technical requirements and software features that drove the design of a module.
If available, links to `verifying unit-tests`_ are given in sub-sections called *Verification*. The sub-section
*Components* lists all documented, non-private components of a module, including variables, functions, and classes.


Module ``cate.core.ds``
=======================

.. automodule:: cate.core.ds
    :members:

.. _dd-cate-core-op:

Module ``cate.core.op``
=======================

.. automodule:: cate.core.op
    :members:

Module ``cate.core.workflow``
=============================

.. automodule:: cate.core.workflow
    :members:

.. _core_plugin:

Module ``cate.core.plugin``
===========================

.. automodule:: cate.core.plugin
    :members:

Package ``cate.conf``
=====================

.. automodule:: cate.conf
    :members:

Package ``cate.ops``
====================

.. automodule:: cate.ops
    :members:
    :noindex:

Module ``cate.cli.main``
========================

.. automodule:: cate.cli.main
    :members:

Package ``cate.webapi``
=======================

.. automodule:: cate.webapi
    :members:

Package ``cate.util``
=====================

.. automodule:: cate.util
    :members:

Module ``cate.util.cache``
==========================

.. automodule:: cate.util.cache
    :members:

Module ``cate.util.cli``
========================

.. automodule:: cate.util.cli
    :members:

Package ``cate.util.im``
========================

.. automodule:: cate.util.im
    :members:

Package ``cate.util.web``
=========================

.. automodule:: cate.util.web
    :members:


