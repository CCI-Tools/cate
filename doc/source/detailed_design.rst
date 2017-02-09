.. _docstrings: https://en.wikipedia.org/wiki/Docstring
.. _verifying unit-tests: https://github.com/CCI-Tools/cate-core/tree/master/test

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

Module ``cate.core.op``
=======================

.. automodule:: cate.core.op
    :members:

Module ``cate.core.workflow``
=============================

.. automodule:: cate.core.workflow
    :members:

Module ``cate.core.plugin``
===========================

.. automodule:: cate.core.plugin
    :members:

Module ``cate.conf``
====================

.. automodule:: cate.conf
    :members:

Module ``cate.ds``
==================

.. automodule:: cate.ds
    :members:

Module ``cate.ops``
===================

.. automodule:: cate.ops
    :members:

Module ``cate.cli.main``
========================

.. automodule:: cate.cli.main
    :members:

Module ``cate.webapi``
======================

.. automodule:: cate.webapi.main
    :members:

Module ``cate.util``
====================

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

Module ``cate.util.im``
=======================

.. automodule:: cate.util.im
    :members:

Module ``cate.util.web``
========================

.. automodule:: cate.util.web
    :members:


