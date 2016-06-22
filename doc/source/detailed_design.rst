.. _docstrings: https://en.wikipedia.org/wiki/Docstring
.. _verifying unit-tests: https://github.com/CCI-Tools/ect-core/tree/master/test

===============
Detailed Design
===============

This chapter provides the CCI Toolbox detailed design documentation. Its is generated from the docstrings_
that are extensively used throughout the Python code.

The documentation is generated for individual modules. Note that this modularisation reflects the effective, internal
(and physical) structure of the Python code. This is not the official *API*, which comprises a relatively stable
subset of the components, types, interfaces, and variables describes here and is described in chapter :doc:`api_reference`.

Each top level module documentation in the following sections provides a *Module Description* that provides
the module's purpose, contents, and possibly its usage. Module descriptions may link into :doc:`op_specs` for further
explanation and traceability of the detailed design. An optional sub-section *Technical Requirements* provides a
mapping from URD requirements to technical requirements and software features that drove the design of a module.
If available, links to `verifying unit-tests`_ are given.


Module ``ect.core.cdm``
=======================

Module unit-tests: `test/test_cdm.py <https://github.com/CCI-Tools/ect-core/blob/master/test/test_cdm.py>`_.

.. automodule:: ect.core.cdm
    :members:


Module ``ect.core.io``
======================

Module unit-tests: `test/test_io.py <https://github.com/CCI-Tools/ect-core/blob/master/test/test_io.py>`_.

.. automodule:: ect.core.io
    :members:

Module ``ect.core.op``
======================

Module unit-tests: `test/test_op.py <https://github.com/CCI-Tools/ect-core/blob/master/test/test_op.py>`_.

.. automodule:: ect.core.op
    :members:

Module ``ect.core.workflow``
============================

Module unit-tests: `test/test_workflow.py <https://github.com/CCI-Tools/ect-core/blob/master/test/test_workflow.py>`_.

.. automodule:: ect.core.workflow
    :members:

Module ``ect.core.plugin``
==========================

Module unit-tests: `test/test_plugin.py <https://github.com/CCI-Tools/ect-core/blob/master/test/test_plugin.py>`_.

.. automodule:: ect.core.plugin
    :members:

Module ``ect.core.monitor``
===========================

Module unit-tests: `test/test_monitor.py <https://github.com/CCI-Tools/ect-core/blob/master/test/test_monitor.py>`_.

.. automodule:: ect.core.monitor
    :members:

Module ``ect.core.util``
========================

Module unit-tests: `test/test_util.py <https://github.com/CCI-Tools/ect-core/blob/master/test/test_util.py>`_.

.. automodule:: ect.core.util
    :members:

Module ``ect.ui.cli``
=====================

Module unit-tests: `test/ui/test_cli.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ui/test_cli.py>`_.

.. automodule:: ect.ui.cli
    :members:

Module ``ect.ops``
==================

Module unit-tests:

* `test/ops/test_resample_2d.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ops/test_resample_2d.py>`_.
* `test/ops/test_downsample_2d.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ops/test_downsample_2d.py>`_.
* `test/ops/test_upsample_2d.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ops/test_upsample_2d.py>`_.
* `test/ops/test_timeseries.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ops/test_timeseries.py>`_.

.. automodule:: ect.ops
    :members:


Module ``ect.ds``
=================

Module unit-tests:

* `test/ds/test_esa_cci_ftp.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ds/test_esa_cci_ftp.py>`_.

.. automodule:: ect.ds
    :members:

