=============
API Reference
=============

Datasets
========

.. autofunction:: ect.query_data_sources

.. autofunction:: ect.open_dataset

.. autofunction:: ect.save_dataset


Operations
==========

Coregistration
--------------

.. autofunction:: ect.ops.coregister

Resampling
----------

.. autofunction:: ect.ops.resample_2d

.. autofunction:: ect.ops.downsample_2d

.. autofunction:: ect.ops.upsample_2d


Subsetting
----------

.. autofunction:: ect.ops.select_var

.. autofunction:: ect.ops.subset_spatial

.. autofunction:: ect.ops.subset_temporal

.. autofunction:: ect.ops.subset_temporal_index

Correlation
-----------

.. autofunction:: ect.ops.pearson_correlation

Timeseries
----------

.. autofunction:: ect.ops.tseries_point

.. autofunction:: ect.ops.tseries_mean


Misc
----

.. autofunction:: ect.ops.harmonize

.. autofunction:: ect.ops.sel


Data Stores and Data Sources API
================================

.. autoclass:: ect.DataStore
    :members:

.. autoclass:: ect.DataSource
    :members:


Operation Registration API
==========================

.. autoclass:: ect.OpRegistration
    :members:

.. autoclass:: ect.OpMetaInfo
    :members:

.. autofunction:: ect.op

.. autofunction:: ect.op_input

.. autofunction:: ect.op_output

.. autofunction:: ect.op_return


Workflow API
============

.. autoclass:: ect.Workflow
    :members:

.. autoclass:: ect.OpStep
    :members:

.. autoclass:: ect.NoOpStep
    :members:

.. autoclass:: ect.ExprStep
    :members:

.. autoclass:: ect.SubProcessStep
    :members:

.. autoclass:: ect.WorkflowStep
    :members:

.. autoclass:: ect.Step
    :members:

.. autoclass:: ect.Node
    :members:

.. autoclass:: ect.NodePort
    :members:

Task Monitoring API
===================

.. autoclass:: ect.Monitor
    :members:

.. autoclass:: ect.ConsoleMonitor
    :members:

