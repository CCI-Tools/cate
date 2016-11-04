=============
API Reference
=============

Datasets
========

.. autofunction:: cate.query_data_sources

.. autofunction:: cate.open_dataset


Operations
==========

Coregistration
--------------

.. autofunction:: cate.ops.coregister

Resampling
----------

.. autofunction:: cate.ops.resample_2d

.. autofunction:: cate.ops.downsample_2d

.. autofunction:: cate.ops.upsample_2d


Subsetting
----------

.. autofunction:: cate.ops.select_var

.. autofunction:: cate.ops.subset_spatial

.. autofunction:: cate.ops.subset_temporal

.. autofunction:: cate.ops.subset_temporal_index

Correlation
-----------

.. autofunction:: cate.ops.pearson_correlation

Timeseries
----------

.. autofunction:: cate.ops.tseries_point

.. autofunction:: cate.ops.tseries_mean


Misc
----

.. autofunction:: cate.ops.harmonize

.. autofunction:: cate.ops.sel


Data Stores and Data Sources API
================================

.. autoclass:: cate.DataStore
    :members:

.. autoclass:: cate.DataSource
    :members:


Operation Registration API
==========================

.. autoclass:: cate.OpRegistration
    :members:

.. autoclass:: cate.OpMetaInfo
    :members:

.. autofunction:: cate.op

.. autofunction:: cate.op_input

.. autofunction:: cate.op_output

.. autofunction:: cate.op_return


Workflow API
============

.. autoclass:: cate.Workflow
    :members:

.. autoclass:: cate.OpStep
    :members:

.. autoclass:: cate.NoOpStep
    :members:

.. autoclass:: cate.ExprStep
    :members:

.. autoclass:: cate.SubProcessStep
    :members:

.. autoclass:: cate.WorkflowStep
    :members:

.. autoclass:: cate.Step
    :members:

.. autoclass:: cate.Node
    :members:

.. autoclass:: cate.NodePort
    :members:

Task Monitoring API
===================

.. autoclass:: cate.Monitor
    :members:

.. autoclass:: cate.ConsoleMonitor
    :members:

