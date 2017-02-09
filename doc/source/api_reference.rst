=============
API Reference
=============

Datasets
========

.. autofunction:: cate.core.query_data_sources

.. autofunction:: cate.core.open_dataset


Operations
==========

Anomaly calculation
-------------------

.. autofunction:: cate.ops.anomaly_internal

.. autofunction:: cate.ops.anomaly_climatology


Arithmetic
----------

.. autofunction:: cate.ops.ds_arithmetics


Averaging
---------

.. autofunction:: cate.ops.long_term_average

.. autofunction:: cate.ops.temporal_agg


Coregistration
--------------

.. autofunction:: cate.ops.coregister


Correlation
-----------

.. autofunction:: cate.ops.pearson_correlation


Input / Output
--------------

.. autofunction:: cate.ops.open_dataset

.. autofunction:: cate.ops.save_dataset

.. autofunction:: cate.ops.read_object

.. autofunction:: cate.ops.write_object

.. autofunction:: cate.ops.read_text

.. autofunction:: cate.ops.write_text

.. autofunction:: cate.ops.read_json

.. autofunction:: cate.ops.write_json

.. autofunction:: cate.ops.read_netcdf

.. autofunction:: cate.ops.write_netcdf3

.. autofunction:: cate.ops.write_netcdf4


Data visualization
------------------

.. autofunction:: cate.ops.plot_map

.. autofunction:: cate.ops.plot_1D


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

.. autoclass:: cate.core.OpRegistration
    :members:

.. autoclass:: cate.core.OpMetaInfo
    :members:

.. autofunction:: cate.core.op

.. autofunction:: cate.core.op_input

.. autofunction:: cate.core.op_output

.. autofunction:: cate.core.op_return


Workflow API
============

.. autoclass:: cate.core.Workflow
    :members:

.. autoclass:: cate.core.OpStep
    :members:

.. autoclass:: cate.core.NoOpStep
    :members:

.. autoclass:: cate.core.ExprStep
    :members:

.. autoclass:: cate.core.SubProcessStep
    :members:

.. autoclass:: cate.core.WorkflowStep
    :members:

.. autoclass:: cate.core.Step
    :members:

.. autoclass:: cate.core.Node
    :members:

.. autoclass:: cate.core.NodePort
    :members:

Task Monitoring API
===================

.. autoclass:: cate.core.Monitor
    :members:

.. autoclass:: cate.core.ConsoleMonitor
    :members:

