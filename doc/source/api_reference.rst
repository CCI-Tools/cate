=============
API Reference
=============

Datasets
========

.. autofunction:: cate.query_data_sources

.. autofunction:: cate.open_dataset


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
---------------

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

