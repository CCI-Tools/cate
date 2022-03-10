.. _api_reference:

=============
API Reference
=============

Datasets
========

.. autofunction:: cate.core.find_data_sources

.. autofunction:: cate.core.open_dataset


Operations
==========

Anomaly calculation
-------------------

.. autofunction:: cate.ops.anomaly_internal

.. autofunction:: cate.ops.anomaly_external


Arithmetic
----------

.. autofunction:: cate.ops.ds_arithmetics


Averaging
---------

.. autofunction:: cate.ops.long_term_average

.. autofunction:: cate.ops.temporal_aggregation


Coregistration
--------------

.. autofunction:: cate.ops.coregister


Correlation
-----------

.. autofunction:: cate.ops.pearson_correlation_scalar

.. autofunction:: cate.ops.pearson_correlation


Data Frame
----------

.. autofunction:: cate.ops.data_frame_min

.. autofunction:: cate.ops.data_frame_max

.. autofunction:: cate.ops.data_frame_query


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

.. autofunction:: cate.ops.read_csv

.. autofunction:: cate.ops.read_geo_data_frame

.. autofunction:: cate.ops.read_netcdf

.. autofunction:: cate.ops.write_netcdf3

.. autofunction:: cate.ops.write_netcdf4


Data visualization
------------------

.. autofunction:: cate.ops.plot_map

.. autofunction:: cate.ops.plot

.. autofunction:: cate.ops.plot_data_frame


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

.. autofunction:: cate.ops.normalize

.. autofunction:: cate.ops.sel

.. autofunction:: cate.ops.from_dataframe

.. autofunction:: cate.ops.identity

.. autofunction:: cate.ops.literal

.. autofunction:: cate.ops.pandas_fillna


Data Stores and Data Sources API
================================

.. autoclass:: cate.core.DataStore
    :members:

.. autoclass:: cate.core.DataSource
    :members:


Operation Registration API
==========================

.. autoclass:: cate.core.Operation
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

.. autoclass:: cate.core.ExpressionStep
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

.. _api-monitoring:

Task Monitoring API
===================

.. autoclass:: cate.core.Monitor
    :members:

.. autoclass:: cate.core.ConsoleMonitor
    :members:

