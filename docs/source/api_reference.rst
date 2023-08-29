.. _api_reference:

.. attention::
    Cate has become the ESA Climate Toolbox.
    This documentation is being discontinued.
    You can find the documentation on the ESA Climate Toolbox
    `here <http://esa-climate-toolbox.readthedocs.io/>`_.

=============
API Reference
=============

Datasets
========

.. autofunction:: cate.core.find_data_store

.. autofunction:: cate.core.open_dataset


Operations
==========

Animation
---------

.. autofunction:: cate.ops.animate_map


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

.. autofunction:: cate.ops.reduce

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


Indexing
--------

.. autofunction:: cate.ops.enso

.. autofunction:: cate.ops.enso_nino34

.. autofunction:: cate.ops.oni


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

.. autofunction:: cate.ops.plot_contour

.. autofunction:: cate.ops.plot_scatter

.. autofunction:: cate.ops.plot_hist

.. autofunction:: cate.ops.plot_hovmoeller


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

.. autofunction:: cate.ops.identity

.. autofunction:: cate.ops.literal

.. autofunction:: cate.ops.pandas_fillna

.. autofunction:: cate.ops.detect_outliers


Operation Registration API
==========================

.. autoclass:: cate.core.Operation
    :members:

.. autoclass:: cate.core.OpMetaInfo
    :members:

.. autofunction:: cate.core.op
    :noindex:

.. autofunction:: cate.core.op_input

.. autofunction:: cate.core.op_output

.. autofunction:: cate.core.op_return

.. autofunction:: cate.core.new_expression_op

.. autofunction:: cate.core.new_subprocess_op


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

.. autofunction:: cate.core.new_workflow_op

.. _api-monitoring:

Task Monitoring API
===================

.. autoclass:: cate.core.Monitor
    :members:

.. autoclass:: cate.core.ConsoleMonitor
    :members:

.. autoclass:: cate.core.ChildMonitor
    :members:

