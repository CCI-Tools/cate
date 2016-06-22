=============
API Reference
=============

.. warning:: The CCI Toolbox API has not yet been been released. The functions and classes given here
    are a subset of the CCI Toolbox components that will likely become official API in the future.


Data Stores and Data Sources
============================

.. autofunction:: ect.query_data_sources

.. autofunction:: ect.open_dataset

.. autoclass:: ect.DataStore
    :members:

.. autoclass:: ect.FileSetDataStore
    :members:

.. autoclass:: ect.DataSource
    :members:

.. autoclass:: ect.FileSetDataSource
    :members:

.. autoclass:: ect.FileSetDataSource
    :members:

.. autoclass:: ect.FileSetInfo
    :members:

Common Data Model
=================

.. autoclass:: ect.Dataset
    :members:


.. autoclass:: ect.DatasetCollection
    :members:


Operations
==========

.. autoclass:: ect.OpRegistration
    :members:

.. autoclass:: ect.OpMetaInfo
    :members:

.. autofunction:: ect.op

.. autofunction:: ect.op_input

.. autofunction:: ect.op_output

.. autofunction:: ect.op_return


Workflows
=========

.. autoclass:: ect.Node
    :members:

.. autoclass:: ect.Workflow
    :members:

.. autoclass:: ect.Step
    :members:

.. autoclass:: ect.OpStep
    :members:

.. autoclass:: ect.ExprStep
    :members:

.. autoclass:: ect.WorkflowStep
    :members:

Task Monitoring
===============

.. autoclass:: ect.Monitor
    :members:

.. autoclass:: ect.ConsoleMonitor
    :members:

