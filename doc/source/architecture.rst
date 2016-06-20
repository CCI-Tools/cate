============
Architecture
============

Design Goals
============


Module Breakdown
================


Common Data Model (**cdm** Module)
==================================


.. figure:: _static/uml/cdm.png
   :scale: 50 %
   :align: center

   DatasetCollection, Dataset, and DatasetAdapter of the **cdm** module

.. figure:: _static/uml/cdm_seq_2.png
   :scale: 50 %
   :align: right

   Dataset collections delegate processing requests to their datasets

Data Stores and Data Sources (**io** Module)
============================================

.. figure:: _static/uml/io.png
   :scale: 100 %
   :align: center

   DataStore and DataSource of the **io** module


.. figure:: _static/uml/io_file_set.png
   :scale: 100 %
   :align: center

   FileSetDataStore and FileSetDataSource of the **io** module

Operations and Processor Management (**op** Module)
===================================================

.. figure:: _static/uml/op.png
   :scale: 100 %
   :align: center

   OpRegistry, OpRegistration, and OpMetaInfo of the **op** module

.. figure:: _static/uml/monitor.png
   :scale: 100 %
   :align: center

   Important components of the **monitor** module

Workflow Management (**workflow** Module)
=========================================

.. figure:: _static/uml/workflow.png
   :scale: 100 %
   :align: center

   Workflow, Node, Step, and Step specialisations of the **workflow** module

.. figure:: _static/uml/workflow_node_connector.png
   :scale: 100 %
   :align: center

   NodeConnector of the **workflow** module

.. figure:: _static/uml/workflow_seq.png
   :scale: 100 %
   :align: center

   By invoking a Workflow, the contained steps are invoked


Data Presentation (**present** Module)
======================================

TODO - nothing here so far

Command-Line Interface (**cli** Module)
=======================================

.. figure:: _static/uml/cli.png
   :scale: 100 %
   :align: center

   Command and Command specialisations of the **cli** module

Plugins Concept (**plugin** Module)
===================================

.. figure:: _static/uml/plugin.png
   :scale: 100 %
   :align: center

   The **plugin** module
