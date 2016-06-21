.. _Electron: http://electron.atom.io/
.. _CCI Open Data Portal: http://cci.esa.int/
.. _THREDDS: http://www.unidata.ucar.edu/software/thredds/current/tds/
.. _xarray: http://xarray.pydata.org/en/stable/
.. _Fiona: http://toblerity.org/fiona/

============
Architecture
============


Design Goals
============

TODO - copy intro sections from ITT, they still apply well.


Overview
========

The CCI Toolbox comprises a "Core" (Python) which provides the a command-line interface (CLI), application
programming interface (API), and a web API interface (WebAPI), and also implements all required climate data
visualisation, processing, and analysis functions. It defines a common climate data model and provides a common
framework to register, lookup and invoke operations and workflows on data represented in the common data model.

The CCI Toolbox graphical user interface, the GUI, is based on web technologies, i.e. JavaScript and HTML-5, and
communicates with the Python core via its WebAPI. The GUI is designed as a native desktop application (uses Electron_
technology for the desktop operating system integration). It will us a Python (RESTful) web server running on the
user's computer and providing the CCI Toolbox' WebAPI service to the GUI. This design allows for later
extensions towards a web application with possibly multiple remote WebAPI services.

The following :numref:`modules` shows the CCI Toolbox GUI, CCI Toolbox Core, and the CCI Portal. Note that although the CCI
Toolbox GUI and Core are shown as separate nodes, they are deployed as a single software installation on the user's
computer.

The ESA `CCI Open Data Portal`_ is the central climate data provider for the CCI Toolbox. It provides time series of essential
climate variables (ECVs) in various spatial and temporal resolutions in netCDF and Shapefile format. At the time of
writing (June 2016), the only operational data access service is via FTP. However, the CCI Portal will soon offer
also data access via a dedicated THREDDS_ server and will support *OPEeNDAP* and *OGC WCS* services.

.. _modules:

.. figure:: _static/uml/modules.png
   :scale: 100 %
   :align: center

   CCI Toolbox GUI, CCI Toolbox Core, and the CCI Portal.

Note that although the CCI Toolbox GUI and Core are shown in :numref:`modules` as separate nodes, they are combined in
one software installation on the user's computer.

The CCI Toolbox Core comprises four main packages of which are described in the following four sections.

.. _ect_core:

Package ``ect.core``
--------------------

The Python package ``ect.core`` is the heart of the CCI Toolbox architecture. It provides a common framework for
climate data I/O and processing and defines the user API. Although designed for climate tooling and use with climate
data the framework and API is more or less application-independent. ``ect.core`` has no dependency on the other
CCI Toolbox packages ``ect.ds``, ``ect.ops``, and ``ect.ui``.

The ``ect.core`` package

* defines the CCI Toolbox' common data model
* provides the means to read climate data and represent it in the common data model
* provides the means to process / transform data in the common data model
* to write data from the common data model to some external representation

As a framework, ``ect.core`` allows plugins to extend the CCI Toolbox capabilities. The most interesting extension
points are

* climate data stores (DS) that will be added to the global data store registry
* climate data visualisation, processing, analysis operations (OP) that will be added to the global operations registry

The modules contained in the ``ect.core`` are all essential and described in detail in the following sub-sections:

* module ``cdm`` - :ref:`cdm`
* module ``io`` - :ref:`io`
* module ``op`` - :ref:`op`
* module ``workflow`` - :ref:`workflow`

There are some utility modules included in ``ect.core`` not included in :numref:`modules` but nevertheless
they are an important part of the API:

* module ``monitor`` - :ref:`monitor`
* module ``plugin`` - :ref:`plugin`
* module ``util`` - Common utility functions

.. _ect_ds:

Package ``ect.ds``
------------------

The Python package ``ect.ds`` contains specific climate data stores (DS). Every module in this package is
dedicated to a specific data store. The ``esa_cci_ftp`` module provides the data store that represents the
ESA CCI Data Access Portal's FTP data.

The package ``ect.ds`` is a *plugin* package. The modules in ``ect.ds`` are activated during installation
and their data sources are registered once the module is imported. In fact, no module in package ``ect.core``
has any knowledge about the package ``ect.ds``.

.. _ect_ops:

Package ``ect.ops``
-------------------

The Python package ``ect.ops`` contains specific visualisation, processing and analysis functions.
Every module in this package is dedicated to a specific operation implementation.
For example the ``timeseries`` module provides an operation that can be used to extract time series from
datasets. Section :ref:`op` describes the registration, lookup, and invocation of operations,
section :ref:`workflow` describes how an operation can become part of a workflow.

Similar to ``ect.ds``, the package ``ect.ops`` is a *plugin* package, only loaded if requested, and no module in
package ``ect.core`` has any knowledge about the package ``ect.ops``.

.. _ect_ui:

Package ``ect.ui``
------------------

The package ``ect.ui`` comprises the modules ``ws`` which implements a RESTful web service that offers the WebAPI
interface for the CCI Toolbox GUI.

The ``cli`` module is described in section :ref:`cli`.


.. _Unidata's Common Data Model: http://www.unidata.ucar.edu/software/thredds/current/netcdf-java/CDM/

.. _cdm:

Common Data Model
=================

Considering the ESA CCI data products as primary source for the CCI Toolbox, a *Common Data Model* (CDM) has to be designed
for both *netCDF CF* formatted datasets as well as for the *ESRI Shapefile* format.

The most important aspect of a common data model in the context of the CCI Toolbox is the applicability of operations
to climate datasets independently of the their underlying format.

Both the netCDF CF and Shapefile format have a long-time tradition in geospatial data domain. Very good, well tested
and popular libraries exist for them in a variety of programming languages. Furthermore, for the netCDF (CF) and HDF5 datasets
there exists already the `Unidata's Common Data Model`_. Creating a new common data model which incorporates netCDF,
HDF5 and Shapefiles models would first be an enormous effort and secondly, user's of the CCI Toolbox API could be
unhappy to deal with yet another API for netCDF, HDF5, or Shapefiles.

Therefore it has been decided to make the CCI Toolbox CDM a lightweight wrapper around existing data models that exists already
for a given format. This wrapper will just make sure that (climate) operations can be performed on the different
data models. CCI Toolbox users can still decide to switch to the underlying, dedicated data model of a format or stay
with he lightweight wrapper that can peroform toolbox operations on a variety of data formats.
However, this approach burdens the CCI Toolbox developer with having to implement each operation for each the
supported data formats. But in doing so comes another advantage: the operations may be implemented very effectively
and performant with respect to a given data layout.


The CCI Toolbox CDM is implemented in the ``cdm`` module and comprises the following types:

.. _uml_cdm:

.. figure:: _static/uml/cdm.png
   :scale: 100 %
   :align: center

   Common Data Model


The ``Dataset`` interface defines the abstract operations that can be performed an all supported data formats. The
``DatasetAdapter`` is the base class for all ``Dataset`` implementations for a given data model. :numref:`uml_cdm`
shows two implementations:

* ``XarrayDatasetAdapter``: a ``Dataset`` implementation for the netCDF CD CDM provided by the excellent xarray_ Python library
* ``ShapefileDatasetAdapter``: a ``Dataset`` implementation for ESRI Shapefiles data models, e.g. as prvided by the
  Fiona_ Python library

A ``DatasetCollection`` is first a concrete collection of datasets and secondly it also implements the
``Dataset`` interface. The ``DatasetCollection`` operation implementations will usually invoke the same operation
on the children of the collection and either return a new collection or aggregate the result in some way. For example,
the *timeseries* operation would extract the time series from netCDF and Shapefiles and then combine the result
as a new instance of either one or the other type as shown in :numref:`uml_cdm_seq_2`. In general, dataset collections
delegate operations to their contained datasets and combine the individual results

.. _uml_cdm_seq_2:

.. figure:: _static/uml/cdm_seq_2.png
   :scale: 50 %
   :align: right

   Dataset collection delegation



Python implementation note: plugins may dynamically extend the ``DatasetCollection``, ``Dataset``, and
``DatasetAdapter`` types by *monkey patching* new operations into them.


.. _io:

Data Stores and Data Sources
============================

.. figure:: _static/uml/io.png
   :scale: 100 %
   :align: center

   DataStore and DataSource of the **io** module


.. figure:: _static/uml/io_file_set.png
   :scale: 100 %
   :align: center

   FileSetDataStore and FileSetDataSource of the **io** module

.. _op:

Operation Management
====================

.. figure:: _static/uml/op.png
   :scale: 100 %
   :align: center

   OpRegistry, OpRegistration, and OpMetaInfo of the **op** module

.. _workflow:

Workflow Management
===================

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


.. _monitor:

.. figure:: _static/uml/monitor.png
   :scale: 100 %
   :align: center

   Important components of the **monitor** module

.. _cli:

Command-Line Interface
======================

.. figure:: _static/uml/cli.png
   :scale: 100 %
   :align: center

   Command and Command specialisations of the **cli** module


.. _plugin:

Plugins Concept
===============

.. figure:: _static/uml/plugin.png
   :scale: 100 %
   :align: center

   The **plugin** module



