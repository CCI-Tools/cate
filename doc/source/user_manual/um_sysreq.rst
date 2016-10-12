===================
System Requirements
===================

Hardware
========

It is recommended to use an up-to-date computer, with at least 8GB of RAM and a multi-core CPU.
The most important bottlenecks will first be the data transfer rate from local data caches into the
executing program, so it is advised to use fast solid state disks. Secondly, the internet connection
speed matters, because the CCI Toolbox will frequently have to download data from remote services
in order to cache it locally.

Operating System
================

The CCI Toolbox is supposed to work on up-to-date Windows, Mac OS X, and Linux.

Python
======

The following Python environment and dependency requirements are only important for you
if you install the CCI Toolbox (Python) Core using the project's ``setup.py`` script.
See section :doc:`um_install` for more information.

The project will later provide a *self-contained installer program* or alternatively,
*a self-unpacking ZIP file*; then Python environment and dependencies will be part of the
CCI Toolbox distribution.

-----------
Environment
-----------

The ESA CCI Toolbox relies on latest Python language features and therefore requires Python 3.5+.
It is recommended to run it within a Python `Anaconda 3.5 <https://www.continuum.io/>`_
or Python `Miniconda 3.5 <http://conda.pydata.org/miniconda.html>`_ environment.

If you run it with the `standard CPython <https://www.python.org/downloads/>`_ installation,
make sure you use a 64-bit version.

------------
Dependencies
------------

The following dependencies are only important for you if you install the CCI Toolbox (Python) Core
using the project's ``setup.py`` script.

API Dependencies
----------------

API dependencies include libraries whose public API are also explicitly part of the CCI Toolbox API.

* `xarray <http://xarray.pydata.org/>`_:
  ``xarray`` (formerly ``xray``) is an open source project and Python package that aims to bring the labeled data
  power of ``pandas`` to the physical sciences, by providing N-dimensional variants of the core ``pandas`` data
  structures.
* `NumPy <http://www.numpy.org/>`_:
  ``NumPy`` is the fundamental package for scientific computing with Python.
* `Pillow <https://pillow.readthedocs.org/en/3.1.x/>`_:
  ``Pillow`` is the friendly ``PIL`` fork by Alex Clark and Contributors.
  ``PIL`` is the Python Imaging Library by Fredrik Lundh and Contributors.
* Not now: `Fiona <http://toblerity.org/fiona/>`_:
  Fiona can read and write real-world data using multi-layered GIS formats and zipped virtual file systems and
  integrates readily with other Python GIS packages such as ``pyproj``, ``Rtree``, and ``Shapely``.
* Not now: `Shapely <https://pypi.python.org/pypi/Shapely>`_:
  Manipulation and analysis of geometric objects in the Cartesian plane.


Implementation Dependencies
---------------------------

Implementation dependencies include libraries whose contents are not exposed in the CCI Toolbox API.
Such dependencies may change at any time.

* `scikit-image <http://scikit-image.org/>`_:
  ``scikit-image`` is a collection of algorithms for image processing.
* `dask <http://dask.pydata.org/>`_:
  Dask is a simple task scheduling system that uses directed acyclic graphs (DAGs) of tasks to break up large
  computations into many small ones. Optionally used by ``xarray`` and ``scikit-image`` for runtime performance
  optimisations. ``xarray`` integrates with ``dask`` to support streaming computation on datasets that don’t
  fit into memory (see http://xarray.pydata.org/en/stable/dask.html).
* `bottleneck <http://berkeleyanalytics.com/bottleneck/>`_:
  Provides fast NumPy array functions written in Cython. Optionally used by ``xarray`` for runtime performance
  optimisations, especially for ``rolling()`` operation
  (see http://xarray.pydata.org/en/stable/computation.html#rolling-window-operations).


Development Dependencies
------------------------

Development dependencies include libraries and tools which you will only need to take care of when you contribute to
the software or you plan to write plugins.


