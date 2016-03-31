=======================
Technical Specification
=======================

System Requirements
===================

ECT requires Python 3.5+. It is recommended to run ECT within an `Anaconda 3.5 <https://www.continuum.io/>`_
or `Miniconda 3.5 <http://conda.pydata.org/miniconda.html>`_ distributions.

Dependencies
============

The following dependencies are only important for you if you install the plain ECT library using the setup.py script.
If you install ECT using pip (conda?) these dependencies will automatically be installed. If you use the ECT installer
or ZIP file, the dependencies will already be part of the binary distribution.


API Dependencies
----------------

API dependencies include libraries whose public API are also explicitly part of the ECT API.

* `xarray <http://xarray.pydata.org/>`_:
  ``xarray`` (formerly ``xray``) is an open source project and Python package that aims to bring the labeled data
  power of ``pandas`` to the physical sciences, by providing N-dimensional variants of the core ``pandas`` data
  structures.
* `NumPy <http://www.numpy.org/>`_:
  ``NumPy`` is the fundamental package for scientific computing with Python.
* `Pillow <https://pillow.readthedocs.org/en/3.1.x/>`_:
  ``Pillow`` is the friendly ``PIL`` fork by Alex Clark and Contributors.
  ``PIL`` is the Python Imaging Library by Fredrik Lundh and Contributors.


Implementation Dependencies
---------------------------

Implementation dependencies include libraries whose contents are not exposed in the ECT API.
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
ECT or you plan to write ECT plugins.

