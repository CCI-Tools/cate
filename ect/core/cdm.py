"""
Description
===========

.. _xarray: http://xarray.pydata.org/en/stable/
.. _Dask: http://dask.pydata.org/en/latest/
.. _ESRI Shapefile: https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf
.. _netCDF: http://www.unidata.ucar.edu/software/netcdf/docs/
.. _Common Data Model: http://www.unidata.ucar.edu/software/thredds/current/netcdf-java/CDM
.. _Fiona: http://toblerity.org/fiona/
.. _CCI Toolbox URD: https://www.dropbox.com/s/0bhp6uwwk6omj8k/CCITBX-URD-v1.0Rev1.pdf?dl=0

This module provides classes and interfaces used to harmonise the access to and operations on various
types of climate datasets, for example gridded data stored in `netCDF`_ files and vector data originating from
`ESRI Shapefile`_ files.

The goal of the ECT is to reuse existing, and well-known APIs for a given data type to a maximum extend
instead of creating a complex new API. The ECT's common data model is therefore designed as a thin
wrapper around the xarray_ N-D Gridded Datasets Python API that represents nicely netCDF, HDF-5 and OPeNDAP
data types, i.e. Unidata's `Common Data Model`_. For the ESRI Shapefile representation we target at
Fiona_, which reads and writes spatial data files.

The use of xarray_ allows the CCI Toolbox to access and process very large datasets without the need to load them
entirely into memory. This feature is enabled by the internal use of the Dask_ library.

The ECT common data model exposes three important classes:

1. :py:class:`Dataset` - an abstract interface describing the common ECT dataset API
2. :py:class:`DatasetAdapter` - wraps an existing dataset and adapts it to the common ``Dataset`` interface
3. :py:class:`DatasetCollection` - a collection of ``Dataset`` objects and at the same time compatible with the
   common ``Dataset`` interface


Technical Requirements
======================

**Common Data Model**

:Description: A common data model is required that abstracts from underlying (climate) data formats.
    of that abstracts design of this module is driven by the following technical requirements given in the
    `CCI Toolbox URD`_.
:URD References:
    * CCIT-UR-DM0001: a) access, b) ingest, c) display, d) process different kinds and sizes of data
    * CCIT-UR-DM0003: multi-dimensional data
    * CCIT-UR-DM0005: access all ECV data products and metadata via standard user-community interfaces, protocols, and tools
    * CCIT-UR-DM0006: access to and ingestion of ESA CCI datasets
    * CCIT-UR-DM0011: access to and ingestion of non-CCI data
    * CCIT-UR-DM0012: handle different input file formats

----

**Common Set of (Climate) Operations**

:Description: Instances of the common data model are the input for various operations used for climate data
    visualisation, processing, and analysis. Depending on the underlying data format / schema, a given
    operations may not be applicable. The API shall provide the means to chack in advance, if a given operation
    is applicable to a given common data model instance.
:URD-References:
    * CCIT-UR-LM0009 to CCIT-UR-LM0018: Geometric Adjustments/Co-registration.
    * CCIT-UR-LM0019 to CCIT-UR-LM0024: Non-geometric Adjustments.
    * CCIT-UR-LM0025 to CCIT-UR-LM0034: Filtering, Extractions, Definitions, Selections.
    * CCIT-UR-LM0035 to CCIT-UR-LM0043: Statistics and Calculations.
    * CCIT-UR-LM0044: GIS Tools.
    * CCIT-UR-LM0045 to CCIT-UR-LM0050: Evaluation and Quality Control.

----

**Handle large Data Sets**

:Description: A single variable in ECV dataset may contain tens of gigabytes of gridded data.
    The common data model must be able to "handle" data sizes by different means. For example, lazy loading
    of data into memory combined with a programming model that allows for partial processing of data subsets
    within an operation.
:URD References:
    * CCIT-UR-DM0002: handle large datasets
    * CCIT-UR-DM0003: multi-dimensional data
    * CCIT-UR-DM0004: multiple inputs

----

Verification
============

The module's unit-tests are located

* `test/ops/test_resample_2d.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ops/test_resample_2d.py>`_.
* `test/ops/test_downsample_2d.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ops/test_downsample_2d.py>`_.
* `test/ops/test_upsample_2d.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ops/test_upsample_2d.py>`_.
* `test/ops/test_timeseries.py <https://github.com/CCI-Tools/ect-core/blob/master/test/ops/test_timeseries.py>`_.

and may be executed using ``$ py.test test/ops/test_<MODULE>.py --cov=ect/ops/<MODULE>.py`` for extra code coverage
information.


Components
==========
"""

from abc import ABCMeta, abstractmethod
from collections import OrderedDict


class Dataset(metaclass=ABCMeta):
    """
    An abstract interface describing the common ECT dataset API.
    """

    @abstractmethod
    def subset(self, spatial_roi=None, temporal_roi=None):
        """
        Return a subset of the dataset.

        :param spatial_roi: The spatial region of interest
        :param temporal_roi: : The temporal region of interest
        :return: subset of the dataset as a dataset of type ``Dataset``.
        """

    @abstractmethod
    def filter(self, variable_names:list=None, regex=False, copy:bool=False):
        """
        Filter the dataset, by leaving only desired variables.

        Whether the ``filter`` method returns a view or a copy of the underlying data depends on the concrete format and
        nature of the data.

        .. _regex: https://docs.python.org/3.5/library/re.html

        :param variable_names: List of regex patterns that identify the variables to keep.
        :param regex: If ``True``, *variable_names* is expected to contain regex_ patterns.
        :param copy: If ``True``, the returned dataset will likely contain data copies of the original dataset
        :return: a new, filtered dataset of type :py:class:`Dataset`
        """

    @abstractmethod
    def close(self):
        """
        Closes data access.
        """


class DatasetAdapter(Dataset, metaclass=ABCMeta):
    """
    An abstract base class that wraps an existing dataset or data structure and adapts it to the common
    :py:class:`Dataset` interface.

    :param wrapped_dataset: The wrapped dataset / data structure
    """

    def __init__(self, wrapped_dataset: object):
        self._wrapped_dataset = wrapped_dataset

    @property
    def wrapped_dataset(self):
        """
        :return: The wrapped dataset / data structure
        """
        return self._wrapped_dataset


class DatasetCollection(Dataset):
    """
    A collection of :py:class:`Dataset`-like objects.

    :param datasets: datasets
    :param named_datasets: named datasets
    """

    def __init__(self, *datasets, **named_datasets):
        self._datasets = OrderedDict()
        for dataset in datasets:
            self.add_dataset(dataset)
        for name, dataset in named_datasets.items():
            self.add_dataset(dataset, name=name)

    @property
    def wrapped_datasets(self):
        """
        :return: A sequence of all wrapped datasets / data structures in the order they have been added.
        """
        return [ds.wrapped_dataset for ds in self._datasets.values()]

    @property
    def datasets(self):
        """
        :return: A sequence of all :py:class:`Dataset` objects
                 in this collection in the order they have been added.
        """
        return [ds for ds in self._datasets.values()]

    def add_dataset(self, dataset, name: str = None):
        """
        Add a new dataset to this collection.
        :param dataset: a :py:class:`Dataset`-like object
        :param name: an optional name
        """
        if not name:
            name = 'ds_' + hex(id(dataset))[2:]
        self._datasets[name] = dataset

    def remove_dataset(self, name_or_dataset):
        """
        Remove the given dataset from this collection.

        :param name_or_dataset: The name of the dataset, the dataset, or the wrapped dataset to be removed.
        :return: The :py:class:`Dataset` that has been removed.
        """
        for name, dataset in self._datasets.items():
            if name_or_dataset is dataset.wrapped_dataset \
                    or name_or_dataset is dataset \
                    or name_or_dataset == name:
                del self._datasets[name]
                return dataset
        return None

    def subset(self, spatial_roi=None, temporal_roi=None):
        """
        Call the :py:meth:`subset()` method on all datasets and return the result as
        a new dataset collection.

        :param spatial_roi: A spatial region of interest
        :param temporal_roi: A temporal region of interest
        :return: a new dataset collection.
        """
        dsc = DatasetCollection()
        for name, dataset in self._datasets.items():
            dsc.add_dataset(dataset.subset(spatial_roi=spatial_roi, temporal_roi=temporal_roi), name=name)
        return dsc

    def filter(self, variable_names: list = None, regex=False, copy: bool = False):
        """
        Filter the dataset in the collection, by leaving only desired variables. Return a new collection
        that contains the filtered datasets.

        Whether the ``filter`` method returns a view or a copy of the underlying data depends on the concrete format and
        nature of the data.

        .. _regex: https://docs.python.org/3.5/library/re.html

        :param variable_names: List of variable_names that identify the variables to keep
        :param regex: If ``True``, *variable_names* is expected to contain regex_ patterns.
        :param copy: If ``True``, the returned dataset will likely contain data copies of the original dataset
        :return: a new, filtered dataset of type :py:class:`Dataset`
        """

        dsc = DatasetCollection()
        for name, dataset in self._datasets.items():
            dsc.add_dataset(dataset.filter(var_names=variable_names, copy=copy))
        return dsc

    def close(self):
        """
        Closes all datasets.
        """
        for dataset in self.datasets:
            dataset.close()
