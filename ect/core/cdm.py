"""
This module provides classes and interfaces used to harmonise the access to and operations on various
types of climate datasets, for example gridded data stored in netCDF files and vector data originating from
ESRI Shapefiles.

The goal of the ECT is to reuse existing, and well-known APIs for a given data type to a maximum extend
instead of creating a complex new API. The ECT's common data model is therefore designed as a thin
wrapper around the `xarray` N-D Gridded Datasets Python API that represents nicely `netCDF`_, HDF-5 and OPeNDAP
data types, i.e. Unidata's `Common Data Model`_.

The ECT common data model exposes three important classes:

1. :py:class:`ect.core.cdm.Dataset` - an abstract interface describing the common ECT dataset API
2. :py:class:`ect.core.cdm.DatasetAdapter` - wraps and existing dataset and adapts it to the common `Dataset` interface
3. :py:class:`ect.core.cdm.DatasetCollection` - a collection of ``Dataset``s which itself is compatible with the
   `Dataset` interface

.. _xarray: http://xarray.pydata.org/en/stable/
.. _netCDF: http://www.unidata.ucar.edu/software/netcdf/docs/
.. _Common Data Model: http://www.unidata.ucar.edu/software/thredds/current/netcdf-java/CDM
"""

from abc import ABCMeta, abstractmethod


class Dataset(metaclass=ABCMeta):
    """
    A collection of generic operations that can act both on vector (?) and gridded raster data (xarray.Dataset).
    """

    @abstractmethod
    def subset(self, spatial_roi=None, temporal_roi=None):
        """
        Return a subset of the dataset.

        :param spatial_roi: The spatial region of interest
        :param temporal_roi: : The temporal region of interest
        :return: subset of the dataset as a dataset of type ``Dataset``.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Closes data access.
        """
        pass


class DatasetAdapter(Dataset, metaclass=ABCMeta):
    """
    An abstract base class representing a generic dataset adapter that can apply all
    **DatasetOperations** to a wrapped dataset of any type.
    """

    def __init__(self, dataset: object):
        self._dataset = dataset

    @property
    def dataset(self):
        return self._dataset


class DatasetCollection(Dataset):
    """
    A data container contains datasets of various types (vector and raster data)
    and implements a set of common operations on these datasets.
    """

    def __init__(self):
        self._datasets = []

    @property
    def datasets(self):
        return [ds.dataset for ds in self._datasets]

    def add_dataset(self, dataset):
        self._datasets.append(dataset)

    def remove_dataset(self, dataset):
        for ds in self._datasets:
            if ds.dataset is dataset:
                self._datasets.remove(ds)
                return ds
        self._datasets.remove(dataset)

    def subset(self, spatial_roi=None, temporal_roi=None):
        dsc = DatasetCollection()
        for dataset in self._datasets:
            dsc.add_dataset(dataset.subset(spatial_roi=spatial_roi, temporal_roi=temporal_roi))
        return dsc

    def close(self):
        for dataset in self._datasets:
            dataset.close()
