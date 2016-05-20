"""
Module Description
==================

This module provides classes and interfaces used to harmonise the access to and operations on various
types of climate datasets, for example gridded data stored in `netCDF`_ files and vector data originating from
`ESRI Shapefiles`_.

The goal of the ECT is to reuse existing, and well-known APIs for a given data type to a maximum extend
instead of creating a complex new API. The ECT's common data model is therefore designed as a thin
wrapper around the `xarray` N-D Gridded Datasets Python API that represents nicely netCDF, HDF-5 and OPeNDAP
data types, i.e. Unidata's `Common Data Model`_.

The ECT common data model exposes three important classes:

1. :py:class:`ect.core.cdm.Dataset` - an abstract interface describing the common ECT dataset API
2. :py:class:`ect.core.cdm.DatasetAdapter` - wraps an existing dataset and adapts it to the common ``Dataset`` interface
3. :py:class:`ect.core.cdm.DatasetCollection` - a collection of ``Dataset`` objects and at the same time compatible with the common ``Dataset`` interface

.. _xarray: http://xarray.pydata.org/en/stable/
.. _ESRI Shapefile: https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf
.. _netCDF: http://www.unidata.ucar.edu/software/netcdf/docs/
.. _Common Data Model: http://www.unidata.ucar.edu/software/thredds/current/netcdf-java/CDM

Module Reference
================
"""

from abc import ABCMeta, abstractmethod


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
        pass

    @abstractmethod
    def close(self):
        """
        Closes data access.
        """
        pass


class DatasetAdapter(Dataset, metaclass=ABCMeta):
    """
    An abstract base class that wraps an existing dataset or data structure and adapts it to the common
    :py:class:`ect.core.cdm.Dataset` interface.

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
    A collection of :py:class:`ect.core.cdm.Dataset`-like objects.
    """

    def __init__(self):
        self._datasets = []

    @property
    def wrapped_datasets(self):
        """
        :return: A sequence of all wrapped datasets / data structures in the order they have been added.
        """
        return [ds.wrapped_dataset for ds in self._datasets]

    @property
    def datasets(self):
        """
        :return: A sequence of all :py:class:`ect.core.cdm.Dataset` objects
                 in this collection in the order they have been added.
        """
        return list(self._datasets)

    def add_dataset(self, dataset):
        """
        Add a new dataset to this collection.
        :param dataset: a :py:class:`ect.core.cdm.Dataset`-like object
        """
        self._datasets.append(dataset)

    def remove_dataset(self, dataset):
        """
        Removed the given dataset from this collection.
        :param dataset: The dataset to be removed. This may also be the original, wrapped dataset.
        :return: The :py:class:`ect.core.cdm.Dataset` that has been removed.
        """
        for ds in self._datasets:
            if ds.wrapped_dataset is dataset:
                self._datasets.remove(ds)
                return ds
        if dataset in self._datasets:
            self._datasets.remove(dataset)
        return None

    def subset(self, spatial_roi=None, temporal_roi=None):
        """
        Calls the :py:method:`ect.core.cdm.Dataset.subset` method on all datasets and return the result as
        a new dataset collection.

        :param spatial_roi: A spatial region of interest
        :param temporal_roi: A temporal region of interest
        :return: a new dataset collection.
        """
        dsc = DatasetCollection()
        for dataset in self._datasets:
            dsc.add_dataset(dataset.subset(spatial_roi=spatial_roi, temporal_roi=temporal_roi))
        return dsc

    def close(self):
        """
        Closes all datasets.
        """
        for dataset in self._datasets:
            dataset.close()
