"""
Module Description
==================

This module provides classes and interfaces used to harmonise the access to and operations on various
types of climate datasets, for example gridded data stored in `netCDF`_ files and vector data originating from
`ESRI Shapefile`_ files.

The goal of the ECT is to reuse existing, and well-known APIs for a given data type to a maximum extend
instead of creating a complex new API. The ECT's common data model is therefore designed as a thin
wrapper around the `xarray` N-D Gridded Datasets Python API that represents nicely netCDF, HDF-5 and OPeNDAP
data types, i.e. Unidata's `Common Data Model`_.

The ECT common data model exposes three important classes:

1. :py:class:`Dataset` - an abstract interface describing the common ECT dataset API
2. :py:class:`DatasetAdapter` - wraps an existing dataset and adapts it to the common ``Dataset`` interface
3. :py:class:`DatasetCollection` - a collection of ``Dataset`` objects and at the same time compatible with the
   common ``Dataset`` interface

.. _xarray: http://xarray.pydata.org/en/stable/
.. _ESRI Shapefile: https://www.esri.com/library/whitepapers/pdfs/shapefile.pdf
.. _netCDF: http://www.unidata.ucar.edu/software/netcdf/docs/
.. _Common Data Model: http://www.unidata.ucar.edu/software/thredds/current/netcdf-java/CDM

Module Reference
================
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
    def close(self):
        """
        Closes data access.
        """

    @abstractmethod
    def filter_dataset(self, filter_=None):
        """
        Filter the dataset, by leaving only the desired variables.
        Changes the dataset in place.

        :param filter_: The ECV variables to keep
        :return: filtered dataset of the type 'Dataset'
        """
        # TODO: Really has to be figured out how this is supposed to work
        # architecturally. Our Xarray based dataset already includes many
        # DataArray variables that need to be filtered out. It's a question
        # how it is going to be with the shapefile implementation.


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

    def close(self):
        """
        Closes all datasets.
        """
        for dataset in self.datasets:
            dataset.close()
