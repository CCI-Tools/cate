"""
Module Description
==================

Implements the `xarray`_ and netCDF `Common Data Model`_ adapter for the ECT common data model.

.. _xarray: http://xarray.pydata.org/en/stable/
.. _Common Data Model: http://www.unidata.ucar.edu/software/thredds/current/netcdf-java/CDM

Module Reference
================
"""

import xarray as xr

from .cdm import DatasetAdapter, DatasetCollection


class XArrayDatasetAdapter(DatasetAdapter):
    def __init__(self, dataset: xr.Dataset):
        super(XArrayDatasetAdapter, self).__init__(dataset)

    def subset(self, spatial_roi=None, temporal_roi=None):
        # implement me using xarray Dataset API
        return self

    def close(self):
        # implement me using xarray Dataset API
        pass


def add_xarray_dataset(container: DatasetCollection, xr_dataset: xr.Dataset):
    container.add_dataset(XArrayDatasetAdapter(xr_dataset))

DatasetCollection.add_xarray_dataset = add_xarray_dataset
