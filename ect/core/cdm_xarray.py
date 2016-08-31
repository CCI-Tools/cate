"""
Description
===========

.. warning:: This module is only partially implemented and lacks essential functionality.

Implements the `xarray`_ and netCDF `Common Data Model`_ adapter for the CCI Toolbox' Common Data Model.

.. _xarray: http://xarray.pydata.org/en/stable/
.. _Common Data Model: http://www.unidata.ucar.edu/software/thredds/current/netcdf-java/CDM

Components
==========
"""

import xarray as xr

from .cdm import DatasetAdapter, DatasetCollection


class XArrayDatasetAdapter(DatasetAdapter):
    def __init__(self, dataset: xr.Dataset):
        super(XArrayDatasetAdapter, self).__init__(dataset)

    def subset(self, spatial_roi=None, temporal_roi=None):
        # implement me using xarray Dataset API
        #raise NotImplementedError()
        return self

    def filter(self, var_names:list=None, regex=False, copy:bool=False):
        if not var_names:
            return self

        dropped_var_names = list(self._wrapped_dataset.data_vars.keys())

        if not regex:
            for var_name in var_names:
                if var_name in dropped_var_names:
                    dropped_var_names.remove(var_name)
        else:
            import re
            for var_name_pattern in var_names:
                prog = re.compile(var_name_pattern)
                for dropped_var_name in list(dropped_var_names):
                    if prog.match(dropped_var_name):
                        dropped_var_names.remove(dropped_var_name)

        return XArrayDatasetAdapter(self._wrapped_dataset.drop(dropped_var_names))

    def close(self):
        if hasattr(self._wrapped_dataset, 'close'):
            self._wrapped_dataset.close()


def add_xarray_dataset(container: DatasetCollection, xr_dataset: xr.Dataset, name: str = None):
    container.add_dataset(XArrayDatasetAdapter(xr_dataset), name=name)


# Monkey-patch DatasetCollection
DatasetCollection.add_xarray_dataset = add_xarray_dataset
