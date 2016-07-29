"""
Description
===========

Subset operations

Components
==========
"""

import xarray as xr
from ect.core.op import op_input, op_output

@op_input('ds', description='A dataset to subset')
@op_input('lat', description='[lat_min, lat_max] to select')
@op_input('lon', description='[lon_min, lon_max] to select')
@op_output('return', description='The subset dataset')
def subset_spatial(ds:xr.Dataset, lat:list, lon:list):
    """
    Do a spatial subset of the dataset

    :param ds: Dataset to subset
    :param lat: A pair of lat_min, lat_max values
    :param lon: A pair of lon_min, lon_max values
    :return: Subset dataset
    """
    lat_slice = slice(lat[0], lat[1])
    lon_slice = slice(lon[0], lon[1])
    indexers = {'lat':lat_slice, 'lon':lon_slice}
    return ds.sel(**indexers)


@op_input('ds', description='A dataset to subset')
@op_input('time', description='[time_min, time_max] to select')
@op_output('return', description='The subset dataset')
def subset_temporal(ds:xr.Dataset, time:list):
    """
    Do a temporal subset of the dataset

    :param ds: Dataset to subset
    :param time: A pair of time_min, time_max values
    :return: Subset dataset
    """
    time_slice = slice(time[0], time[1])
    indexers = {'time':time_slice}
    return ds.sel(**indexers)


@op_input('ds', description='A dataset to subset')
@op_input('time', description='[time_index_min, time_index_max] to select')
@op_output('return', description='The subset dataset')
def subset_temporal_index(ds:xr.Dataset, time:list):
    """
    Do a temporal indices based subset

    :param ds: Dataset to subset
    :param time: A pair of time_min_index, time_max_index
    :return: Subset dataset
    """
    time_slice = slice(time[0], time[1])
    indexers = {'time':time_slice}
    return ds.isel(**indexers)
