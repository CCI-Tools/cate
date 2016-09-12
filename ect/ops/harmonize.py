"""
Description
===========

Dataset harmonization operation.

Components
==========
"""

import xarray as xr

from ect.core.op import op_input

@op_input('ds_list', description='A list of datasets to harmonize')
def harmonize(datasets:list):
    """
    Harmonize the given datasets in place. E.g. change dimension names
    if they differ from expected values.

    :param datasets: A list of datasets to harmonize
    """
    for dataset in ds_list:
        _harmonize_dataset(dataset)


def _harmonize_dataset(ds:xr.Dataset):
    """
    Harmonize a single dataset

    :param Dataset: A dataset to harmonize
    """
    lat_name = _get_lat_dim_name(ds)
    lon_name = _get_lon_dim_name(ds)

    name_dict = dict()
    if lat_name:
        name_dict[lat_name] = 'lat'

    if lon_name:
        name_dict[lon_name] = 'lon'

    ds.rename(name_dict, inplace=True)

def _get_lon_dim_name(xarray: xr.Dataset) -> str:
    return _get_dim_name(xarray, ['lon', 'longitude', 'long'])


def _get_lat_dim_name(xarray: xr.Dataset) -> str:
    return _get_dim_name(xarray, ['lat', 'latitude'])


def _get_dim_name(xarray: xr.Dataset, possible_names) -> str:
    for name in possible_names:
        if name in xarray.dims:
            return name
    return None
