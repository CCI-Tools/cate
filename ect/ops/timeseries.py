"""
Description
===========

Simple time-series extraction operation.

Components
==========
"""

import xarray as xr

from ect.core.op import op_input, op_output


@op_input('lat', value_range=[-90, 90])
@op_input('lon', value_range=[-180, 180])
@op_input('method', value_set=['nearest', 'ffill', 'bfill', None])
@op_output('return', description='A timeseries dataset.')
def timeseries(dataset: xr.Dataset, lat: float, lon: float, method: str='nearest') -> xr.Dataset:
    """
    Extract time-series from *ds* at given *lat*, *lon* position using interpolation *method*.

    :param ds: The dataset of type :py:class:`xarray.Dataset`.
    :param lat: The latitude in the range of -90 to 90 degrees.
    :param lon: The longitude in the range of -180 to 180 degrees.
    :param method: One of ``nearest``, ``ffill``, ``bfill``.
    :return:
    """
    wrapped_xarray = dataset
    xarray_timeseries = _xarray_timeseries(wrapped_xarray, lat=lat, lon=lon, method=method)
    return xarray_timeseries


@op_input('ds', description='A dataset from which to extract time series')
@op_output('return', description='A timeseries dataset')
def timeseries_mean(ds: xr.Dataset):
    """
    Extract spatial mean timeseries from the given dataset

    :param ds: The dataset of type :py:class:`Dataset`
    :return: Time series dataset
    """
    reduce_along = {'dim':['lat','lon']}
    retset = ds.mean(**reduce_along)
    return retset


def _xarray_timeseries(dataset: xr.Dataset, lat: float, lon: float, method: str) -> xr.Dataset:
    lat_dim = _get_lat_dim_name(dataset)
    lon_dim = _get_lon_dim_name(dataset)
    indexers = {lat_dim: lat, lon_dim: lon}
    return dataset.sel(method=method, **indexers)


def _get_lon_dim_name(xarray: xr.Dataset) -> str:
    return _get_dim_name(xarray, ['lon', 'longitude', 'long'])


def _get_lat_dim_name(xarray: xr.Dataset) -> str:
    return _get_dim_name(xarray, ['lat', 'latitude'])


def _get_dim_name(xarray: xr.Dataset, possible_names) -> str:
    for name in possible_names:
        if name in xarray.dims:
            return name
    return None
