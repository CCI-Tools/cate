# The MIT License (MIT)
# Copyright (c) 2016 by the ECT Development Team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
Description
===========

Simple time-series extraction operation.

Components
==========
"""

import xarray as xr

from ect.core.op import op_input, op_return, op


@op(tags=['timeseries', 'temporal', 'point'])
@op_input('ds')
@op_input('lat', value_range=[-90, 90])
@op_input('lon', value_range=[-180, 180])
@op_input('method', value_set=['nearest', 'ffill', 'bfill', None])
@op_return(description='A timeseries dataset.')
def timeseries(ds: xr.Dataset, lat: float, lon: float, method: str = 'nearest') -> xr.Dataset:
    """
    Extract time-series from *ds* at given *lat*, *lon* position using interpolation *method*.

    :param ds: The dataset of type :py:class:`xarray.Dataset`.
    :param lat: The latitude in the range of -90 to 90 degrees.
    :param lon: The longitude in the range of -180 to 180 degrees.
    :param method: One of ``nearest``, ``ffill``, ``bfill``.
    :return: A timeseries dataset
    """
    if len(ds.dims) != 3 or not ('time' in ds.dims):
        raise ValueError('The timeseries operation is implemented only for\
                a three dimensional dataset with a time dimension.')

    lat_dim = _get_lat_dim_name(ds)
    lon_dim = _get_lon_dim_name(ds)
    indexers = {lat_dim: lat, lon_dim: lon}
    return ds.sel(method=method, **indexers)


@op(tags=['timeseries', 'temporal', 'aggregate', 'mean'])
@op_input('ds', description='A dataset from which to extract time series')
@op_return(description='A timeseries dataset')
def timeseries_mean(ds: xr.Dataset):
    """
    Extract spatial mean timeseries from the given dataset

    :param ds: The dataset of type :py:class:`Dataset`
    :return: Time series dataset
    """
    if len(ds.dims) != 3 or not ('time' in ds.dims):
        raise ValueError('The timeseries operation is implemented only for\
                a three dimensional dataset with a time dimension.')

    # Expecting a harmonized dataset
    reduce_along = {'dim': ['lat', 'lon']}
    retset = ds.mean(**reduce_along)
    return retset

def _get_lon_dim_name(ds: xr.Dataset) -> str:
    return _get_dim_name(ds, ['lon', 'longitude', 'long', 'x'])


def _get_lat_dim_name(ds: xr.Dataset) -> str:
    return _get_dim_name(ds, ['lat', 'latitude', 'y'])


def _get_dim_name(ds: xr.Dataset, possible_names) -> str:
    for name in possible_names:
        if name in ds.dims:
            return name
    return None
