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

Operations for coregistration of datasets

Components
==========
"""
# TODO (Gailis, 20160623, Migrate this to using routines that handle downsampling better (resampling.py))

import numpy as np
import xarray as xr
from ect.core.op import op_input, op_return, op
from mpl_toolkits import basemap


@op(tags=['geometric', 'coregistration'])
@op_input('ds_slave', description='xr.Dataset that will be resampled on the masters grid')
@op_input('ds_master', description='xr.Dataset whose lat/lon coordinates are used as the resampling grid')
@op_input('method', value_set=['nearest', 'bilinear', 'cubic'], description='Interpolation method to use.')
@op_return(description='The resampled slave dataset')
def coregister(ds_master: xr.Dataset, ds_slave: xr.Dataset, method: str = 'nearest') -> xr.Dataset:
    """
    Perform coregistration of two datasets by resampling the slave dataset unto the
    grid of the master.

    :param ds_master: The master dataset of type :py:class:`xr.Dataset`
    :param ds_slave: The slave dataset of type :py:class:`xr.Dataset`
    :param method: Interpolation method to use. 'nearest','bilinear','cubic' 
    :return: The slave dataset resampled on the master's grid
    """
    methods = {'nearest': 0, 'bilinear': 1, 'cubic': 3}
    return (_resample_dataset(ds_master, ds_slave, methods[method]))


def _resample_slice(slice_, grid_lon, grid_lat, order=1):
    """
    Resample a single time slice of a larger xr.DataArray

    :param slice: xr.DataArray single slice
    :param grid_lon: meshgrid of longitudes for the new grid
    :param grid_lat: meshgrid of latitudes for the new grid
    :param order: Interpolation method 0 - nearest neighbour, 1 - bilinear (default), 3 - cubic spline
    :return: xr.DataArray, resampled slice
    """
    result = basemap.interp(slice_.values, slice_['lon'].data, slice_['lat'].data, grid_lon, grid_lat)
    return xr.DataArray(result)


def _resample_array(array, lon, lat, order=1):
    """
    Resample the given xr.DataArray to a new grid defined by grid_lat and grid_lon

    :param array: xr.DataArray with lat,lon and time coordinates
    :param lat: 'lat' xr.DataArray attribute for the new grid
    :param lon: 'lon' xr.DataArray attribute for the new grid
    :param order: 0 for nearest-neighbor interpolation, 1 for bilinear interpolation,
    3 for cubic spline (default 1). order=3 requires scipy.ndimage.
    :return: None, changes 'array' in place.
    """
    grid_lon, grid_lat = np.meshgrid(lon.data, lat.data)
    kwargs = {'grid_lon': grid_lon, 'grid_lat': grid_lat}
    temp_array = array.groupby('time').apply(_resample_slice, **kwargs)
    chunks = list(temp_array.shape[1:])
    chunks.insert(0, 1)
    return xr.DataArray(temp_array.values,
                        name=array.name,
                        dims=array.dims,
                        coords={'time': array.time, 'lat': lat, 'lon': lon},
                        attrs=array.attrs).chunk(chunks=chunks)


def _resample_dataset(ds_master, ds_slave, order=1):
    """
    Resample slave onto the grid of the master.
    This does spatial resampling the whole dataset, e.g., all
    variables in the slave dataset that have (time, lat, lon) dimensions.
    This method works only if both datasets have (time, lat, lon) dimensions. Due to
    limitations in basemap interp.

    Note that dataset attributes are not propagated due to currently undecided CDM attributes' set.

    :param ds_master: xr.Dataset whose lat/lon coordinates are used as the resampling grid
    :param ds_slave: xr.Dataset that will be resampled on the masters' grid
    :param order: Interpolation method to use. 0 - nearest neighbour, 1 - bilinear, 3 - cubic spline
    :return: xr.Dataset The resampled slave
    """
    master_keys = ds_master.dims.keys()
    slave_keys = ds_master.dims.keys()

    # It is expected that slave and master have the same dimensions
    if master_keys != slave_keys:
        return ds_slave

    lon = ds_master['lon']
    lat = ds_master['lat']

    kwargs = {'lon': lon, 'lat': lat, 'order': order}
    retset = ds_slave.apply(_resample_array, **kwargs)
    return retset
