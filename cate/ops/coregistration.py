# The MIT License (MIT)
# Copyright (c) 2016, 2017 by the ESA CCI Toolbox development team and contributors
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

Operations
==========

coregister - coregister two datasets that are defined on global, pixel-registered grids that are
equidistant in lat/lon coordinates.

"""
# TODO (Gailis, 20160623, Migrate this to using routines that handle downsampling better (resampling.py))

from typing import Tuple

import numpy as np
import xarray as xr

from cate.core.op import op_input, op

# This imports ops/resampling.py, instead of a name from ops.__init__
from cate.ops import resampling


@op(tags=['geometric', 'coregistration', 'geom', 'global', 'resampling'])
@op_input('method_us', value_set=['nearest', 'linear'])
@op_input('method_ds', value_set=['first', 'last', 'mean', 'mode', 'var', 'std'])
def coregister(ds_master: xr.Dataset,
               ds_slave: xr.Dataset,
               method_us: str = 'linear',
               method_ds: str = 'mean') -> xr.Dataset:
    """
    Perform coregistration of two datasets by resampling the slave dataset unto the
    grid of the master. If upsampling has to be performed, this is achieved using
    interpolation, if downsampling has to be performed, the pixels of the slave dataset
    are aggregated to form a coarser grid.

    This operation works on datasets whose spatial dimensions are defined on global,
    pixel-registered and equidistant in lat/lon coordinates grids. E.g., data points
    define the middle of a pixel and pixels have the same size across the dataset.

    This operation will resample all variables in a dataset, as the lat/lon grid is 
    defined per dataset. It works only if all variables in the dataset have (time/lat/lon)
    dimensions.

    For an overview of downsampling/upsampling methods used in this operation, please
    see https://github.com/CAB-LAB/gridtools

    Whether upsampling or downsampling has to be performed is determined automatically
    based on the relationship of the grids of the provided datasets.

    :param ds_master: The dataset whose grid is used for resampling
    :param ds_slave: The dataset that will be resampled
    :param method_us: Interpolation method to use for upsampling.
    :param method_ds: Interpolation method to use for downsampling.
    :return: The slave dataset resampled on the grid of the master
    """
    # Check if the grid is global, equidistant and pixel-registered
    # The datasets are expected to be harmonized
    lat_bounds = (-90.0, 90.0)
    lon_bounds = (-180.0, 180.0)
    lats = [ds_master['lat'].values, ds_slave['lat'].values]
    lons = [ds_master['lon'].values, ds_slave['lon'].values]

    # The raised error does not say which dataset it was that failed the check, because
    # the datasets don't have identifiers apart from contained variables.
    for lat in lats:
        if not _is_equidistant(lat, lat_bounds):
            raise ValueError('The provided dataset does not seem to be '
                             'global, equidistant and pixel-registered.')

    for lon in lons:
        if not _is_equidistant(lon, lon_bounds):
            raise ValueError('The provided dataset does not seem to be '
                             'global, equidistant and pixel-registered.')

    methods_us = {'nearest': 10, 'linear': 11}
    methods_ds = {'first': 50, 'last': 51, 'mean': 54, 'mode': 56, 'var': 57, 'std': 58}
    return _resample_dataset(ds_master, ds_slave, methods_us[method_us], methods_ds[method_ds])


def _is_equidistant(array: np.ndarray, bounds: Tuple[float, float]) -> bool:
    """
    Check if the given 1D array is equidistant within the given bounds. E.g. the
    distance between the lower boundary and the first element of the array should 
    be equal to the distance between every element and it's neighbours, as well as
    between the last element of the array and the upper boundary.

    :param array: The array that should be equidistant
    :param bounds: The bounds within the array should be equidistant.
    """
    # There should be half a pixel difference between the lower boundary and the pixel
    # center.
    step = (bounds[0] - array[0]) * 2
    for i in range(0, len(array)):
        if i == len(array) - 1:
            curr_step = (array[i] - bounds[1]) * 2
        else:
            curr_step = array[i] - array[i + 1]

        if curr_step != step:
            return False

    return True


def _resample_slice(arr_slice: xr.DataArray, w: int, h: int, ds_method: int, us_method: int) -> xr.DataArray:
    """
    Resample a single time slice of a larger xr.DataArray

    :param arr_slice: xr.DataArray single slice
    :param w: The desired new width (amount of longitudes)
    :param h: The desired new height (amount of latitudes)
    :param ds_method: Downsampling method, see resampling.py
    :param us_method: Upsampling method, see resampling.py
    :return: resampled slice
    """
    result = resampling.resample_2d(arr_slice.values, w, h, ds_method, us_method)
    return xr.DataArray(result)


def _resample_array(array: xr.DataArray, lon: xr.DataArray, lat: xr.DataArray, method_us: int,
                    method_ds: int) -> xr.DataArray:
    """
    Resample the given xr.DataArray to a new grid defined by lat and lon

    :param array: xr.DataArray with lat,lon and time coordinates
    :param lat: 'lat' xr.DataArray attribute for the new grid
    :param lon: 'lon' xr.DataArray attribute for the new grid
    :param method_us: Interpolation method to use for upsampling, see resampling.py
    :param method_ds: Interpolation method to use for downsampling, see resampling.py
    :return: The resampled array
    """
    # Determine width and height of the resampled array
    width = lon.values.size
    height = lat.values.size

    kwargs = {'w': width, 'h': height, 'ds_method': method_ds, 'us_method': method_us}
    temp_array = array.groupby('time').apply(_resample_slice, **kwargs)
    chunks = list(temp_array.shape[1:])
    chunks.insert(0, 1)
    return xr.DataArray(temp_array.values,
                        name=array.name,
                        dims=array.dims,
                        coords={'time': array.time, 'lat': lat, 'lon': lon},
                        attrs=array.attrs).chunk(chunks=chunks)


def _resample_dataset(ds_master: xr.Dataset, ds_slave: xr.Dataset, method_us: int, method_ds: int) -> xr.Dataset:
    """
    Resample slave onto the grid of the master.
    This does spatial resampling the whole dataset, e.g., all
    variables in the slave dataset.
    This method works only if both datasets have (time, lat, lon) dimensions. 

    Note that dataset attributes are not propagated due to currently undecided CDM attributes' set.

    :param ds_master: xr.Dataset whose lat/lon coordinates are used as the resampling grid
    :param ds_slave: xr.Dataset that will be resampled on the masters' grid
    :param method_us: Interpolation method for upsampling, see resampling.py
    :param method_ds: Interpolation method for downsampling, see resampling.py
    :return: xr.Dataset The resampled slave dataset
    """
    # master_keys = ds_master.dims.keys()
    # slave_keys = ds_master.dims.keys()

    lon = ds_master['lon']
    lat = ds_master['lat']

    kwargs = {'lon': lon, 'lat': lat, 'method_us': method_us, 'method_ds': method_ds}
    return ds_slave.apply(_resample_array, **kwargs)
