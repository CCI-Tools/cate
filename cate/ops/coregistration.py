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

coregister - coregister two datasets that are defined on pixel-registered grids that are
equidistant in lat/lon coordinates.

"""
from typing import Tuple

import numpy as np
import xarray as xr
import math

from cate.core.op import op_input, op, op_return
from cate.core.types import ValidationError
from cate.util.monitor import Monitor

from cate.ops import resampling
from cate.ops.normalize import adjust_spatial_attrs


@op(tags=['geometric', 'coregistration'],
    version='1.1')
@op_input('method_us', value_set=['nearest', 'linear'])
@op_input('method_ds', value_set=['first', 'last', 'mean', 'mode', 'var', 'std'])
@op_return(add_history=True)
def coregister(ds_master: xr.Dataset,
               ds_slave: xr.Dataset,
               method_us: str = 'linear',
               method_ds: str = 'mean',
               monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Perform coregistration of two datasets by resampling the slave dataset unto the
    grid of the master. If upsampling has to be performed, this is achieved using
    interpolation, if downsampling has to be performed, the pixels of the slave dataset
    are aggregated to form a coarser grid.

    The returned dataset will contain the lat/lon intersection of provided
    master and slave datasets, resampled unto the master grid frequency.

    This operation works on datasets whose spatial dimensions are defined on
    pixel-registered and equidistant in lat/lon coordinates grids. E.g., data points
    define the middle of a pixel and pixels have the same size across the dataset.

    This operation will resample all variables in a dataset, as the lat/lon grid is
    defined per dataset. It works only if all variables in the dataset have lat
    and lon as dimensions.

    For an overview of downsampling/upsampling methods used in this operation, please
    see https://github.com/CAB-LAB/gridtools

    Whether upsampling or downsampling has to be performed is determined automatically
    based on the relationship of the grids of the provided datasets.

    :param ds_master: The dataset whose grid is used for resampling
    :param ds_slave: The dataset that will be resampled
    :param method_us: Interpolation method to use for upsampling.
    :param method_ds: Interpolation method to use for downsampling.
    :param monitor: a progress monitor.
    :return: The slave dataset resampled on the grid of the master
    """
    try:
        grids = (('slave', ds_slave['lat'].values, -90),
                 ('slave', ds_slave['lon'].values, -180),
                 ('master', ds_master['lat'].values, -90),
                 ('master', ds_master['lon'].values, -180))
    except KeyError:
        raise ValidationError('Coregistration requires that both datasets are'
                              ' spatial datasets with lon and lat dimensions. The'
                              ' dimensionality of the provided master dataset is: {},'
                              ' the dimensionality of the provided slave dataset is:'
                              ' {}. Running the normalize operation might help in'
                              ' case spatial dimensions have different'
                              ' names'.format(ds_master.dims, ds_slave.dims))

    # Check if all arrays of the slave dataset have the required dimensionality
    for key in ds_slave.data_vars:
        if not _is_valid_array(ds_slave[key]):
            raise ValidationError('{} data array of slave dataset is not valid for'
                                  ' coregistration. The data array is expected to'
                                  ' have lat and lon dimensions. The data array has'
                                  ' the following dimensions: {}. Consider running'
                                  ' select_var operation to exclude this'
                                  ' data array'.format(key, ds_slave[key].dims))

    # Check if the grids of the provided datasets are equidistant and pixel
    # registered
    for array in grids:
        if not _within_bounds(array[1], array[2]):
            raise ValidationError('The {} dataset grid does not fall into required'
                                  ' boundaries. Required boundaries are ({}, {}),'
                                  ' dataset boundaries are ({}, {}). Running the'
                                  ' normalize operation'
                                  ' may help.'.format(array[0],
                                                      array[2],
                                                      abs(array[2]),
                                                      array[1][0],
                                                      array[1][-1]))
        if not _is_equidistant(array[1]):
            raise ValidationError('The {} dataset grid is not'
                                  ' equidistant, can not perform'
                                  ' coregistration'.format(array[0]))

        if not _is_pixel_registered(array[1], array[2]):
            raise ValidationError('The {} dataset grid is not'
                                  ' pixel-registered, can not perform'
                                  ' coregistration'.format(array[0]))

    # Don't do anything if datasets already have the same spatial definition
    if np.array_equal(ds_master['lat'].values, ds_slave['lat'].values) and \
       np.array_equal(ds_master['lon'].values, ds_slave['lon'].values):
        return ds_slave

    # Co-register
    methods_us = {'nearest': 10, 'linear': 11}
    methods_ds = {'first': 50, 'last': 51, 'mean': 54, 'mode': 56, 'var': 57, 'std': 58}

    return _resample_dataset(ds_master, ds_slave, methods_us[method_us], methods_ds[method_ds], monitor)


def _is_equidistant(array: np.ndarray) -> bool:
    """
    Check if the given 1D array is equidistant. E.g. the
    distance between all elements of the array should be equal.

    :param array: The array that should be equidistant
    """
    step = abs(array[1] - array[0])
    for i in range(0, len(array) - 1):
        curr_step = abs(array[i + 1] - array[i])
        if not math.isclose(curr_step, step, rel_tol=1e-3):
            return False

    return True


def _is_pixel_registered(array: np.ndarray, origin) -> bool:
    """
    Check if the given coordinate array is pixel registered. E.g., values
    should denote the 'middle' point of a pixel.

    :param array: The array that should be pixel registered
    :param origin: The origin value for the values in the given array
    """
    step = abs(array[1] - array[0])
    return math.isclose((((array[0] - step / 2) - origin) % step), 0, abs_tol=0.1)


def _is_valid_array(array: xr.DataArray) -> bool:
    """
    Check if the provided xarray Data Array is valid for coregistration.
    Meaning, it has more than two dimensions and at least contains lat and lon.

    :param array: Array to check for validity
    :return: True if the given array is valid
    """
    return (len(array.dims) >= 2 and
            'lat' in array.dims and
            'lon' in array.dims)


def _within_bounds(array: np.ndarray, low_bound) -> bool:
    """
    Check if the given array falls into the given bounds. In cate we work with
    grids that are symmetrical around zero.

    :param array: Array to check
    :param low_bound: lower boundary
    :return: True if falls into bounds
    """
    return (array[0] >= low_bound and array[-1] <= abs(low_bound))


def _resample_slice(arr_slice: xr.DataArray, w: int, h: int, ds_method: int, us_method: int, parent_monitor: Monitor) -> xr.DataArray:
    """
    Resample a single time slice of a larger xr.DataArray

    :param arr_slice: xr.DataArray single slice
    :param w: The desired new width (amount of longitudes)
    :param h: The desired new height (amount of latitudes)
    :param ds_method: Downsampling method, see resampling.py
    :param us_method: Upsampling method, see resampling.py
    :param parent_monitor: the parent progress monitor.
    :return: resampled slice
    """
    monitor = parent_monitor.child(1)
    with monitor.observing("resample slice"):
        # In some cases the grouped dimension is not automatically squeezed out
        result = resampling.resample_2d(np.ma.masked_invalid(arr_slice.squeeze().values),
                                        w,
                                        h,
                                        ds_method,
                                        us_method)
        return xr.DataArray(result)


def _resample_array(array: xr.DataArray, lon: xr.DataArray, lat: xr.DataArray, method_us: int,
                    method_ds: int, parent_monitor: Monitor) -> xr.DataArray:
    """
    Resample the given xr.DataArray to a new grid defined by lat and lon

    :param array: xr.DataArray with lat,lon and time coordinates
    :param lat: 'lat' xr.DataArray attribute for the new grid
    :param lon: 'lon' xr.DataArray attribute for the new grid
    :param method_us: Interpolation method to use for upsampling, see resampling.py
    :param method_ds: Interpolation method to use for downsampling, see resampling.py
    :param parent_monitor: the parent progress monitor.
    :return: The resampled array
    """
    # Determine width and height of the resampled array
    width = lon.values.size
    height = lat.values.size

    monitor = parent_monitor.child(1)

    kwargs = {'w': width, 'h': height, 'ds_method': method_ds, 'us_method': method_us, 'parent_monitor': monitor}

    groupby_list = list(array.dims)
    for dim in ['lon', 'lat']:
        groupby_list.remove(dim)

    if 0 == len(groupby_list):
        # a 2d dataset, can't do groupby => do a simple slice resample
        with monitor.starting("coregister dataarray", total_work=1):
            temp_array = _resample_slice(array, **kwargs)
            coords = {'lat': lat, 'lon': lon}
            return xr.DataArray(temp_array.values,
                                name=array.name,
                                dims=array.dims,
                                coords=coords,
                                attrs=array.attrs).chunk()

    num_steps = 1
    for dim in groupby_list:
        num_steps = num_steps * len(array[dim])

    with monitor.starting("coregister dataarray", total_work=num_steps):
        temp_array = _nested_groupby_apply(array, groupby_list, _resample_slice, kwargs)
        chunks = {'lat': height, 'lon': width}
        coords = {'lat': lat, 'lon': lon}
        for dim in groupby_list:
            coords[dim] = array[dim]
            # One spatial slice is one dask chunk, e.g. chunking is
            # (1,1,1..1,len(lat),len(lon))
            chunks[dim] = 1
        return xr.DataArray(temp_array.values,
                            name=array.name,
                            dims=array.dims,
                            coords=coords,
                            attrs=array.attrs).chunk(chunks=chunks)


def _resample_dataset(ds_master: xr.Dataset, ds_slave: xr.Dataset, method_us: int, method_ds: int, monitor: Monitor) -> xr.Dataset:
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
    :param monitor: a progress monitor.
    :return: xr.Dataset The resampled slave dataset
    """
    # Find lat/lon bounds of the intersection of master and slave grids. The
    # bounds should fall on pixel boundaries for both spatial dimensions for
    # both datasets
    lat_min, lat_max = _find_intersection(ds_master['lat'].values,
                                          ds_slave['lat'].values,
                                          global_bounds=(-90, 90))
    lon_min, lon_max = _find_intersection(ds_master['lon'].values,
                                          ds_slave['lon'].values,
                                          global_bounds=(-180, 180))

    # Subset slave dataset and master grid. We're not using here the subset
    # operation, because the subset operation may produce datasets that cross
    # the anti-meridian by design. However, such a disjoint dataset can not be
    # resampled using our current resampling methods.
    lat_slice = slice(lat_min, lat_max)
    lon_slice = slice(lon_min, lon_max)

    lon = ds_master['lon'].sel(lon=lon_slice)
    lat = ds_master['lat'].sel(lat=lat_slice)
    ds_slave = ds_slave.sel(lon=lon_slice, lat=lat_slice)

    if np.array_equal(lon.values, ds_slave['lon'].values) and \
       np.array_equal(lat.values, ds_slave['lat'].values):
        # The original grids already have the same spatial definition
        return ds_slave

    with monitor.starting("coregister dataset", len(ds_slave.data_vars)):
        kwargs = {'lon': lon, 'lat': lat, 'method_us': method_us, 'method_ds': method_ds, 'parent_monitor': monitor}
        retset = ds_slave.apply(_resample_array, keep_attrs=True, **kwargs)

    return adjust_spatial_attrs(retset)


def _find_intersection(first: np.ndarray,
                       second: np.ndarray,
                       global_bounds: Tuple[float, float]) -> Tuple[float, float]:
    """
    Find 1D intersection of given arrays such that the resulting intersection
    bounds fall on 'pixel' boundaries for both given arrays.

    :param first: First 1D array
    :param second: Second 1D array
    :param global_bounds: (min, max) maximum interval for a valid intersection
    :return: (min, max) intersection bounds
    """
    first_px_size = abs(first[1] - first[0])
    second_px_size = abs(second[1] - second[0])

    minimum = max(first[0] - first_px_size / 2,
                  second[0] - second_px_size / 2)
    maximum = min(first[-1] + first_px_size / 2,
                  second[-1] + second_px_size / 2)

    delta = maximum - minimum
    if delta < max(first_px_size, second_px_size):
        raise ValidationError('Could not find a valid intersection to perform'
                              ' coregistration on')

    # Make sure min/max fall on pixel boundaries for both grids
    # Because there exists a number N denoting how many smaller pixels fall
    # into one larger pixel (for pixel registered datasets with the same
    # origin) => the boundary has to be adjusted by steps equal
    # to smaller pixels.
    finer = min(first_px_size, second_px_size)
    safety = 100
    i = 0
    while (not math.isclose(((minimum - global_bounds[0]) % first_px_size), 0, abs_tol=0.1) and
           not math.isclose(((minimum - global_bounds[0]) % second_px_size), 0, abs_tol=0.1)):
        if i == safety:
            raise ValidationError('Could not find a valid intersection to perform'
                                  ' coregistration on')
        minimum = minimum + finer
        i = i + 1

    i = 0
    while (not math.isclose(((global_bounds[1] - maximum) % first_px_size), 0, abs_tol=0.1) and
           not math.isclose(((global_bounds[1] - maximum) % second_px_size), 0, abs_tol=0.1)):
        if i == safety:
            raise ValidationError('Could not find a valid intersection to perform'
                                  ' coregistration on')
        maximum = maximum - finer
        i = i + 1

    # This is possible in some cases when mis-aligned grid arrays are presented
    if maximum <= minimum:
        raise ValidationError('Could not find a valid intersection to perform'
                              ' coregistration on')

    return (minimum, maximum)


def _nested_groupby_apply(array: xr.DataArray,
                          groupby: list,
                          apply_fn: object,
                          kwargs: dict):
    """
    Perform a nested groupby over given dimensions and apply a function on the
    last 'slice'

    :param array: xr.DataArray to perform groupby on
    :param groupby: a list of coordinate labels over which to perform groupby
    :param apply_fn: The function to apply
    :return: groupby-split-appy result
    """
    if len(groupby) == 1:
        return array.groupby(groupby[0], squeeze=True).apply(apply_fn, **kwargs)
    else:
        return array.groupby(groupby[0], squeeze=True).apply(_nested_groupby_apply,
                                                             groupby=groupby[1:],
                                                             apply_fn=apply_fn,
                                                             kwargs=kwargs)
