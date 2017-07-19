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

Dataset normalization operation.

Components
==========
"""

import xarray as xr
import numpy as np

from jdcal import jd2gcal
from datetime import datetime

from cate.core.op import op
from cate.core.cdm import get_lon_dim_name, get_lat_dim_name


@op(tags=['utility'])
def normalize(ds: xr.Dataset) -> xr.Dataset:
    """
    Normalize the geo- and time-coding upon opening the given dataset w.r.t.
    to a common (CF-compatible) convention used within Cate. This will maximize the compatibility of
    a dataset for usage with Cate's operations.

    That is,
    * variables named "latitude" will be renamed to "lat";
    * variables named "longitude" or "long" will be renamed to "lon";

    Then, for equi-rectangular grids,
    * Remove 2D "lat" and "lon" variables;
    * Two new 1D coordinate variables "lat" and "lon" will be generated from original 2D forms.

    Finally, it will be ensured that a "time" coordinate variable will be of type *datetime*.

    :param ds: The dataset to normalize.
    :return: The normalized dataset, or the original dataset, if it is already "normal".
    """

    ds = _normalize_lat_lon(ds)
    ds = _normalize_lat_lon_2d(ds)

    # Handle Julian Day Time
    try:
        if ds.time.long_name.lower().strip() == 'time in julian days':
            return _normalize_jd2datetime(ds)
    except AttributeError:
        pass

    return ds


def _normalize_lat_lon(ds: xr.Dataset) -> xr.Dataset:
    """
    Rename variables named 'longitude' or 'long' to 'lon', and 'latitude' to 'lon'.
    :param ds: some xarray dataset
    :return: a normalized xarray dataset, or the original one
    """
    lat_name = get_lat_dim_name(ds)
    lon_name = get_lon_dim_name(ds)

    name_dict = dict()
    if lat_name and 'lat' not in ds:
        name_dict[lat_name] = 'lat'

    if lon_name and 'lon' not in ds:
        name_dict[lon_name] = 'lon'

    if name_dict:
        ds = ds.rename(name_dict)

    return ds


def _normalize_lat_lon_2d(ds: xr.Dataset) -> xr.Dataset:
    """
    Detect 2D 'lat', 'lon' variables that span a equi-rectangular grid. Then:
    Drop original 'lat', 'lon' variables
    Rename original dimensions names of 'lat', 'lon' variables, usually ('y', 'x'), to ('lat', 'lon').
    Insert new 1D 'lat', 'lon' coordinate variables with dimensions 'lat' and 'lon', respectively.
    :param ds: some xarray dataset
    :return: a normalized xarray dataset, or the original one
    """
    if not ('lat' in ds and 'lon' in ds):
        return ds

    lat_var = ds['lat']
    lon_var = ds['lon']

    lat_dims = lat_var.dims
    lon_dims = lon_var.dims
    if lat_dims != lon_dims:
        return ds

    spatial_dims = lon_dims
    if len(spatial_dims) != 2:
        return ds

    x_dim_name = spatial_dims[-1]
    y_dim_name = spatial_dims[-2]

    lat_data_1 = lat_var[:, 0]
    lat_data_2 = lat_var[:, -1]
    lon_data_1 = lon_var[0, :]
    lon_data_2 = lon_var[-1, :]

    equal_lat = np.allclose(lat_data_1, lat_data_2, equal_nan=True)
    equal_lon = np.allclose(lon_data_1, lon_data_2, equal_nan=True)
    if not (equal_lat and equal_lon):
        return ds

    ds = ds.drop(['lon', 'lat'])

    ds = ds.rename({
        x_dim_name: 'lon',
        y_dim_name: 'lat',
    })

    ds = ds.assign_coords(lon=np.array(lon_data_1), lat=np.array(lat_data_1))

    return ds


def _normalize_jd2datetime(ds: xr.Dataset) -> xr.Dataset:
    """
    Convert the time dimension of the given dataset from Julian date to
    datetime.

    :param ds: Dataset on which to run conversion
    """
    ds = ds.copy()
    # Decode JD time
    tuples = [jd2gcal(x, 0) for x in ds.time.values]
    # Replace JD time with datetime
    ds.time.values = [datetime(x[0], x[1], x[2]) for x in tuples]
    # Adjust attributes
    ds.time.attrs['long_name'] = 'time'
    ds.time.attrs['calendar'] = 'standard'

    return ds


def adjust_spatial_attrs(ds: xr.Dataset) -> xr.Dataset:
    """
    Adjust the global spatial attributes of the dataset by doing some
    introspection of the dataset and adjusting the appropriate attributes
    accordingly.

    In case the determined attributes do not exist in the dataset, these will
    be added.

    For more information on suggested global attributes see
    `Attribute Convention for Data Discovery <http://wiki.esipfed.org/index.php/Attribute_Convention_for_Data_Discovery>`_

    :param ds: Dataset to adjust
    :return: Adjusted dataset
    """
    ds = ds.copy()

    for dim in ('lon', 'lat'):
        geoattrs = _get_spatial_props(ds, dim)

        for key in geoattrs:
            if geoattrs[key]:
                ds.attrs[key] = geoattrs[key]

    lon_min = ds.attrs['geospatial_lon_min']
    lat_min = ds.attrs['geospatial_lat_min']
    lon_max = ds.attrs['geospatial_lon_max']
    lat_max = ds.attrs['geospatial_lat_max']

    ds.attrs['geospatial_bounds'] = 'POLYGON(({} {}, {} {}, {} {},\
 {} {}, {} {}))'.format(lon_min, lat_min, lon_min, lat_max, lon_max, lat_max,
                        lon_max, lat_min, lon_min, lat_min)

    # Determination of the following attributes from introspection in a general
    # way is ambiguous, hence it is safer to drop them than to risk preserving
    # out of date attributes.
    drop = ['geospatial_bounds_crs', 'geospatial_bounds_vertical_crs',
            'geospatial_vertical_min', 'geospatial_vertical_max',
            'geospatial_vertical_positive', 'geospatial_vertical_units',
            'geospatial_vertical_resolution']

    for key in drop:
        ds.attrs.pop(key, None)

    return ds


def adjust_temporal_attrs(ds: xr.Dataset) -> xr.Dataset:
    """
    Adjust the global temporal attributes of the dataset by doing some
    introspection of the dataset and adjusting the appropriate attributes
    accordingly.

    In case the determined attributes do not exist in the dataset, these will
    be added.

    For more information on suggested global attributes see
    `Attribute Convention for Data Discovery <http://wiki.esipfed.org/index.php/Attribute_Convention_for_Data_Discovery>`_

    :param ds: Dataset to adjust
    :return: Adjusted dataset
    """
    ds = ds.copy()

    ds.attrs['time_coverage_start'] = str(ds.time.values[0])
    ds.attrs['time_coverage_end'] = str(ds.time.values[-1])
    ds.attrs['time_coverage_resolution'] = _get_temporal_res(ds.time.values)
    ds.attrs['time_coverage_duration'] = _get_duration(ds.time.values)

    return ds


def _get_spatial_props(ds: xr.Dataset, dim: str) -> tuple:
    """
    Get spatial boundaries, resolution and units of the given dimension of the given
    dataset. If the 'bounds' are explicitly defined, these will be used for
    boundary calculation, otherwise it will rest purely on information gathered
    from 'dim' itself.

    :param ds: Dataset
    :param dim: Dimension name
    :return: A dictionary {'attr_name': attr_value}
    """
    ret = dict()

    try:
        dim_res = abs(ds[dim].values[1] - ds[dim].values[0])
        res_name = 'geospatial_{}_resolution'.format(dim)
        ret[res_name] = dim_res
    except KeyError:
        raise ValueError('Dimension {} not found in the provided'
                         ' dataset.').format(dim)

    min_name = 'geospatial_{}_min'.format(dim)
    max_name = 'geospatial_{}_max'.format(dim)
    units_name = 'geospatial_{}_units'.format(dim)

    try:
        # According to CF Conventions the corresponding 'bounds' variable name
        # should be in the attributes of the coordinate variable
        bnds = ds[dim].attrs['bounds']
        dim_min = min(ds[bnds].values[0][0], ds[bnds].values[-1][1])
        dim_max = max(ds[bnds].values[0][0], ds[bnds].values[-1][1])
    except KeyError:
        dim_min = min(ds[dim].values[0], ds[dim].values[-1]) - dim_res * 0.5
        dim_max = max(ds[dim].values[0], ds[dim].values[-1]) + dim_res * 0.5

    ret[max_name] = dim_max
    ret[min_name] = dim_min

    try:
        dim_units = ds[dim].attrs['units']
    except KeyError:
        dim_units = None

    ret[units_name] = dim_units

    return ret


def _get_temporal_res(time: np.ndarray) -> str:
    """
    Determine temporal resolution of the given datetimes array.

    See also: `ISO 8601 Durations <https://en.wikipedia.org/wiki/ISO_8601#Durations>`_

    :param time: A numpy array containing np.datetime64 objects
    :return: Temporal resolution formatted as an ISO 8601:2004 duration string
    """
    delta = time[1] - time[0]
    days = delta.astype('timedelta64[D]') / np.timedelta64(1, 'D')

    if (27 < days) and (days < 32):
        return 'P1M'
    else:
        return 'P{}D'.format(int(days))


def _get_duration(time: np.ndarray) -> str:
    """
    Determine the duration of the given datetimes array.

    See also: `ISO 8601 Durations <https://en.wikipedia.org/wiki/ISO_8601#Durations>`_

    :param time: A numpy array containing np.datetime64 objects
    :return: Temporal resolution formatted as an ISO 8601:2004 duration string
    """
    delta = time[-1] - time[0]
    days = delta.astype('timedelta64[D]') / np.timedelta64(1, 'D')
    return 'P{}D'.format(int(days))
