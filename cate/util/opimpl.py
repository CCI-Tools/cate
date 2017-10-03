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

__author__ = "Janis Gailis (S[&]T Norway)"

from typing import Optional, Sequence, Union, Tuple

import xarray as xr
import numpy as np
from shapely.geometry import Point, box, LineString, Polygon
from shapely.wkt import loads, dumps

from jdcal import jd2gcal
from datetime import datetime


def normalize_impl(ds: xr.Dataset) -> xr.Dataset:
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
    lat_name = get_lat_dim_name_impl(ds)
    lon_name = get_lon_dim_name_impl(ds)

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


def adjust_spatial_attrs_impl(ds: xr.Dataset) -> xr.Dataset:
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
            if geoattrs[key] is not None:
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


def adjust_temporal_attrs_impl(ds: xr.Dataset) -> xr.Dataset:
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

    tempattrs = _get_temporal_props(ds)

    for key in tempattrs:
        if tempattrs[key] is not None:
            ds.attrs[key] = tempattrs[key]
        else:
            ds.attrs.pop(key, None)

    return ds


def _get_temporal_props(ds: xr.Dataset) -> dict:
    """
    Get temporal boundaries, resolution and duration of the given dataset. If
    the 'bounds' are explicitly defined, these will be used for calculation,
    otherwise it will rest on information gathered from the 'time' dimension
    itself.

    :param ds: Dataset
    :return: A dictionary {'attr_name': attr_value}
    """
    ret = dict()

    try:
        # According to CF conventions, the 'bounds' variable name should be in
        # the attributes of the coordinate variable
        bnds = ds['time'].attrs['bounds']
        time_min = ds[bnds].values[0][0]
        time_max = ds[bnds].values[-1][1]
    except KeyError:
        time_min = ds['time'].values[0]
        time_max = ds['time'].values[-1]

    if time_min != time_max:
        ret['time_coverage_duration'] = _get_duration(time_min, time_max)
    else:
        ret['time_coverage_duration'] = None

    if ds['time'].values[0] == ds['time'].values[-1]:
        ret['time_coverage_resolution'] = None
    else:
        ret['time_coverage_resolution'] = _get_temporal_res(ds.time.values)

    ret['time_coverage_start'] = str(time_min)
    ret['time_coverage_end'] = str(time_max)

    return ret


def _get_spatial_props(ds: xr.Dataset, dim: str) -> dict:
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
        raise ValueError('Dimension {} not found in the provided dataset.'.format(dim))

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


def _get_duration(tmin: np.datetime64, tmax: np.datetime64) -> str:
    """
    Determine the duration of the given datetimes.

    See also: `ISO 8601 Durations <https://en.wikipedia.org/wiki/ISO_8601#Durations>`_

    :param tmin: Time minimum
    :param tmax: Time maximum
    :return: Temporal resolution formatted as an ISO 8601:2004 duration string
    """
    delta = tmax - tmin
    day = np.timedelta64(1, 'D')
    days = (delta.astype('timedelta64[D]') / day) + 1
    return 'P{}D'.format(int(days))


def get_lon_dim_name_impl(ds: Union[xr.Dataset, xr.DataArray]) -> Optional[str]:
    """
    Get the name of the longitude dimension.
    :param ds: An xarray Dataset
    :return: the name or None
    """
    return _get_dim_name(ds, ['lon', 'longitude', 'long'])


def get_lat_dim_name_impl(ds: Union[xr.Dataset, xr.DataArray]) -> Optional[str]:
    """
    Get the name of the latitude dimension.
    :param ds: An xarray Dataset
    :return: the name or None
    """
    return _get_dim_name(ds, ['lat', 'latitude'])


def _get_dim_name(ds: Union[xr.Dataset, xr.DataArray], possible_names: Sequence[str]) -> Optional[str]:
    for name in possible_names:
        if name in ds.dims:
            return name
    return None


def subset_spatial_impl(ds: xr.Dataset,
                        region: Polygon,
                        mask: bool = True) -> xr.Dataset:
    """
    Do a spatial subset of the dataset

    :param ds: Dataset to subset
    :param region: Spatial region to subset
    :param mask: Should values falling in the bounding box of the polygon but
    not the polygon itself be masked with NaN.
    :return: Subset dataset
    """
    # Get the bounding box
    lon_min, lat_min, lon_max, lat_max = region.bounds

    # Validate the bounding box
    if (not (-90 <= lat_min <= 90)) or \
            (not (-90 <= lat_max <= 90)) or \
            (not (-180 <= lon_min <= 180)) or \
            (not (-180 <= lon_max <= 180)):
        raise ValueError('Provided polygon extent outside of geospatial'
                         ' bounds: latitude [-90;90], longitude [-180;180]')

    simple_polygon = False
    if region.equals(box(lon_min, lat_min, lon_max, lat_max)):
        # Don't do the computationally intensive masking if the provided
        # region is a simple box-polygon, for which there will be nothing to
        # mask.
        simple_polygon = True

    crosses_antimeridian = _crosses_antimeridian(region)

    if crosses_antimeridian and not simple_polygon:
        # Unlikely but plausible
        raise NotImplementedError('Spatial subsets crossing the anti-meridian'
                                  ' are currently implemented for simple,'
                                  ' rectangular polygons only.')

    if crosses_antimeridian:
        # Shapely messes up longitudes if the polygon crosses the antimeridian
        lon_min, lon_max = lon_max, lon_min

        # Can't perform a simple selection with slice, hence we have to
        # construct an appropriate longitude indexer for selection
        lon_left_of_idl = slice(lon_min, 180)
        lon_right_of_idl = slice(-180, lon_max)
        lon_index = xr.concat((ds.lon.sel(lon=lon_right_of_idl),
                               ds.lon.sel(lon=lon_left_of_idl)), dim='lon')
        indexers = {'lon': lon_index, 'lat': slice(lat_min, lat_max)}
        retset = ds.sel(**indexers)

        if mask:
            # Preserve the original longitude dimension, masking elements that
            # do not belong to the polygon with NaN.
            return retset.reindex_like(ds.lon)
        else:
            # Return the dataset with no NaNs and with a disjoint longitude
            # dimension
            return retset

    if not mask or simple_polygon:
        # The polygon doesn't cross the IDL, it is a simple box -> Use a simple slice
        lat_slice = slice(lat_min, lat_max)
        lon_slice = slice(lon_min, lon_max)
        indexers = {'lat': lat_slice, 'lon': lon_slice}
        return ds.sel(**indexers)

    # Create the mask array. The result of this is a lon/lat DataArray where
    # all values falling in the region or on its boundary are denoted with True
    # and all the rest with False
    lonm, latm = np.meshgrid(ds.lon.values, ds.lat.values)
    mask = np.array([Point(lon, lat).intersects(region) for lon, lat in
                     zip(lonm.ravel(), latm.ravel())], dtype=bool)
    mask = xr.DataArray(mask.reshape(lonm.shape),
                        coords={'lon': ds.lon, 'lat': ds.lat},
                        dims=['lat', 'lon'])

    # Mask values outside the polygon with NaN, crop the dataset
    return ds.where(mask, drop=True)

def _crosses_antimeridian(region: Polygon) -> bool:
    """
    Determine if the given region crosses the Antimeridian line, by converting
    the given Polygon from -180;180 to 0;360 and checking if the antimeridian
    line crosses it.

    This only works with Polygons without holes

    :param region: Polygon to test
    """
    # Retrieving the points of the Polygon are a bit troublesome, parsing WKT
    # is more straightforward and probably faster
    new_wkt = 'POLYGON (('

    # TODO (forman): @JanisGailis this is probably the most inefficient antimeridian-crossing-test I've ever seen :D
    #      What is the problem in using the exterior longitude coordinates?
    #      Please note:
    #      - WKT parsing and formatting is actually NEVER faster than direct coordinate access (C-impl.!)
    #      - The polygon's interior can be neglected, only the exterior is required for the test
    #      - Area computations as used here are probably expensive.
    #      - An accurate and really fast test takes the orientation into account (polygon.is_ccw)
    #        and detects intersections of each segement with the antimeridian line.

    # [10:-2] gets rid of POLYGON (( and ))
    for point in dumps(region)[10:-2].split(','):
        point = point.strip()
        lon, lat = point.split(' ')
        lon = float(lon)
        if -180 <= lon < 0:
            lon += 360
        new_wkt += '{} {}, '.format(lon, lat)
    new_wkt = new_wkt[:-2] + '))'

    converted = loads(new_wkt)

    # There's a problem at this point. Any polygon crossed by the zeroth
    # meridian can in principle convert to an inverted polygon that is crossed
    # by the antimeridian.

    if not converted.is_valid:
        # The polygon 'became' invalid upon conversion => probably the original
        # polygon is what we want
        return False

    test_line = LineString([(180, -90), (180, 90)])
    if test_line.crosses(converted):
        # The converted polygon seems to be valid and crossed by the
        # antimeridian. At this point there's no 'perfect' way how to tell if
        # we wanted the converted polygon or the original one.

        # A simple heuristic is to check for size. The smaller one is quite
        # likely the desired one
        if converted.area < region.area:
            return True
        else:
            return False


def subset_temporal_impl(ds: xr.Dataset,
                         time_range: Tuple[datetime, datetime]) -> xr.Dataset:
    """
    Do a temporal subset of the dataset.

    :param ds: Dataset or dataframe to subset
    :param time_range: Time range to select
    :return: Subset dataset
    """
    # If it can be selected, go ahead
    try:
        time_slice = slice(time_range[0], time_range[1])
        indexers = {'time': time_slice}
        return ds.sel(**indexers)
    except TypeError:
        raise ValueError('Time subset operation expects a dataset with the'
                         ' time coordinate of type datetime64[ns], but received'
                         ' {}. Running the normalize operation on this'
                         ' dataset may help'.format(ds.time.dtype))


def subset_temporal_index_impl(ds: xr.Dataset,
                               time_ind_min: int,
                               time_ind_max: int) -> xr.Dataset:
    """
    Do a temporal indices based subset

    :param ds: Dataset or dataframe to subset
    :param time_ind_min: Minimum time index to select
    :param time_ind_max: Maximum time index to select
    :return: Subset dataset
    """
    # we're creating a slice that includes both ends
    # to have the same functionality as subset_temporal
    time_slice = slice(time_ind_min, time_ind_max + 1)
    indexers = {'time': time_slice}
    return ds.isel(**indexers)
