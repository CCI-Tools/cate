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

from datetime import datetime
from typing import Optional, Sequence, Union, Tuple

import numpy as np
import xarray as xr
from jdcal import jd2gcal
from shapely.geometry import box, LineString, Polygon
from matplotlib import path

from .types import PolygonLike
from ..util.misc import to_list
from ..util.monitor import Monitor


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


def adjust_spatial_attrs_impl(ds: xr.Dataset, allow_point: bool) -> xr.Dataset:
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

    copied = False

    for dim in ('lon', 'lat'):
        geo_spatial_attrs = _get_geo_spatial_attrs(ds, dim)
        if geo_spatial_attrs:
            for key in geo_spatial_attrs:
                if geo_spatial_attrs[key] is not None:
                    if not copied:
                        ds = ds.copy()
                        copied = True
                    ds.attrs[key] = geo_spatial_attrs[key]

    lon_min = ds.attrs.get('geospatial_lon_min')
    lat_min = ds.attrs.get('geospatial_lat_min')
    lon_max = ds.attrs.get('geospatial_lon_max')
    lat_max = ds.attrs.get('geospatial_lat_max')

    if lon_min is not None and lat_min is not None and lon_max is not None and lat_max is not None:

        if not copied:
            ds = ds.copy()

        ds.attrs['geospatial_bounds'] = 'POLYGON(({} {}, {} {}, {} {}, {} {}, {} {}))'. \
            format(lon_min, lat_min, lon_min, lat_max, lon_max, lat_max, lon_max, lat_min, lon_min, lat_min)

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

    temporal_attrs = _get_temporal_attrs(ds)

    if temporal_attrs:
        ds = ds.copy()
        for key in temporal_attrs:
            if temporal_attrs[key] is not None:
                ds.attrs[key] = temporal_attrs[key]
            else:
                ds.attrs.pop(key, None)

    return ds


def _get_temporal_attrs(ds: xr.Dataset) -> Optional[dict]:
    """
    Get temporal boundaries, resolution and duration of the given dataset. If
    the 'bounds' are explicitly defined, these will be used for calculation,
    otherwise it will rest on information gathered from the 'time' dimension
    itself.

    :param ds: Dataset
    :return: A dictionary {'attr_name': attr_value}
    """

    if 'time' not in ds:
        return None

    coord_var = ds['time']

    if 'bounds' in coord_var.attrs:
        # According to CF Conventions the corresponding 'bounds' coordinate variable name
        # should be in the attributes of the coordinate variable
        bnds_name = coord_var.attrs['bounds']
    else:
        # If 'bounds' attribute is missing, the bounds coordinate variable may be named "<dim>_bnds"
        bnds_name = 'time_bnds'

    ret = dict()

    if bnds_name in ds:
        bnds_var = ds[bnds_name]
        dim_min = bnds_var.values[0][0]
        dim_max = bnds_var.values[-1][1]
    elif len(coord_var.values) >= 2:
        dim_min = coord_var.values[0]
        dim_max = coord_var.values[-1]
    elif len(coord_var.values) == 1:
        dim_min = coord_var.values[0]
        dim_max = coord_var.values[0]
    else:
        # Cannot determine temporal extent for dimension dim_name
        return None

    if dim_min != dim_max:
        duration = _get_duration(dim_min, dim_max)
    else:
        duration = None

    if dim_min < dim_max and len(coord_var.values) >= 2:
        resolution = _get_temporal_res(coord_var.values)
    else:
        resolution = None

    ret['time_coverage_duration'] = duration
    ret['time_coverage_resolution'] = resolution
    ret['time_coverage_start'] = str(dim_min)
    ret['time_coverage_end'] = str(dim_max)

    return ret


def _get_geo_spatial_attrs(ds: xr.Dataset, dim_name: str, allow_point: bool = False) -> Optional[dict]:
    """
    Get spatial boundaries, resolution and units of the given dimension of the given
    dataset. If the 'bounds' are explicitly defined, these will be used for
    boundary calculation, otherwise it will rest purely on information gathered
    from 'dim' itself.

    :param ds: The dataset
    :param dim_name: The dimension name.
    :param allow_point: True, if it is ok to have no actual spatial extent.
    :return: A dictionary {'attr_name': attr_value}
    """
    ret = dict()

    if dim_name not in ds:
        return None

    ret = dict()

    coord_var = ds[dim_name]

    if 'bounds' in coord_var.attrs:
        # According to CF Conventions the corresponding 'bounds' coordinate variable name
        # should be in the attributes of the coordinate variable
        bnds_name = coord_var.attrs['bounds']
    else:
        # If 'bounds' attribute is missing, the bounds coordinate variable may be named "<dim>_bnds"
        bnds_name = '%s_bnds' % dim_name

    if bnds_name in ds:
        bnds_var = ds[bnds_name]
        dim_res = abs(bnds_var.values[0][1] - bnds_var.values[0][0])
        dim_min = min(bnds_var.values[0][0], bnds_var.values[-1][1])
        dim_max = max(bnds_var.values[0][0], bnds_var.values[-1][1])
    elif len(coord_var.values) >= 2:
        dim_res = abs(coord_var.values[1] - coord_var.values[0])
        dim_min = min(coord_var.values[0], coord_var.values[-1]) - 0.5 * dim_res
        dim_max = max(coord_var.values[0], coord_var.values[-1]) + 0.5 * dim_res
    elif len(coord_var.values) == 1 and allow_point:
        # Actually a point with no extent
        dim_res = 0.0
        dim_min = coord_var.values[0]
        dim_max = coord_var.values[0]
    else:
        # Cannot determine spatial extent for dimension dim_name
        return None

    if 'units' in coord_var.attrs:
        dim_units = coord_var.attrs['units']
    else:
        dim_units = None

    res_name = 'geospatial_{}_resolution'.format(dim_name)
    min_name = 'geospatial_{}_min'.format(dim_name)
    max_name = 'geospatial_{}_max'.format(dim_name)
    units_name = 'geospatial_{}_units'.format(dim_name)
    ret[res_name] = dim_res
    ret[min_name] = dim_min
    ret[max_name] = dim_max
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


def _lat_inverted(lat: xr.DataArray) -> bool:
    """
    Determine if the latitude is inverted
    """
    if lat.values[0] > lat.values[-1]:
        return True

    return False


def get_extents(region: PolygonLike.TYPE):
    """
    Get extents of a PolygonLike, handles explicitly given
    coordinates.

    :param region: PolygonLike to introspect
    :return: ([lon_min, lat_min, lon_max, lat_max], boolean_explicit_coords)
    """
    explicit_coords = False
    try:
        maybe_rectangle = to_list(region, dtype=float)
        if maybe_rectangle is not None:
            if len(maybe_rectangle) == 4:
                lon_min, lat_min, lon_max, lat_max = maybe_rectangle
                explicit_coords = True
    except BaseException:
        # The polygon must be convertible, but it's complex
        polygon = PolygonLike.convert(region)
        if not polygon.is_valid:
            # Heal polygon, see #506 and Shapely User Manual
            region = polygon.buffer(0)
        else:
            region = polygon
        # Get the bounding box
        lon_min, lat_min, lon_max, lat_max = region.bounds

    return([lon_min, lat_min, lon_max, lat_max], explicit_coords)


def _pad_extents(ds: xr.Dataset, extents: tuple):
    """
    Pad extents by half a pixel in both directions, to make sure subsetting
    slices include all pixels crossed by the bounding box. Set extremes
    to maximum valid geospatial extents.
    """
    try:
        lon_pixel = abs(ds.lon.values[1] - ds.lon.values[0])
        lon_min = extents[0] - lon_pixel / 2
        lon_max = extents[2] + lon_pixel / 2
    except IndexError:
        # 1D dimension, leave extents as they were
        lon_min = extents[0]
        lon_max = extents[2]

    try:
        lat_pixel = abs(ds.lat.values[1] - ds.lat.values[0])
        lat_min = extents[1] - lat_pixel / 2
        lat_max = extents[3] + lat_pixel / 2
    except IndexError:
        lat_min = extents[1]
        lat_max = extents[3]

    lon_min = -180 if lon_min < -180 else lon_min
    lat_min = -90 if lat_min < -90 else lat_min
    lon_max = 180 if lon_max > 180 else lon_max
    lat_max = 90 if lat_max > 90 else lat_max

    return (lon_min, lat_min, lon_max, lat_max)


def subset_spatial_impl(ds: xr.Dataset,
                        region: PolygonLike.TYPE,
                        mask: bool = True,
                        monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Do a spatial subset of the dataset

    :param ds: Dataset to subset
    :param region: Spatial region to subset
    :param mask: Should values falling in the bounding box of the polygon but
    not the polygon itself be masked with NaN.
    :return: Subset dataset
    """
    monitor.start('Subset', 10)
    # Validate input
    try:
        polygon = PolygonLike.convert(region)
    except BaseException as e:
        raise e

    extents, explicit_coords = get_extents(region)

    lon_min, lat_min, lon_max, lat_max = extents

    # Validate the bounding box
    if (not (-90 <= lat_min <= 90)) or \
            (not (-90 <= lat_max <= 90)) or \
            (not (-180 <= lon_min <= 180)) or \
            (not (-180 <= lon_max <= 180)):
        raise ValueError('Provided polygon extends outside of geospatial'
                         ' bounds: latitude [-90;90], longitude [-180;180]')

    # Don't do the computationally intensive masking if the provided
    # region is a simple box-polygon, for which there will be nothing to
    # mask.
    simple_polygon = polygon.equals(box(lon_min, lat_min, lon_max, lat_max))

    # Pad extents to include crossed pixels
    lon_min, lat_min, lon_max, lat_max = _pad_extents(ds, extents)

    crosses_antimeridian = (lon_min > lon_max) if explicit_coords else _crosses_antimeridian(polygon)
    lat_inverted = _lat_inverted(ds.lat)
    if lat_inverted:
        lat_index = slice(lat_max, lat_min)
    else:
        lat_index = slice(lat_min, lat_max)

    if crosses_antimeridian and not simple_polygon and mask:
        # Unlikely but plausible
        raise NotImplementedError('Spatial subsets crossing the anti-meridian'
                                  ' are currently implemented for simple,'
                                  ' rectangular polygons only, or complex polygons'
                                  ' without masking.')
    monitor.progress(1)
    if crosses_antimeridian:
        # Shapely messes up longitudes if the polygon crosses the antimeridian
        if not explicit_coords:
            lon_min, lon_max = lon_max, lon_min

        # Can't perform a simple selection with slice, hence we have to
        # construct an appropriate longitude indexer for selection
        lon_left_of_idl = slice(lon_min, 180)
        lon_right_of_idl = slice(-180, lon_max)
        lon_index = xr.concat((ds.lon.sel(lon=lon_right_of_idl),
                               ds.lon.sel(lon=lon_left_of_idl)), dim='lon')

        indexers = {'lon': lon_index, 'lat': lat_index}
        retset = ds.sel(**indexers)

        # Preserve the original longitude dimension, masking elements that
        # do not belong to the polygon with NaN.
        with monitor.observing(8):
            return retset.reindex_like(ds.lon)

    lon_slice = slice(lon_min, lon_max)
    indexers = {'lat': lat_index, 'lon': lon_slice}
    retset = ds.sel(**indexers)

    if len(retset.lat) == 0 or len(retset.lon) == 0:
        # Empty return dataset => region out of dataset bounds
        raise ValueError("Can not select a region outside dataset boundaries.")

    if not mask or simple_polygon or explicit_coords:
        # The polygon doesn't cross the anti-meridian, it is a simple box -> Use a simple slice
        with monitor.observing(8):
            return retset

    # Create the mask array. The result of this is a lon/lat DataArray where
    # all pixels falling in the region or on its boundary are denoted with True
    # and all the rest with False. Works on polygon exterior

    polypath = path.Path(np.column_stack([polygon.exterior.coords.xy[0],
                                          polygon.exterior.coords.xy[1]]))

    # Handle also a single pixel and 1D edge cases
    if len(retset.lat) == 1 or len(retset.lon) == 1:
        # Create a mask directly on pixel centers
        lonm, latm = np.meshgrid(retset.lon.values, retset.lat.values)
        grid_points = [(lon, lat) for lon, lat in zip(lonm.ravel(), latm.ravel())]
        mask = polypath.contains_points(grid_points)
        mask = mask.reshape(lonm.shape)
        mask = xr.DataArray(mask,
                            coords={'lon': retset.lon.values, 'lat': retset.lat.values},
                            dims=['lat', 'lon'])

        with monitor.observing(5):
            # Apply the mask to data
            retset = retset.where(mask, drop=True)
        return retset

    # The normal case
    # Create a grid of pixel vertices
    lon_pixel = abs(ds.lon.values[1] - ds.lon.values[0])
    lat_pixel = abs(ds.lat.values[1] - ds.lat.values[0])

    lon_min = retset.lon.values[0] - lon_pixel / 2
    lon_max = retset.lon.values[-1] + lon_pixel / 2
    lat_min = retset.lat.values[0] - lat_pixel / 2
    lat_max = retset.lat.values[-1] + lat_pixel / 2

    lat_grid = np.linspace(lat_min, lat_max, len(retset.lat.values) + 1)
    lon_grid = np.linspace(lon_min, lon_max, len(retset.lon.values) + 1)

    lonm, latm = np.meshgrid(lon_grid, lat_grid)

    # Mark all grid points falling within the polygon as True

    monitor.progress(1)
    grid_points = [(lon, lat) for lon, lat in zip(lonm.ravel(), latm.ravel())]

    monitor.progress(1)
    mask = polypath.contains_points(grid_points)

    monitor.progress(1)

    mask = mask.reshape(lonm.shape)
    # Vectorized 'rolling window' numpy magic to go from pixel vertices to pixel centers
    mask = mask[1:, 1:] + mask[1:, :-1] + mask[:-1, 1:] + mask[:-1, :-1]

    mask = xr.DataArray(mask,
                        coords={'lon': retset.lon.values, 'lat': retset.lat.values},
                        dims=['lat', 'lon'])

    with monitor.observing(5):
        # Apply the mask to data
        retset = retset.where(mask, drop=True)

    monitor.done()
    return retset


def _crosses_antimeridian(region: Polygon) -> bool:
    """
    Determine if the given region crosses the Antimeridian line, by converting
    the given Polygon from -180;180 to 0;360 and checking if the antimeridian
    line crosses it.

    This only works with Polygons without holes

    :param region: Polygon to test
    """

    # Convert region to only have positive longitudes.
    # This way we can perform a simple antimeridian check

    old_exterior = region.exterior.coords
    new_exterior = []
    for point in old_exterior:
        lon, lat = point[0], point[1]
        if -180. <= lon < 0.:
            lon += 360.
        new_exterior.append((lon, lat))
    converted_region = Polygon(new_exterior)

    # There's a problem at this point. Any polygon crossed by the zero-th
    # meridian can in principle convert to an inverted polygon that is crossed
    # by the antimeridian.

    if not converted_region.is_valid:
        # The polygon 'became' invalid upon conversion => probably the original
        # polygon is what we want

        # noinspection PyBroadException
        try:
            # First try cleaning up geometry that is invalid
            converted_region = converted_region.buffer(0)
        except BaseException:
            pass
        if not converted_region.is_valid:
            return False

    test_line = LineString([(180, -90), (180, 90)])
    if test_line.crosses(converted_region):
        # The converted polygon seems to be valid and crossed by the
        # antimeridian. At this point there's no 'perfect' way how to tell if
        # we wanted the converted polygon or the original one.

        # A simple heuristic is to check for size. The smaller one is quite
        # likely the desired one
        if converted_region.area < region.area:
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
