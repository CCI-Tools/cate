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

import warnings
from datetime import datetime
from typing import Optional, Sequence, Union, Tuple

import cftime
import numpy as np
import xarray as xr
from matplotlib import path
from shapely.geometry import box, LineString, Polygon

from xcube.core.normalize import normalize_dataset

from .types import PolygonLike, ValidationError
from ..util.misc import to_list
from ..util.monitor import Monitor

__author__ = "Janis Gailis (S[&]T Norway)" \
             "Norman Fomferra (Brockmann Consult GmbH)"

DatetimeTypes = np.datetime64, cftime.datetime, datetime
Datetime = Union[np.datetime64, cftime.datetime, datetime]


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

    Then, if no coordinate variable time is present but the CF attributes "time_coverage_start" and optionally
    "time_coverage_end" are given, a scalar time dimension and coordinate variable will be generated.

    Finally, it will be ensured that a "time" coordinate variable will be of type *datetime*.

    :param ds: The dataset to normalize.
    :return: The normalized dataset, or the original dataset, if it is already "normal".
    """
    ds = normalize_dataset(ds)
    ds = _normalize_inverted_lat(ds)
    return ds


def _normalize_inverted_lat(ds: xr.Dataset) -> xr.Dataset:
    """
    In case the latitude decreases, invert it
    :param ds: some xarray dataset
    :return: a normalized xarray dataset
    """
    try:
        if _lat_inverted(ds.lat):
            ds = ds.sel(lat=slice(None, None, -1))
    except AttributeError:
        # The dataset doesn't have 'lat', probably not geospatial
        pass
    except ValueError:
        # The dataset still has an ND 'lat' array
        pass
    return ds


def adjust_temporal_attrs_impl(ds: xr.Dataset) -> xr.Dataset:
    """
    Adjust the global temporal attributes of the dataset by doing some
    introspection of the dataset and adjusting the appropriate attributes
    accordingly.

    In case the determined attributes do not exist in the dataset, these will
    be added.

    For more information on suggested global attributes see
    `Attribute Convention for Data Discovery
    <http://wiki.esipfed.org/index.php/Attribute_Convention_for_Data_Discovery>`_

    :param ds: Dataset to adjust
    :return: Adjusted dataset
    """

    temporal_attrs = _get_temporal_cf_attrs_from_var(ds)

    if temporal_attrs:
        ds = ds.copy()
        # Align temporal attributes with the ones from the shallow Dataset copy
        for key in temporal_attrs:
            if temporal_attrs[key] is not None:
                ds.attrs[key] = temporal_attrs[key]
            else:
                ds.attrs.pop(key, None)

    return ds


def _get_temporal_cf_attrs_from_var(ds: xr.Dataset, var_name: str = 'time') -> Optional[dict]:
    """
    Get temporal boundaries, resolution and duration of the given dataset. If
    the 'bounds' are explicitly defined, these will be used for calculation,
    otherwise it will rest on information gathered from the 'time' dimension
    itself.

    :param ds: Dataset
    :param var_name: The variable/dimension name.
    :return: A dictionary {'attr_name': attr_value}
    """

    if var_name not in ds:
        return None

    var = ds[var_name]

    if 'bounds' in var.attrs:
        # According to CF Conventions the corresponding 'bounds' coordinate variable name
        # should be in the attributes of the coordinate variable
        bnds_name = var.attrs['bounds']
    else:
        # If 'bounds' attribute is missing, the bounds coordinate variable may be named "<dim>_bnds"
        bnds_name = f'{var_name}_bnds'

    dim_min = dim_max = None
    dim_var = None

    if bnds_name in ds:
        bnds_var = ds[bnds_name]
        if len(bnds_var.shape) == 2 and bnds_var.shape[0] > 0 and bnds_var.shape[1] == 2:
            dim_var = bnds_var
            if bnds_var.shape[0] > 1:
                dim_min = bnds_var[0, 0]
                dim_max = bnds_var[-1, 1]
            else:
                dim_min = bnds_var[0, 0]
                dim_max = bnds_var[0, 1]

    if dim_var is None:
        if len(var.shape) == 1 and var.shape[0] > 0:
            dim_var = var
            if var.shape[0] > 1:
                dim_min = var[0]
                dim_max = var[-1]
            else:
                dim_min = var[0]
                dim_max = var[0]

    # Make sure dim_min and dim_max are valid and are time instances
    # See https://github.com/CCI-Tools/cate/issues/643
    if dim_var is None \
            or not _is_supported_time_dtype(dim_var.dtype):
        # Cannot determine temporal extent for dimension var_name
        return None

    dim_min = dim_min.values
    dim_max = dim_max.values

    if dim_min != dim_max:
        duration = _get_duration(dim_min, dim_max)
    else:
        duration = None

    if dim_min < dim_max and len(var) >= 2:
        resolution = _get_temporal_res(var)
    else:
        resolution = None

    return dict(time_coverage_start=_to_isoformat(dim_min),
                time_coverage_end=_to_isoformat(dim_max),
                time_coverage_duration=duration,
                time_coverage_resolution=resolution)


def _is_supported_time_dtype(dtype: np.dtype) -> bool:
    return any(np.issubdtype(dtype, t) for t in DatetimeTypes)


def _to_isoformat(time_value: np.ndarray) -> str:
    if np.issubdtype(time_value.dtype, cftime.datetime):
        return time_value.item().isoformat()
    else:
        return np.datetime_as_string(time_value, unit='s')


def _get_temporal_res(time: xr.DataArray) -> str:
    """
    Determine temporal resolution of the given datetimes array.

    See also: `ISO 8601 Durations <https://en.wikipedia.org/wiki/ISO_8601#Durations>`_

    :param time: A numpy array containing np.datetime64 objects
    :return: Temporal resolution formatted as an ISO 8601:2004 duration string
    """
    delta = time[1] - time[0]
    days = delta.values.astype('timedelta64[D]') / np.timedelta64(1, 'D')

    if (27 < days) and (days < 32):
        return 'P1M'
    else:
        return 'P{}D'.format(int(days))


def _get_duration(tmin: Datetime, tmax: Datetime) -> str:
    """
    Determine the duration of the given datetimes.

    See also: `ISO 8601 Durations <https://en.wikipedia.org/wiki/ISO_8601#Durations>`_

    :param tmin: Time minimum
    :param tmax: Time maximum
    :return: Temporal resolution formatted as an ISO 8601:2004 duration string
    """
    day = np.timedelta64(1, 'D')
    days = (np.timedelta64(tmax - tmin).astype(dtype='timedelta64[D]') / day) + 1
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
    if lat[0] > lat[-1]:
        return True

    return False


# TODO (forman): idea: introduce ExtentLike type, or make this a class method of PolygonLike and GeometryLike
def get_extents(region: PolygonLike.TYPE) -> Tuple[Tuple[float, float, float, float], bool]:
    """
    Get extents of a PolygonLike, handles explicitly given
    coordinates.

    :param region: PolygonLike to introspect
    :return: ([lon_min, lat_min, lon_max, lat_max], boolean_explicit_coords)
    """
    explicit_coords = False
    extents = None
    # noinspection PyBroadException
    try:
        maybe_rectangle = to_list(region, dtype=float)
        if maybe_rectangle is not None:
            if len(maybe_rectangle) == 4:
                extents = maybe_rectangle
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
        extents = region.bounds

    if extents:
        return extents, explicit_coords
    else:
        raise ValidationError('Could not determine extents of a polygon')


def _pad_extents(ds: xr.Dataset, extents: Sequence[float]):
    """
    Pad extents by half a pixel in both directions, to make sure subsetting
    slices include all pixels crossed by the bounding box. Set extremes
    to maximum valid geospatial extents.
    """
    try:
        lon_pixel = abs(ds.lon[1] - ds.lon[0])
        lon_min = extents[0] - lon_pixel / 2
        lon_max = extents[2] + lon_pixel / 2
    except IndexError:
        # 1D dimension, leave extents as they were
        lon_min = extents[0]
        lon_max = extents[2]

    try:
        lat_pixel = abs(ds.lat[1] - ds.lat[0])
        lat_min = extents[1] - lat_pixel / 2
        lat_max = extents[3] + lat_pixel / 2
    except IndexError:
        lat_min = extents[1]
        lat_max = extents[3]

    lon_min = -180 if lon_min < -180 else lon_min
    lat_min = -90 if lat_min < -90 else lat_min
    lon_max = 180 if lon_max > 180 else lon_max
    lat_max = 90 if lat_max > 90 else lat_max

    return lon_min, lat_min, lon_max, lat_max


def reset_non_spatial(ds_source: xr.Dataset, ds_target: xr.Dataset):
    """
    Find non spatial data arrays in ds_source and set the corresponding
    data variables in ds_target to original ones.

    :param ds_source: Source dataset
    :param ds_target: Target dataset
    """
    non_spatial = list()
    for var_name in ds_source.data_vars.keys():
        if 'lat' not in ds_source[var_name].dims and \
                'lon' not in ds_source[var_name].dims:
            non_spatial.append(var_name)

    retset = ds_target
    for var in non_spatial:
        retset[var] = ds_source[var]

    return retset


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
    :param monitor: optional progress monitor
    :return: Subset dataset
    """

    # Validate whether lat and lon exists.

    if not hasattr(ds, 'lon') or not hasattr(ds, 'lat'):
        raise ValidationError('Cannot apply regional subset. No (valid) geocoding found.')

    if hasattr(ds, 'lon') and len(ds.lon.shape) != 1 \
            or hasattr(ds, 'lat') and len(ds.lat.shape) != 1:
        raise ValidationError('Geocoding not recognised. Variables "lat" and/or "lon" have more than one dimension.')

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
        raise ValidationError('Provided polygon extends outside of geo-spatial bounds.'
                              ' Latitudes should be from -90 to 90 and longitudes from -180 to 180 degrees.')

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
        # TODO (forman): reimplement entirely, respect dask arrays, use roll() array operation
        warnings.warn('Entering inefficient code block that requires refactoring.'
                      ' Expect long execution time and/or out of memory errors.')

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
        ds_subset = ds.sel(**indexers)

        # Preserve the original longitude dimension, masking elements that
        # do not belong to the polygon with NaN.
        with monitor.observing('subset'):
            return reset_non_spatial(ds, ds_subset.reindex_like(ds.lon))

    lon_slice = slice(lon_min, lon_max)
    indexers = {'lat': lat_index, 'lon': lon_slice}
    ds_subset = ds.sel(**indexers)

    if len(ds_subset.lat) == 0 or len(ds_subset.lon) == 0:
        # Empty return dataset => region out of dataset bounds
        raise ValueError("Can not select a region outside dataset boundaries.")

    if not mask or simple_polygon or explicit_coords:
        # The polygon doesn't cross the anti-meridian, it is a simple box -> Use a simple slice
        with monitor.observing('subset'):
            return reset_non_spatial(ds, ds_subset)

    # TODO (forman): reimplement following code entirely, respect dask arrays,
    #  use efficient mask operations from xcube
    warnings.warn('Entering inefficient code block that requires refactoring.'
                  ' Expect long execution time and/or out of memory errors.')

    # Create the mask array. The result of this is a lon/lat DataArray where
    # all pixels falling in the region or on its boundary are denoted with True
    # and all the rest with False. Works on polygon exterior

    polypath = path.Path(np.column_stack([polygon.exterior.coords.xy[0],
                                          polygon.exterior.coords.xy[1]]))

    # Handle also a single pixel and 1D edge cases
    ds_subset_lon_values = ds_subset.lon.values
    ds_subset__lat_values = ds_subset.lat.values
    if len(ds_subset.lat) == 1 or len(ds_subset.lon) == 1:
        # Create a mask directly on pixel centers
        lonm, latm = np.meshgrid(ds_subset.lon.values, ds_subset.lat.values)
        grid_points = [(lon, lat) for lon, lat in zip(lonm.ravel(), latm.ravel())]
        mask_arr = polypath.contains_points(grid_points)
        mask_arr = mask_arr.reshape(lonm.shape)
        mask_arr = xr.DataArray(mask_arr,
                                coords={'lon': ds_subset.lon, 'lat': ds_subset.lat},
                                dims=[ds_subset.lat.dims[0], ds_subset.lon.dims[0]])

        with monitor.observing('subset'):
            # Apply the mask to data
            ds_subset = ds_subset.where(mask_arr, drop=True)

        return reset_non_spatial(ds, ds_subset)

    # The normal case
    # Create a grid of pixel vertices
    lon_pixel = abs(ds.lon[1] - ds.lon[0])
    lat_pixel = abs(ds.lat[1] - ds.lat[0])
    lon_min = ds_subset.lon[0] - lon_pixel / 2
    lon_max = ds_subset.lon[-1] + lon_pixel / 2
    lat_min = ds_subset.lat[0] - lat_pixel / 2
    lat_max = ds_subset.lat[-1] + lat_pixel / 2

    lat_grid = np.linspace(lat_min, lat_max, ds_subset.lat.size + 1, dtype='float32')
    lon_grid = np.linspace(lon_min, lon_max, ds_subset.lon.size + 1, dtype='float32')

    try:
        lonm, latm = np.meshgrid(lon_grid, lat_grid)
    except MemoryError:
        raise ValidationError('Not enough memory to mask the dataset with the given'
                              ' polygon. Try subsetting with masking disabled')

    # Mark all grid points falling within the polygon as True

    monitor.progress(1)
    try:
        grid_points = np.empty((len(lat_grid) * len(lon_grid), 2), dtype='float32')
    except MemoryError:
        raise ValidationError('Not enough memory to mask the dataset with the given'
                              ' polygon. Try subsetting with masking disabled')
    for i, el in enumerate(((lon, lat) for lon, lat in zip(lonm.ravel(), latm.ravel()))):
        grid_points[i] = el

    monitor.progress(1)
    try:
        mask_arr = polypath.contains_points(grid_points)
    except MemoryError:
        raise ValidationError('Not enough memory to mask the dataset with the given'
                              ' polygon. Try subsetting with masking disabled')

    monitor.progress(1)

    mask_arr = mask_arr.reshape(lonm.shape)
    # Vectorized 'rolling window' numpy magic to go from pixel vertices to pixel centers
    mask_arr = mask_arr[1:, 1:] + mask_arr[1:, :-1] + mask_arr[:-1, 1:] + mask_arr[:-1, :-1]

    mask_arr = xr.DataArray(mask_arr,
                            coords={'lon': ds_subset_lon_values, 'lat': ds_subset__lat_values},
                            dims=['lat', 'lon'])

    with monitor.observing('subset'):
        # Apply the mask to data
        ds_subset = ds_subset.where(mask_arr, drop=False)

    monitor.done()
    return reset_non_spatial(ds, ds_subset)


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
        # noinspection PyTypeChecker
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
