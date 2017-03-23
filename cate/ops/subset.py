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

Subset operations

Components
==========
"""

import xarray as xr
import numpy as np
import jdcal
from shapely.geometry import Point, box, LineString
from shapely.wkt import loads, dumps

from cate.core.op import op, op_input, op_return
from cate.core.types import PolygonLike, TimeRangeLike


@op(tags=['geometric', 'subset', 'spatial', 'geom'], version='1.0')
@op_input('region', data_type=PolygonLike)
@op_return(add_history=True)
def subset_spatial(ds: xr.Dataset,
                   region: PolygonLike.TYPE,
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
    region = PolygonLike.convert(region)
    lon_min, lat_min, lon_max, lat_max = region.bounds

    # Validate the bounding box
    if (not (-90 <= lat_min <= 90)) or \
            (not (-90 <= lat_max <= 90)) or \
            (not (-180 <= lon_min <= 180)) or \
            (not (-180 <= lon_max <= 180)):
        raise ValueError('Provided polygon extends outside of geospatial'
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
        raise NotImplementedError('Spatial subset over the International Date'
                                  ' Line is currently implemented for simple,'
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


def _crosses_antimeridian(region: PolygonLike.TYPE) -> bool:
    """
    Determine if the given region crosses the Antimeridian line, by converting
    the given Polygon from -180;180 to 0;360 and checking if the antimeridian
    line crosses it.

    This only works with Polygons without holes

    :param region: PolygonLike to test
    """
    region = PolygonLike.convert(region)

    # Retrieving the points of the Polygon are a bit troublesome, parsing WKT
    # is more straightforward and probably faster
    new_wkt = 'POLYGON (('

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


@op(tags=['subset', 'temporal'], version='1.0')
@op_input('time_range', data_type=TimeRangeLike)
@op_return(add_history=True)
def subset_temporal(ds: xr.Dataset,
                    time_range: TimeRangeLike.TYPE) -> xr.Dataset:
    """
    Do a temporal subset of the dataset.

    :param ds: Dataset to subset
    :param time_range: Time range to select
    :return: Subset dataset
    """
    time_range = TimeRangeLike.convert(time_range)
    # If it can be selected, go ahead
    try:
        time_slice = slice(time_range[0], time_range[1])
        indexers = {'time': time_slice}
        return ds.sel(**indexers)
    except TypeError:
        raise ValueError('Time subset operation expects a dataset with the'
                         ' time coordinate of type datetime64[ns], but received'
                         ' {}. Running the harmonization operation on this'
                         ' dataset may help'.format(ds.time.dtype))


@op(tags=['subset', 'temporal'], version='1.0')
@op_return(add_history=True)
def subset_temporal_index(ds: xr.Dataset,
                          time_ind_min: int,
                          time_ind_max: int) -> xr.Dataset:
    """
    Do a temporal indices based subset

    :param ds: Dataset to subset
    :param time_ind_min: Minimum time index to select
    :param time_ind_max: Maximum time index to select
    :return: Subset dataset
    """
    # we're creating a slice that includes both ends
    # to have the same functionality as subset_temporal
    time_slice = slice(time_ind_min, time_ind_max + 1)
    indexers = {'time': time_slice}
    return ds.isel(**indexers)
