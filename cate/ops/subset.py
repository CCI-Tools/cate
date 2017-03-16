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
from shapely.geometry import Point, box

from cate.core.op import op, op_input
from cate.core.types import PolygonLike

@op(tags=['geometric', 'subset', 'spatial', 'geom'])
@op_input('region', data_type=PolygonLike)
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
    if (not (-90 <= lat_min <= 90)) or\
       (not (-90 <= lat_max <= 90)) or\
       (not (-180 <= lon_min <= 180)) or\
       (not (-180 <= lon_max <= 180)):
        raise ValueError('Provided polygon extends outside of geospatial'
                         ' bounds: latitude [-90;90], longitude [-180;180]')

    simple_polygon = False
    if region.equals(box(lon_min, lat_min, lon_max, lat_max)):
        # Don't do the computationally intensive masking if the provided
        # region is a simple box-polygon, for which there will be nothing to
        # mask.
        simple_polygon = True

    if not mask or simple_polygon:
        # Slice
        lat_slice = slice(lat_min, lat_max)
        lon_slice = slice(lon_min, lon_max)
        indexers = {'lat': lat_slice, 'lon': lon_slice}
        return ds.sel(**indexers)

    # Create the mask array. The result of this is a lon/lat DataArray where
    # all values falling in the region or on its boundary are denoted with True
    # and all the rest with False
    lonm, latm = np.meshgrid(ds.lon.values, ds.lat.values)
    mask = np.array([Point(lon, lat).intersects(region) for lon,lat in\
                     zip(lonm.ravel(), latm.ravel())], dtype=bool)
    mask = xr.DataArray(mask.reshape(lonm.shape),
                        coords={'lon': ds.lon, 'lat': ds.lat},
                        dims=['lat', 'lon'])

    # Mask values outside the polygon with NaN, crop the dataset
    return ds.where(mask, drop=True)


@op(tags=['subset', 'temporal'])
def subset_temporal(ds: xr.Dataset,
                    time_min: str,
                    time_max: str) -> xr.Dataset:
    """
    Do a temporal subset of the dataset. When using this operation on the
    command line, it is neccessary to enclose the times in quotes, or in
    escaped quotes when on Linux.

    Windows time_min='YYYY-MM-DD'
    Linux(bash) time_min=\\'YYYY-MM-DD\\'

    :param ds: Dataset to subset
    :param time_min: Minimum time 'YYYY-MM-DD'
    :param time_max: Maximum time 'YYYY-MM-DD'
    :return: Subset dataset
    """
    # If it can be selected, go ahead
    try:
        time_slice = slice(time_min, time_max)
        indexers = {'time': time_slice}
        return ds.sel(**indexers)
    except TypeError:
        # Couldn't select because of unexpected time format but we're going to
        # try more
        pass

    # Handle Julian Day time format
    start = dict()
    end = dict()

    start['y'], start['m'], start['d'] = time_min.split('-')
    end['y'], end['m'], end['d'] = time_max.split('-')

    start_jd1, start_jd2 = jdcal.gcal2jd(start['y'], start['m'], start['d'])
    start_jd = start_jd1 + start_jd2

    end_jd1, end_jd2 = jdcal.gcal2jd(end['y'], end['m'], end['d'])
    end_jd = end_jd1 + end_jd2

    time_slice = slice(start_jd, end_jd)
    indexers = {'time': time_slice}
    return ds.sel(**indexers)


@op(tags=['subset', 'temporal'])
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
