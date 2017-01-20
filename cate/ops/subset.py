# The MIT License (MIT)
# Copyright (c) 2016 by the Cate Development Team and contributors
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
from cate.core.op import op, op_input
import jdcal


@op(tags=['geometric', 'subset', 'spatial', 'geom'])
@op_input('lat_min', units='degrees', value_range=[-90, 90])
@op_input('lat_max', units='degrees', value_range=[-90, 90])
@op_input('lon_min', units='degrees', value_range=[-180, 180])
@op_input('lon_max', units='degrees', value_range=[-180, 180])
def subset_spatial(ds: xr.Dataset,
                   lat_min: float,
                   lat_max: float,
                   lon_min: float,
                   lon_max: float) -> xr.Dataset:
    """
    Do a spatial subset of the dataset

    :param ds: Dataset to subset
    :param lat_min: Minimum latitude value
    :param lat_max: Maximum latitude value
    :param lon_min: Minimum longitude value
    :param lon_max: Maximum longitude value
    :return: Subset dataset
    """
    lat_slice = slice(lat_min, lat_max)
    lon_slice = slice(lon_min, lon_max)
    indexers = {'lat': lat_slice, 'lon': lon_slice}
    return ds.sel(**indexers)


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
