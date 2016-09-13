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

Subset operations

Components
==========
"""

import xarray as xr
from ect.core.op import op_input, op_output


@op_input('ds', description='A dataset to subset')
@op_input('lat', description='[lat_min, lat_max] to select')
@op_input('lon', description='[lon_min, lon_max] to select')
@op_output('return', description='The subset dataset')
def subset_spatial(ds: xr.Dataset, lat: list, lon: list):
    """
    Do a spatial subset of the dataset

    :param ds: Dataset to subset
    :param lat: A pair of lat_min, lat_max values
    :param lon: A pair of lon_min, lon_max values
    :return: Subset dataset
    """
    lat_slice = slice(lat[0], lat[1])
    lon_slice = slice(lon[0], lon[1])
    indexers = {'lat': lat_slice, 'lon': lon_slice}
    return ds.sel(**indexers)


@op_input('ds', description='A dataset to subset')
@op_input('time', description='[time_min, time_max] to select')
@op_output('return', description='The subset dataset')
def subset_temporal(ds: xr.Dataset, time: list):
    """
    Do a temporal subset of the dataset

    :param ds: Dataset to subset
    :param time: A pair of time_min, time_max values
    :return: Subset dataset
    """
    time_slice = slice(time[0], time[1])
    indexers = {'time': time_slice}
    return ds.sel(**indexers)


@op_input('ds', description='A dataset to subset')
@op_input('time', description='[time_index_min, time_index_max] to select')
@op_output('return', description='The subset dataset')
def subset_temporal_index(ds: xr.Dataset, time: list):
    """
    Do a temporal indices based subset

    :param ds: Dataset to subset
    :param time: A pair of time_min_index, time_max_index
    :return: Subset dataset
    """
    # we're creating a slice that includes both ends
    # to have the same functionality as subset_temporal
    time_slice = slice(time[0], time[1] + 1)
    indexers = {'time': time_slice}
    return ds.isel(**indexers)
