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

from cate.core.op import op, op_input, op_return
from cate.core.types import PolygonLike, TimeRangeLike, DatasetLike
from cate.ops.normalize import adjust_spatial_attrs, adjust_temporal_attrs

from cate.util.opimpl import subset_spatial_impl, subset_temporal_impl
from cate.util.opimpl import subset_temporal_index_impl


@op(tags=['geometric', 'spatial', 'subset'], version='1.0')
@op_input('region', data_type=PolygonLike)
@op_return(add_history=True)
def subset_spatial(ds: xr.Dataset,
                   region: PolygonLike.TYPE,
                   mask: bool = True) -> xr.Dataset:
    """
    Do a spatial subset of the dataset

    :param ds: Dataset to subset
    :param region: Spatial region to subset
    :param mask: Should values falling in the bounding box of the polygon but not the polygon itself be masked with NaN.
    :return: Subset dataset
    """
    region = PolygonLike.convert(region)
    return adjust_spatial_attrs(subset_spatial_impl(ds, region, mask))


@op(tags=['subset', 'temporal', 'filter'], version='1.0')
@op_input('ds', data_type=DatasetLike)
@op_input('time_range', data_type=TimeRangeLike)
@op_return(add_history=True)
def subset_temporal(ds: DatasetLike.TYPE,
                    time_range: TimeRangeLike.TYPE) -> xr.Dataset:
    """
    Do a temporal subset of the dataset.

    :param ds: Dataset or dataframe to subset
    :param time_range: Time range to select
    :return: Subset dataset
    """
    ds = DatasetLike.convert(ds)
    time_range = TimeRangeLike.convert(time_range)
    return adjust_temporal_attrs(subset_temporal_impl(ds, time_range))


@op(tags=['subset', 'temporal', 'filter', 'utility'], version='1.0')
@op_input('ds', data_type=DatasetLike)
@op_return(add_history=True)
def subset_temporal_index(ds: DatasetLike.TYPE,
                          time_ind_min: int,
                          time_ind_max: int) -> xr.Dataset:
    """
    Do a temporal indices based subset

    :param ds: Dataset or dataframe to subset
    :param time_ind_min: Minimum time index to select
    :param time_ind_max: Maximum time index to select
    :return: Subset dataset
    """
    ds = DatasetLike.convert(ds)
    return subset_temporal_index_impl(ds, time_ind_min, time_ind_max)
