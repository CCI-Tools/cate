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
from typing import Dict

import xarray as xr

from cate.core.op import op, op_input, op_return
from cate.core.types import PolygonLike, TimeRangeLike, DatasetLike, PointLike, DictLike
from cate.ops.normalize import adjust_spatial_attrs, adjust_temporal_attrs

from cate.util.opimpl import subset_spatial_impl, subset_temporal_impl, subset_temporal_index_impl, _get_spatial_props


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


@op(tags=['subset', 'utility'], version='1.0')
@op_input('ds', data_type=DatasetLike)
@op_input('point', data_type=PointLike)
@op_input('dim_index', data_type=DictLike, nullable=True)
@op_return(add_history=False)
def subset_point(ds: DatasetLike.TYPE,
                 point: PointLike.TYPE,
                 dim_index: DictLike.TYPE = None) -> Dict:
    """
    Do a subset at a point location. The returned dict will contain scalar
    values of all variables for which all dimension have been given in ``dim_index``.
    For the dimensions *lon* and *lat* a nearest neighbour lookup is performed.
    All other dimensions must mach exact.

    :param ds: Dataset or dataframe to subset
    :param point: geographic point given by longitude and latitude
    :param dim_index: Keyword arguments with names matching dimensions and values given by scalars.
    :return: A dict with the scalar values of all variables and the variable names as keys.
    """
    ds = DatasetLike.convert(ds)
    point = PointLike.convert(point)
    dim_index = DictLike.convert(dim_index) or {}

    lon_lat_indexers = {'lon': point.x, 'lat': point.y}
    tolerance = _get_tolerance(ds)

    variable_values = {}
    var_names = sorted(ds.data_vars.keys())
    for var_name in var_names:
        if not var_name.endswith('_bnds'):
            variable = ds.data_vars[var_name]
            indexers = {}
            used_dims = {'lat', 'lon'}
            for dim_name, dim_value in dim_index.items():
                if dim_name in variable.dims:
                    indexers[dim_name] = dim_value
                    used_dims.add(dim_name)
            if set(variable.dims) == set(used_dims):
                try:
                    lon_lat_data = variable.sel(**indexers)
                except KeyError:
                    # if there is no exact match for the "additional" dims, skip this variable
                    continue
                try:
                    point_data = lon_lat_data.sel(method='nearest', tolerance=tolerance, **lon_lat_indexers)
                except KeyError:
                    # if there is no point within the given tolerance, return an empty dict
                    return {}
                if not variable_values:
                    variable_values['lat'] = float(point_data.lat)
                    variable_values['lon'] = float(point_data.lon)
                variable_values[var_name] = float(point_data.values)
    return variable_values


def _get_tolerance(ds: xr.Dataset):
    lon_res_attr_name = 'geospatial_lon_resolution'
    lat_res_attr_name = 'geospatial_lat_resolution'
    if lat_res_attr_name in ds.attrs and lon_res_attr_name in ds.attrs:
        lon_res = ds.attrs[lon_res_attr_name]
        lat_res = ds.attrs[lat_res_attr_name]
    else:
        lon_res = _get_spatial_props(ds, 'lon')[lon_res_attr_name]
        lat_res = _get_spatial_props(ds, 'lat')[lat_res_attr_name]
    return (lon_res + lat_res) / 2
