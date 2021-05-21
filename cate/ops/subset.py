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
from cate.core.opimpl import subset_spatial_impl, subset_temporal_impl, subset_temporal_index_impl
from cate.core.types import PolygonLike, TimeRangeLike, DatasetLike, PointLike, DictLike
from cate.ops.normalize import adjust_spatial_attrs, adjust_temporal_attrs
from cate.util.misc import to_scalar
from cate.util.monitor import Monitor
from cate.util.undefined import UNDEFINED

from xcube.core.normalize import get_geo_spatial_cf_attrs_from_var


@op(tags=['geometric', 'spatial', 'subset'], version='1.0')
@op_input('region', data_type=PolygonLike)
@op_return(add_history=True)
def subset_spatial(ds: xr.Dataset,
                   region: PolygonLike.TYPE,
                   mask: bool = True,
                   monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Do a spatial subset of the dataset

    :param ds: Dataset to subset
    :param region: Spatial region to subset
    :param mask: Should values falling in the bounding box of the polygon but not the polygon
    itself be masked with NaN.
    :param monitor: A monitor to report the progress of the process
    :return: Subset dataset
    """
    return adjust_spatial_attrs(subset_spatial_impl(ds, region, mask, monitor), allow_point=True)


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


def extract_point(ds: DatasetLike.TYPE,
                  point: PointLike.TYPE,
                  indexers: DictLike.TYPE = None,
                  tolerance_default: float = 0.01) -> Dict:
    """
    Extract data at the given point location. The returned dict will contain scalar
    values for all variables for which all dimension have been given in ``indexers``.
    For the dimensions *lon* and *lat* a nearest neighbour lookup is performed.
    All other dimensions must mach exact.

    :param ds: Dataset or dataframe to subset
    :param point: Geographic point given by longitude and latitude
    :param indexers: Optional indexers into data array of *var*. The *indexers* is a dictionary
           or a comma-separated string of key-value pairs that maps the variable's dimension names
           to constant labels. e.g. "layer=4".
    :param tolerance_default: The default longitude and latitude tolerance for the nearest
           neighbour lookup.
           It will only be used, if it is not possible to deduce the resolution of the dataset.
    :return: A dict with the scalar values of all variables and the variable names as keys.
    """
    ds = DatasetLike.convert(ds)
    point = PointLike.convert(point)
    indexers = DictLike.convert(indexers) or {}

    lon_lat_indexers = {'lon': point.x, 'lat': point.y}
    tolerance = _get_tolerance(ds, tolerance_default)

    variable_values = {}
    var_names = sorted(ds.data_vars.keys())
    for var_name in var_names:
        if not var_name.endswith('_bnds'):
            variable = ds.data_vars[var_name]
            effective_indexers = {}
            used_dims = {'lat', 'lon'}
            for dim_name, dim_value in indexers.items():
                if dim_name in variable.dims:
                    effective_indexers[dim_name] = dim_value
                    used_dims.add(dim_name)
            if set(variable.dims) == used_dims:
                try:
                    lon_lat_data = variable.sel(**effective_indexers)
                except KeyError:
                    # if there is no exact match for the "additional" dims, skip this variable
                    continue
                try:
                    point_data = lon_lat_data.sel(method='nearest',
                                                  tolerance=tolerance,
                                                  **lon_lat_indexers)
                except KeyError:
                    # if there is no point within the given tolerance, return an empty dict
                    return {}
                if not variable_values:
                    variable_values['lat'] = float(point_data.lat)
                    variable_values['lon'] = float(point_data.lon)
                value = to_scalar(point_data.values, ndigits=3)
                if value is not UNDEFINED:
                    variable_values[var_name] = value
    return variable_values


def _get_tolerance(ds: xr.Dataset, tolerance_default: float):
    lon_res_attr_name = 'geospatial_lon_resolution'
    lat_res_attr_name = 'geospatial_lat_resolution'
    if lat_res_attr_name in ds.attrs and lon_res_attr_name in ds.attrs:
        lon_res = ds.attrs[lon_res_attr_name]
        lat_res = ds.attrs[lat_res_attr_name]
    else:
        try:
            lon_res = get_geo_spatial_cf_attrs_from_var(ds, 'lon')[lon_res_attr_name]
            lat_res = get_geo_spatial_cf_attrs_from_var(ds, 'lat')[lat_res_attr_name]
        except ValueError:
            return tolerance_default
    if isinstance(lon_res, str) and lon_res.find(' ') > 0:
        lon_res = lon_res.split(' ')[0]
    if isinstance(lat_res, str) and lat_res.find(' ') > 0:
        lat_res = lat_res.split(' ')[0]
    try:
        return (float(lon_res) + float(lat_res)) / 2
    except ValueError:
        return tolerance_default
