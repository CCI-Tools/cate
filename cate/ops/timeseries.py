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

Simple time-series extraction operation.

Functions
=========
"""

import xarray as xr

from cate.core.op import op_input, op, op_return
from cate.ops.select import select_var
from cate.core.types import VarNamesLike, PointLike
from cate.util import Monitor


@op(tags=['timeseries', 'temporal', 'filter', 'point'], version='1.0')
@op_input('point', data_type=PointLike)
@op_input('method', value_set=['nearest', 'ffill', 'bfill'])
@op_input('var', value_set_source='ds', data_type=VarNamesLike)
@op_return(add_history=True)
def tseries_point(ds: xr.Dataset,
                  point: PointLike.TYPE,
                  var: VarNamesLike.TYPE = None,
                  method: str = 'nearest') -> xr.Dataset:
    """
    Extract time-series from *ds* at given *lon*, *lat* position using
    interpolation *method* for each *var* given in a comma separated list of
    variables.

    The operation returns a new timeseries dataset, that contains the point
    timeseries for all required variables with original variable
    meta-information preserved.

    If a variable has more than three dimensions, the resulting timeseries
    variable will preserve all other dimensions except for lon/lat.

    :param ds: The dataset from which to perform timeseries extraction.
    :param point: Point to extract, e.g. (lon,lat)
    :param var: Variable(s) for which to perform the timeseries selection
                if none is given, all variables in the dataset will be used.
    :param method: Interpolation method to use.
    :return: A timeseries dataset
    """
    point = PointLike.convert(point)
    lon = point.x
    lat = point.y

    if not var:
        var = '*'

    retset = select_var(ds, var=var)
    indexers = {'lat': lat, 'lon': lon}
    retset = retset.sel(method=method, **indexers)

    # The dataset is no longer a spatial dataset -> drop associated global
    # attributes
    drop = ['geospatial_bounds_crs', 'geospatial_bounds_vertical_crs',
            'geospatial_vertical_min', 'geospatial_vertical_max',
            'geospatial_vertical_positive', 'geospatial_vertical_units',
            'geospatial_vertical_resolution', 'geospatial_lon_min',
            'geospatial_lat_min', 'geospatial_lon_max', 'geospatial_lat_max']

    for key in drop:
        retset.attrs.pop(key, None)

    return retset


@op(tags=['timeseries', 'temporal'], version='1.0')
@op_input('ds')
@op_input('var', value_set_source='ds', data_type=VarNamesLike)
@op_return(add_history=True)
def tseries_mean(ds: xr.Dataset,
                 var: VarNamesLike.TYPE,
                 std_suffix: str = '_std',
                 calculate_std: bool = True,
                 monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Extract spatial mean timeseries of the provided variables, return the
    dataset that in addition to all the information in the given dataset
    contains also timeseries data for the provided variables, following
    naming convention 'var_name1_ts_mean'

    If a data variable with more dimensions than time/lat/lon is provided,
    the data will be reduced by taking the mean of all data values at a single
    time position resulting in one dimensional timeseries data variable.

    :param ds: The dataset from which to perform timeseries extraction.
    :param var: Variables for which to perform timeseries extraction
    :param calculate_std: Whether to calculate std in addition to mean
    :param std_suffix: Std suffix to use for resulting datasets, if std is calculated.
    :param monitor: a progress monitor.
    :return: Dataset with timeseries variables
    """
    if not var:
        var = '*'

    retset = select_var(ds, var)
    names = retset.data_vars.keys()

    with monitor.starting("Calculate mean", total_work=len(names)):
        for name in names:
            dims = list(ds[name].dims)
            dims.remove('time')
            with monitor.observing("Calculate mean"):
                retset[name] = retset[name].mean(dim=dims, keep_attrs=True)
            retset[name].attrs['Cate_Description'] = 'Mean aggregated over {} at each point in time.'.format(dims)
            std_name = name + std_suffix
            retset[std_name] = ds[name].std(dim=dims)
            retset[std_name].attrs['Cate_Description'] = 'Accompanying std values for variable \'{}\''.format(name)

    return retset
