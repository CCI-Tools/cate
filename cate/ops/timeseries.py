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

Simple time-series extraction operation.

Functions
=========
"""

import xarray as xr

from cate.core.op import op_input, op
from cate.ops.select import select_var


@op(tags=['timeseries', 'temporal', 'point'])
@op_input('lat', units='degrees', value_range=[-90, 90])
@op_input('lon', units='degrees', value_range=[-180, 180])
@op_input('method', value_set=['nearest', 'ffill', 'bfill'])
# TODO (Gailis, 27.09.16) See issues #45 and #46
# def tseries_point(ds: xr.Dataset,
#                   lat: float,
#                   lon: float,
#                   var: Union[str, List[str],
#                   None], method: str = 'nearest') -> xr.Dataset:
def tseries_point(ds: xr.Dataset,
                  lat: float,
                  lon: float,
                  var: str = None,
                  method: str = 'nearest') -> xr.Dataset:
    """
    Extract time-series from *ds* at given *lat*, *lon* position using
    interpolation *method* for each *var* given in a comma separated list of
    variables.

    The operation returns a new timeseries dataset, that contains the point
    timeseries for all required variables with original variable
    meta-information preserved.

    If a variable has more than three dimensions, the resulting timeseries
    variable will preserve all other dimensions except for lat/lon.

    :param ds: The dataset from which to perform timeseries extraction.
    :param lat: Latitude of the point to extract.
    :param lon: Longitude of the point to extract.
    :param var: Variable(s) for which to perform the timeseries selection
                if none is given, all variables in the dataset will be used.
    :param method: Interpolation method to use.
    :return: A timeseries dataset
    """
    if not var:
        var = '*'

    retset = select_var(ds, var=var)
    indexers = {'lat': lat, 'lon': lon}
    return retset.sel(method=method, **indexers)


@op(tags=['timeseries', 'temporal', 'aggregate', 'mean'])
# TODO (Gailis, 27.09.16) See issues #45 and #46
# def timeseries_mean(ds: xr.Dataset,
#           var: Union[None, str, List[str]] = None) -> xr.Dataset:
def tseries_mean(ds: xr.Dataset,
                 var: str,
                 std_suffix: str = '_std',
                 calculate_std: bool = True) -> xr.Dataset:
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
    :return: Dataset with timeseries variables
    """
    if not var:
        var = '*'

    retset = select_var(ds, var)
    names = retset.data_vars.keys()

    for name in names:
        dims = list(ds[name].dims)
        dims.remove('time')
        retset[name] = retset[name].mean(dim=dims, keep_attrs=True)
        retset[name].attrs['Cate_Description'] = 'Mean aggregated over {} at each point in time.'.format(dims)
        std_name = name+std_suffix
        retset[std_name] = ds[name].std(dim=dims)
        retset[std_name].attrs['Cate_Description'] = 'Accompanying std values for variable \'{}\''.format(name)

    return retset
