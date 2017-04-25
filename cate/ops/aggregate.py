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

This module provides aggregation operations

Components
==========
"""

import xarray as xr

from cate.core.op import op, op_input
from cate.ops.select import select_var
from cate.util.monitor import Monitor
from cate.core.types import VarNamesLike


@op(tags=['aggregate'])
@op_input('var', data_type=VarNamesLike)
def long_term_average(ds: xr.Dataset,
                      var: VarNamesLike.TYPE = None,
                      monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Perform long term average of the given dataset by doing a mean of monthly
    values over the time range covered by the dataset. E.g. it averages all
    January values, all February values, etc, to create a dataset with twelve
    time slices each containing a mean of respective monthly values.

    For further information on climatological datasets, see
    http://cfconventions.org/cf-conventions/v1.6.0/cf-conventions.html#climatological-statistics

    :param ds: A monthly dataset to average
    :param var: If given, only these variables will be preserved in the
    resulting dataset
    :param monitor: A progress monitor
    :return: A climatological long term average dataset
    """
    # Check if time dtype is what we want
    if 'datetime64[ns]' != ds.time.dtype:
        raise ValueError('Long term average operation expects a dataset with the'
                         ' time coordinate of type datetime64[ns], but received'
                         ' {}. Running the harmonization operation on this'
                         ' dataset may help'.format(ds.time.dtype))

    # Check if we have a monthly dataset
    months = ds.time['time.month'].values
    if (months[1] - months[0]) != 1:
        raise ValueError('Long term average operation expects a monthly dataset'
                         'running temporal aggregation on this dataset'
                         'beforehand may help.')

    var = VarNamesLike.convert(var)
    ds = select_var(ds, var)

    total_work = 100

    with monitor.starting('LTA', total_work=total_work):
        monitor.progress(work=0)
        step = total_work / 12
        kwargs = {'dim': 'time', 'keep_attrs': True,
                  'monitor': monitor, 'step': step}
        retset = ds.groupby('time.month', squeeze=False).apply(_mean, **kwargs)
    retset = retset.rename({'month': 'time'})
    return retset


def _mean(x, dim, keep_attrs, monitor, step):
    retset = x.mean(dim=dim, keep_attrs=keep_attrs)
    monitor.progress(work=step)
    return retset


@op(tags=['aggregate'])
@op_input('method', value_set=['mean', 'max'])
def temporal_aggregation(ds: xr.Dataset,
                         method: str = 'mean') -> xr.Dataset:
    """
    Perform monthly aggregation of a daily dataset according to the given
    method.

    :param ds: Dataset to aggregate
    :param method: Aggregation method
    :return: Aggregated dataset
    """
    # Check if time dtype is what we want
    if 'datetime64[ns]' != ds.time.dtype:
        raise ValueError('Temporal aggregation operation expects a dataset with the'
                         ' time coordinate of type datetime64[ns], but received'
                         ' {}. Running the harmonization operation on this'
                         ' dataset may help'.format(ds.time.dtype))

    # Check if we have a daily dataset
    days = ds.time['time.day'].values
    if (days[1] - days[0]) != 1:
        raise ValueError('Long term average operation expects a daily dataset')
