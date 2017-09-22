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
import pandas as pd
import numpy as np

from cate.core.op import op, op_input, op_return
from cate.ops.select import select_var
from cate.util.monitor import Monitor
from cate.core.types import VarNamesLike, DatasetLike

from cate.ops.normalize import adjust_temporal_attrs


@op(tags=['aggregate', 'temporal'], version='1.0')
@op_input('ds', data_type=DatasetLike)
@op_input('var', value_set_source='ds', data_type=VarNamesLike)
@op_return(add_history=True)
def long_term_average(ds: DatasetLike.TYPE,
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
    ds = DatasetLike.convert(ds)
    # Check if time dtype is what we want
    if 'datetime64[ns]' != ds.time.dtype:
        raise ValueError('Long term average operation expects a dataset with the'
                         ' time coordinate of type datetime64[ns], but received'
                         ' {}. Running the normalize operation on this'
                         ' dataset may help'.format(ds.time.dtype))

    # Check if we have a monthly dataset
    try:
        if ds.attrs['time_coverage_resolution'] != 'P1M':
            raise ValueError('Long term average operation expects a monthly dataset'
                             ' running temporal aggregation on this dataset'
                             ' beforehand may help.')
    except KeyError:
        raise ValueError('Could not determine temporal resolution. Running the'
                         ' normalize operation may help.')

    var = VarNamesLike.convert(var)
    # Shallow
    retset = ds.copy()
    if var:
        retset = select_var(retset, var)

    time_min = pd.Timestamp(ds.time.values[0])
    time_max = pd.Timestamp(ds.time.values[-1])

    total_work = 100

    with monitor.starting('LTA', total_work=total_work):
        monitor.progress(work=0)
        step = total_work / 12
        kwargs = {'monitor': monitor, 'step': step}
        retset = retset.groupby('time.month', squeeze=False).apply(_mean, **kwargs)

    # Make the return dataset CF compliant
    retset = retset.rename({'month': 'time'})
    retset['time'] = pd.date_range('{}-01-01'.format(time_min.year),
                                   freq='MS',
                                   periods=12)

    climatology_bounds = xr.DataArray(data=np.tile([time_min, time_max],
                                                   (12, 1)),
                                      dims=['time', 'nv'],
                                      name='climatology_bounds')
    retset['climatology_bounds'] = climatology_bounds
    retset.time.attrs = ds.time.attrs
    retset.time.attrs['climatology'] = 'climatology_bounds'

    for var in retset.data_vars:
        try:
            retset[var].attrs['cell_methods'] = \
                    retset[var].attrs['cell_methods'] + ' time: mean over years'
        except KeyError:
            retset[var].attrs['cell_methods'] = 'time: mean over years'

    return retset


def _mean(ds: xr.Dataset, monitor: Monitor, step: float):
    """
    Calculate mean of the given dataset and update the given monitor.

    :param ds: Dataset to take the mean of
    :param monitor: Monitor to update
    :param step: Work step
    """
    retset = ds.mean(dim='time', keep_attrs=True)
    monitor.progress(work=step)
    return retset


@op(tags=['aggregate', 'temporal'], version='1.1')
@op_input('method', value_set=['mean', 'max', 'median', 'prod', 'sum', 'std',
                               'var', 'argmax', 'argmin', 'first', 'last'])
@op_input('ds', data_type=DatasetLike)
@op_return(add_history=True)
def temporal_aggregation(ds: DatasetLike.TYPE,
                         method: str = 'mean',
                         monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Perform monthly aggregation of a daily dataset according to the given
    method.

    :param ds: Dataset to aggregate
    :param method: Aggregation method
    :return: Aggregated dataset
    """
    ds = DatasetLike.convert(ds)
    # Check if time dtype is what we want
    if 'datetime64[ns]' != ds.time.dtype:
        raise ValueError('Temporal aggregation operation expects a dataset with the'
                         ' time coordinate of type datetime64[ns], but received'
                         ' {}. Running the normalize operation on this'
                         ' dataset may help'.format(ds.time.dtype))

    # Check if we have a daily dataset
    try:
        if ds.attrs['time_coverage_resolution'] != 'P1D':
            raise ValueError('Temporal aggregation operation expects a daily dataset')
    except KeyError:
        raise ValueError('Could not determine temporal resolution. Running'
                         ' the normalize operation beforehand may help.')

    with monitor.observing("resample dataset"):
        retset = ds.resample(freq='MS', dim='time', keep_attrs=True, how=method)

    for var in retset.data_vars:
        try:
            retset[var].attrs['cell_methods'] = \
                    retset[var].attrs['cell_methods'] + \
                    ' time: {} within years'.format(method)
        except KeyError:
            retset[var].attrs['cell_methods'] = 'time: {} within years'.format(method)

    return adjust_temporal_attrs(retset)
