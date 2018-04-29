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
from cate.core.types import VarNamesLike, DatasetLike, ValidationError, DimNamesLike

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
    :param var: If given, only these variables will be preserved in the resulting dataset
    :param monitor: A progress monitor
    :return: A climatological long term average dataset
    """
    ds = DatasetLike.convert(ds)
    # Check if time dtype is what we want
    if 'datetime64[ns]' != ds.time.dtype:
        raise ValidationError('Long term average operation expects a dataset with the'
                              ' time coordinate of type datetime64[ns], but received'
                              ' {}. Running the normalize operation on this'
                              ' dataset may help'.format(ds.time.dtype))

    # Check if we have a monthly dataset
    try:
        if ds.attrs['time_coverage_resolution'] != 'P1M':
            raise ValidationError('Long term average operation expects a monthly dataset'
                                  ' running temporal aggregation on this dataset'
                                  ' beforehand may help.')
    except KeyError:
        raise ValidationError('Could not determine temporal resolution. Running'
                              ' the adjust_temporal_attrs operation beforehand may'
                              ' help.')

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
@op_input('ds', data_type=DatasetLike)
@op_input('method', value_set=['mean', 'max', 'median', 'prod', 'sum', 'std',
                               'var', 'argmax', 'argmin', 'first', 'last'])
@op_input('output_resolution', value_set=['month', 'season'])
@op_return(add_history=True)
def temporal_aggregation(ds: DatasetLike.TYPE,
                         method: str = 'mean',
                         output_resolution: str = 'month',
                         custom_resolution: str = None,
                         monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Perform monthly aggregation of a daily dataset according to the given
    method and output resolution.

    Note that the operation does not perform weighting. Depending on the
    combination of input and output resolutions, as well as aggregation
    method, the resulting dataset might yield unexpected results.

    Resolution 'month' will result in a monthly dataset with each month
    denoted by its first date. Resolution 'season' will result in a dataset
    aggregated to DJF, MAM, JJA, SON seasons, each denoted by the first
    date of the season.

    The operation also works with custom resolution strings, see:
    http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases
    If ``custom_resolution`` is provided, it will override ``output_resolution``.

    Some examples:
      'QS-JUN' produces an output dataset on a quarterly resolution where the
      year ends in June and each quarter is denoted by its first date
      '8MS' produces an output dataset on a five-month resolution where each
      period is denoted by the first date. Note that such periods will not be
      consistent over years.
      '8D' produces a dataset on an eight day resolution

    :param ds: Dataset to aggregate
    :param method: Aggregation method
    :param output_resolution: Desired temporal resolution of the output dataset
    :param custom_resolution: A custom temporal resolution, overrides output_resolution
    :return: Aggregated dataset
    """
    ds = DatasetLike.convert(ds)
    # Check if time dtype is what we want
    if 'datetime64[ns]' != ds.time.dtype:
        raise ValidationError('Temporal aggregation operation expects a dataset with the'
                              ' time coordinate of type datetime64[ns], but received'
                              ' {}. Running the normalize operation on this'
                              ' dataset may help'.format(ds.time.dtype))

    # Try to figure out the input frequency
    try:
        in_freq = ds.attrs['time_coverage_resolution']
    except KeyError:
        raise ValidationError('Could not determine temporal resolution of input dataset.'
                              ' Running the adjust_temporal_attrs operation beforehand may'
                              ' help.')

    if custom_resolution:
        freq = custom_resolution
    else:
        frequencies = {'month': 'MS', 'season': 'QS-NOV'}
        freq = frequencies[output_resolution]

    _validate_freq(in_freq, freq)

    with monitor.observing("resample dataset"):
        retset = ds.resample(freq=freq, dim='time', keep_attrs=True, how=method)

    for var in retset.data_vars:
        try:
            retset[var].attrs['cell_methods'] = \
                retset[var].attrs['cell_methods'] + \
                ' time: {} within years'.format(method)
        except KeyError:
            retset[var].attrs['cell_methods'] = 'time: {} within years'.format(method)

    return adjust_temporal_attrs(retset)


def _validate_freq(in_res: str, out_res: str) -> None:
    """
    Validate the aggregation step

    See also: `ISO 8601 Durations <https://en.wikipedia.org/wiki/ISO_8601#Durations>`_
    """
    # Validate output frequency as a valid offset string
    try:
        dates = pd.date_range('2000-01-01', periods=5, freq=out_res)
    except ValueError:
        raise ValidationError('Invalid custom resolution: {}.'
                              ' Please check operation documentation.'.format(out_res))

    # Assuming simple ISO_8601 periods: PXXD/M
    try:
        count = int(in_res[1:-1])
    except ValueError:
        raise ValidationError('Could not interpret time coverage resolution of'
                              ' the given dataset: {}'.format(in_res))

    if in_res == 'P1M' and out_res == 'MS':
        raise ValidationError('Input dataset is already at the requested output resolution.'
                              'Execution stopped.')

    in_delta = pd.Timedelta(count, unit=in_res[-1])
    out_delta = dates[1] - dates[0]

    if out_delta < in_delta:
        raise ValidationError('Requested output resolution is smaller than dataset resolution.'
                              ' This operation only performs aggregation to larger resolutions.')
    elif out_delta == in_delta:
        raise ValidationError('Input dataset is already at the requested output resolution.'
                              'Execution stopped.')

    return


@op(tags=['aggregate'], version='1.0')
@op_input('ds', data_type=DatasetLike)
@op_input('var', data_type=VarNamesLike, value_set_source='ds')
@op_input('dim', data_type=DimNamesLike, value_set_source='ds')
@op_input('method', value_set=['mean', 'min', 'max', 'sum', 'median'])
@op_return(add_history=True)
def reduce(ds: DatasetLike.TYPE,
           var: VarNamesLike.TYPE = None,
           dim: DimNamesLike.TYPE = None,
           method: str = 'mean',
           monitor: Monitor = Monitor.NONE):
    """
    Reduce the given variables of the given dataset along the given dimensions.
    If no variables are given, all variables of the dataset will be reduced. If
    no dimensions are given, all dimensions will be reduced. If no variables
    have been given explicitly, it can be set that only variables featuring numeric
    values should be reduced.

    :param ds: Dataset to reduce
    :param var: Variables in the dataset to reduce
    :param dim: Dataset dimensions along which to reduce
    :param method: reduction method
    :param monitor: A progress monitor
    """
    ufuncs = {'min': np.nanmin, 'max': np.nanmax, 'mean': np.nanmean,
              'median': np.nanmedian, 'sum': np.nansum}

    if not var:
        var = list(ds.data_vars.keys())
    var_names = VarNamesLike.convert(var)

    if not dim:
        dim = list(ds.coords.keys())
    else:
        dim = DimNamesLike.convert(dim)

    retset = ds.copy()

    for var_name in var_names:
        intersection = [value for value in dim if value in retset[var_name].dims]
        with monitor.starting("Reduce dataset", total_work=100):
            monitor.progress(5)
            with monitor.child(95).observing("Reduce"):
                retset[var_name] = retset[var_name].reduce(ufuncs[method],
                                                           dim=intersection,
                                                           keep_attrs=True)

    return retset
