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
from datetime import timezone

import numpy as np
import pandas as pd
import xarray as xr
from xarray.core.resample import DatasetResample as resampler

from cate.core.op import op, op_input, op_return
from cate.core.types import VarNamesLike, DatasetLike, ValidationError, DimNamesLike
from cate.ops.normalize import adjust_temporal_attrs
from cate.ops.select import select_var
from cate.util.monitor import Monitor


@op(tags=['aggregate', 'temporal'], version='1.5')
@op_input('ds', data_type=DatasetLike)
@op_input('var', value_set_source='ds', data_type=VarNamesLike)
@op_return(add_history=True)
def long_term_average(ds: DatasetLike.TYPE,
                      var: VarNamesLike.TYPE = None,
                      monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Create a 'mean over years' dataset by averaging the values of the given input
    dataset over all years. The output is a climatological dataset with the same
    resolution as the input dataset. E.g. a daily input dataset will create a daily
    climatology consisting of 365 days, a monthly input dataset will create a monthly
    climatology, etc.

    Seasonal input datasets must have matching seasons over all years denoted by the
    same date each year. E.g., first date of each quarter. The output dataset will
    then be a seasonal climatology where each season is denoted with the same date
    as in the input dataset.

    For further information on climatological datasets, see
    http://cfconventions.org/cf-conventions/v1.6.0/cf-conventions.html#climatological-statistics

    :param ds: A dataset to average
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

    try:
        t_resolution = ds.attrs['time_coverage_resolution']
    except KeyError:
        raise ValidationError('Could not determine temporal resolution. Running'
                              ' the adjust_temporal_attrs operation beforehand may'
                              ' help.')

    var = VarNamesLike.convert(var)
    # Shallow

    if var:
        ds = select_var(ds, var)

    if t_resolution == 'P1D':
        return _lta_daily(ds)
    elif t_resolution == 'P1M':
        return _lta_monthly(ds, monitor)
    else:
        return _lta_general(ds, monitor)


def _lta_monthly(ds: xr.Dataset, monitor: Monitor):
    """
    Carry out a long term average on a monthly dataset

    :param ds: Dataset to aggregate
    :param monitor: Progress monitor
    :return: Aggregated dataset
    """
    time_min = pd.Timestamp(ds.time.values[0], tzinfo=timezone.utc)
    time_max = pd.Timestamp(ds.time.values[-1], tzinfo=timezone.utc)
    total_work = 100
    retset = ds

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


def _groupby_day(ds: xr.Dataset, monitor: Monitor, step: float):
    """
    Groupby the given dataset by day of month and apply mean to it

    :param ds: Dataset to aggregate
    :param monitor: Progress monitor
    :param step: Progress step
    """
    kwargs = {'monitor': monitor, 'step': step}
    return ds.groupby('time.day', squeeze=False).apply(_mean, **kwargs)


def _lta_daily(ds: xr.Dataset):
    """
    Carry out a long term average of a daily dataset

    :param ds: Dataset to aggregate
    :return: Aggregated dataset
    """

    retset = ds.groupby('time.dayofyear', squeeze=False).mean('time')

    for var in retset.data_vars:
        try:
            retset[var].attrs['cell_methods'] = \
                retset[var].attrs['cell_methods'] + ' time: mean over years'
        except KeyError:
            retset[var].attrs['cell_methods'] = 'time: mean over years'

    return retset


def _lta_general(ds: xr.Dataset, monitor: Monitor):
    """
    Try to carry out a long term average in a general case, notably
    in the case of having seasonal datasets

    :param ds: Dataset to aggregate
    :param monitor: Progress monitor
    :return: Aggregated dataset
    """
    time_min = pd.Timestamp(ds.time.values[0], tzinfo=timezone.utc)
    time_max = pd.Timestamp(ds.time.values[-1], tzinfo=timezone.utc)
    total_work = 100
    retset = ds

    # The dataset should feature time periods consistent over years
    # and denoted with the same dates each year
    if not _is_seasonal(ds.time):
        raise ValidationError("A long term average dataset can not be created for"
                              " a dataset with inconsistent seasons.")

    # Get 'representative year'
    c = 0
    for group in ds.time.groupby('time.year'):
        c = c + 1
        if c == 1:
            rep_year = group[1].time
            continue
        if c == 2 and len(group[1].time) > len(rep_year):
            rep_year = group[1].time
            break

    with monitor.starting('LTA', total_work=total_work):
        monitor.progress(work=0)
        step = total_work / len(rep_year.time)
        kwargs = {'monitor': monitor, 'step': step}
        retset = retset.groupby('time.month', squeeze=False).apply(_groupby_day, **kwargs)

    # Make the return dataset CF compliant
    retset = retset.stack(time=('month', 'day'))

    # Turn month, day coordinates to time
    retset = retset.reset_index('time')
    retset = retset.drop(['month', 'day'])
    retset['time'] = rep_year.time

    climatology_bounds = xr.DataArray(data=np.tile([time_min, time_max],
                                                   (len(rep_year), 1)),
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


def _is_seasonal(time: xr.DataArray):
    """
    Check if the given timestamp dataarray features consistent
    seasons. E.g. Each year has the same date-month values in it.
    """
    c = 0
    test = None
    for group in time.groupby('time.year'):
        # Test (month, day) dates of all years against
        # (month, day) dates of the first year, or second
        # year in case the first year is not full
        c = c + 1
        np_time = group[1].time.values
        months = pd.DatetimeIndex(np_time).month
        days = pd.DatetimeIndex(np_time).day
        if c == 1:
            first_months = months
            first_days = days
            continue
        elif c == 2:
            second_months = months
            second_days = days
            if len(second_months) > len(first_months):
                test = list(zip(second_months, second_days))
                for date in zip(first_months, first_days):
                    if date not in test:
                        return False
            else:
                test = list(zip(first_months, first_days))
                for date in zip(second_months, second_days):
                    if date not in test:
                        return False
            continue

        for date in zip(months, days):
            if date not in test:
                return False

    return True


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


@op(tags=['aggregate', 'temporal'], version='1.5')
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
    Perform aggregation of dataset according to the given
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
      year ends in 1st of June and each quarter is denoted by its first date
      '8MS' produces an output dataset on an eight-month resolution where each
      period is denoted by the first date. Note that such periods will not be
      consistent over years.
      '8D' produces a dataset on an eight day resolution

    :param ds: Dataset to aggregate
    :param method: Aggregation method
    :param output_resolution: Desired temporal resolution of the output dataset
    :param custom_resolution: Custom temporal resolution, overrides output_resolution
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
        frequencies = {'month': 'MS', 'season': 'QS-DEC'}
        freq = frequencies[output_resolution]

    _validate_freq(in_freq, freq)

    with monitor.observing("resample dataset"):
        try:
            retset = getattr(resampler, method)(ds.resample(time=freq, keep_attrs=True))
        except AttributeError:
            raise ValidationError(f'Provided aggregation method {method} is not valid.')

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
                              ' Execution stopped.')

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
@op_input('var', value_set_source='ds', data_type=VarNamesLike)
@op_input('dim', value_set_source='ds', data_type=DimNamesLike)
@op_input('method', value_set=['mean', 'min', 'max', 'sum', 'median'])
@op_return(add_history=True)
def reduce(ds: DatasetLike.TYPE,
           var: VarNamesLike.TYPE = None,
           dim: DimNamesLike.TYPE = None,
           method: str = 'mean',
           monitor: Monitor = Monitor.NONE) -> xr.Dataset:
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

    ds = DatasetLike.convert(ds)

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
