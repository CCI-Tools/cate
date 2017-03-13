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

This module provides averaging operations.

Components
==========
"""

from datetime import datetime

import xarray as xr

from cate.core.ds import DATA_STORE_REGISTRY, query_data_sources
from cate.core.op import op, op_input
from cate.ops.select import select_var
from cate.ops.io import save_dataset
from cate.util import to_datetime_range, to_datetime
from cate.util.monitor import Monitor


@op(tags=['temporal', 'mean', 'average', 'long_running'])
def long_term_average(source: str,
                      year_min: int,
                      year_max: int,
                      file: str,
                      var: str = None,
                      save: bool = False,
                      monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Perform the long term monthly average of the given monthly or daily data
    source for the given range of years.

    Depending on the given year range, data size, as well as internet
    connection quality, this operation can potentially take a very long time
    to finish.

    Careful consideration is needed in choosing the var parameter to create
    meaningful outputs. This is unique for each data source.

    :param source: The data source from which to extract the monthly average
    :param year_min: The earliest year of the desired time range
    :param year_max: The most recent year of the desired time range
    :param file: filepath where to save the long term average dataset
    :param var: If given, only these variable names will be preserved in the
    output.
    :param save: If True, saves the data downloaded during this operation. This
    can potentially be a very large amount of data.
    :param monitor: A progress monitor to use
    :return: The Long Term Average dataset.
    """
    n_years = year_max - year_min + 1
    res = 0
    total_work = 100

    # Select the appropriate data source
    data_store_list = DATA_STORE_REGISTRY.get_data_stores()
    data_sources = query_data_sources(data_store_list, name=source)
    if len(data_sources) == 0:
        raise ValueError("No data_source found for the given query\
                         term {}".format(source))
    elif len(data_sources) > 1:
        raise ValueError("{} data_sources found for the given query\
                         term {}".format(data_sources, source))

    data_source = data_sources[0]
    source_info = data_source.cache_info

    # Check if we have a monthly data source
    fq = data_source.meta_info['time_frequency']
    if fq != 'mon':
        raise ValueError("Only monthly datasets are supported for time being.")

    with monitor.starting('LTA', total_work=total_work):
        # Set up the monitor
        monitor.progress(work=0)
        step = total_work*0.9/n_years

        # Process the data source year by year
        year = year_min
        while year != year_max+1:

            tmin = "{}-01-01".format(year)
            tmax = "{}-12-31".format(year)

            # Determine if the data for the given year are already downloaded
            # If at least one file of the given time range is present, we
            # don't delete the data for this year, we do the syncing anyway.
            was_already_downloaded = False
            dt_range = to_datetime_range(tmin, tmax)
            for date in source_info:
                if dt_range[0] <= date <= dt_range[1]:
                    was_already_downloaded = True
                    # One is enough
                    break

            worked = monitor._worked
            data_source.sync(dt_range, monitor=monitor.child(work=step*0.9))
            if worked == monitor._worked:
                monitor.progress(work=step*0.9)

            ds = data_source.open_dataset(dt_range)

            # Filter the dataset
            ds = select_var(ds, var)

            try:
                if res == 0:
                    res = ds/n_years
                else:
                    # Xarray doesn't do automatic alignment for in place
                    # operations, hence we have to do it manually
                    res = res + ds.reindex_like(res)/n_years
            except TypeError:
                raise TypeError('One or more data arrays feature a dtype that\
                                can not be divided. Consider using the var\
                                parameter to filter the dataset.')

            ds.close()
            # delete data for the current year, if it should be deleted and it
            # was not already downloaded.
            if (not save) and (not was_already_downloaded):
                data_source.delete_local(dt_range)

            monitor.progress(work=step*0.1)

            year = year + 1

        monitor.progress(msg='Saving the LTA dataset')
        save_dataset(res, file)
        monitor.progress(total_work*0.1)

    return res


@op(tags=['temporal', 'mean', 'aggregation', 'long_running'])
@op_input('level', value_set=['mon'])
@op_input('method', value_set=['mean'])
def temporal_agg(source: str,
                 start_date: str = None,
                 end_date: str = None,
                 var: str = None,
                 level: str = 'mon',
                 method: str = 'mean',
                 save_data: bool = False,
                 monitor: Monitor = Monitor.NONE) -> (xr.Dataset, str):
    """
    Perform temporal aggregation of the given data source to the given level
    using the given method for the given time range. Only full time periods
    of the given time range will be aggregated.

    Depending on the given time range, data size, as well as internet
    connection quality, this operation can potentially take a very long time
    to finish.

    Careful consideration is needed in choosing the var parameter to create
    meaningful outputs. This is unique for each data source.

    The aggregation result is saved into the local data store for later reuse.

    :param source: Data source to aggregate
    :param start_date: Start date of aggregation. If not given, data source
    start date is used instead
    :param end_date: End date of aggregation. If not given, data source end
    date is used instead
    :param var: If given, only these dataset variables will be preserved in the
    result
    :param level: Aggregation level
    :param method: Aggregation method
    :param save_data: Whether to save data downloaded during this operation.
    This can potentially be a lot of data.
    :param monitor: A progress monitor to use
    :return: The local data source identifier for the aggregated data
    """
    # Raise not implemented, while not finished
    raise ValueError("Operation is not implemented.")

    # Select the appropriate data source
    data_store_list = DATA_STORE_REGISTRY.get_data_stores()
    data_sources = query_data_sources(data_store_list, name=source)
    if len(data_sources) == 0:
        raise ValueError("No data_source found for the given query "
                         "term {}".format(source))
    elif len(data_sources) > 1:
        raise ValueError("{} data_sources found for the given query "
                         "term {}".format(data_sources, source))

    data_source = data_sources[0]
    source_info = data_source.cache_info

    # We have to do this to have temporal coverage info in meta_info
    data_source._init_file_list()

    # Check if the data source temporal resolution is known
    known_res = ('day', '8-days', 'mon', 'yr')

    fq = data_source.meta_info['time_frequency']
    if (not fq) or (fq not in known_res):
        raise ValueError("The given data source features unknown time "
                         "resolution: {}".format(fq))

    # Check if the operation supports the desired aggregation step
    valid_steps = list()
    valid_steps.append(('day', 'mon'))
    if (fq, level) not in valid_steps:
        raise ValueError("Currently the operation does not support aggregation"
                         " from {} to {}".format(fq, level))

    # Determine start and end dates
    if not start_date:
        start_date = data_source.meta_info['temporal_coverage_start']
    start_date = to_datetime(start_date)
    # If start_date is not start of the month, move it to the 1st of next
    # month
    if start_date.day != 1:
        try:
            start_date = datetime(start_date.year, start_date.month+1, 1)
        except ValueError:
            # We have tried to set the month to 13
            start_date = datetime(start_date.year+1, 1, 1)

    if not end_date:
        end_date = data_source.meta_info['temporal_coverage_end']
    end_date = to_datetime(end_date)
    # If end date is not end of the month, move it to the last day of the
    # previous month
    if not _is_end_of_month(end_date):
        try:
            end_date = datetime(end_date.year, end_date.month-1, 27)
        except ValueError:
            # We have tried to set the month to 0
            end_date = datetime(end_date.year-1, 12, 31)

    end_date = _end_of_month(end_date.year, end_date.month)

    # Determine the count of processing periods
    n_periods = (end_date.year - start_date.year + 1) * 12\
        + end_date.month - start_date.month - 11
    # 2000-4-1, 2000-6-30 -> 12 + 2 -11 = 3

    if n_periods < 1:
        raise ValueError("The given time range does not contain any full "
                         "calendar months to do aggregation with.")

    # Set up the monitor
    total_work = 100
    with monitor.starting('Aggregate', total_work=total_work):
        monitor.progress(work=0)
        step = total_work*0.9/n_periods

        # Process the data source period by period
        tmin = start_date
        while tmin < end_date:
            tmax = _end_of_month(tmin.year, tmin.month)

            # Determine if the data for the given period are already downloaded
            # If at least one file of the given time range is present, we
            # don't delete the data for this period, we do the syncing anyway
            was_already_downloaded = False
            dt_range = to_datetime_range(tmin, tmax)
            for date in source_info:
                if dt_range[0] <= date <= dt_range[1]:
                    was_already_downloaded = True
                    # One is enough
                    break

            worked = monitor._worked
            data_source.sync(dt_range, monitor=monitor.child(work=step*0.9))
            if worked == monitor._worked:
                monitor.progress(work=step*0.9)

            ds = data_source.open_dataset(dt_range)

            # Filter the dataset
            ds = select_var(ds, var)

            # Do the aggregation

            # Save the dataset for this period into local data store

            # Close and delete the files if needed
            ds.close()
            # delete data for the current period,if it should be deleted and it
            # was not already downloaded.
            if (not save_data) and (not was_already_downloaded):
                data_source.delete_local(dt_range)

            monitor.progress(work=step*0.1)

            # tmin for next iteration
            try:
                tmin = datetime(tmin.year, tmin.month+1, 1)
            except ValueError:
                # Couldn't add a month -> end of year
                tmin = datetime(tmin.year+1, 1, 1)
            pass

    monitor.progress(work=step*0.1)

    # Return the local data source id
    return None


def _is_end_of_month(date: datetime) -> bool:
    """
    Returns a boolean denoting whether the given date is the last day of
    the month.

    :param date: A datetime date
    :return: Whether this is the last day of the month
    """
    try:
        datetime(date.year, date.month, date.day+1)
        return False
    except ValueError:
        # Couldn't add a day -> end of month
        return True


def _end_of_month(year: int, month: int) -> datetime:
    """
    Given a year and a month, returns a date of the last day of the given month

    :param year: Year
    :param month: Month
    :return: Last date of the year/month combination
    """
    # If an invalid month is given, datetime will raise the exception
    date = datetime(year, month, 28)
    for day in range(28, 32):
        date = datetime(year, month, day)
        if _is_end_of_month(date):
            return date
