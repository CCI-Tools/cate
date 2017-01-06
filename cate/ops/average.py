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

This module provides averaging operations.

Components
==========
"""

import xarray as xr

from cate.core.op import op, op_input
from cate.ops import select_var, open_dataset, save_dataset
from cate.core.monitor import Monitor
from cate.core.util import to_datetime_range
from cate.core.ds import DATA_STORE_REGISTRY, query_data_sources


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

    with monitor.starting('LTA', total_work=total_work):
        # Set up the monitor
        monitor.progress(work=0)
        step = total_work*0.9/n_years

        # Process the data source year by year
        year = year_min
        while year != year_max+1:
            source_info = data_source.cache_info

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

            # If daily dataset, has to be converted to monthly first

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
                 start_date: str,
                 end_date: str,
                 var: str = None,
                 level: str = 'mon',
                 method: str = 'mean',
                 save_data: bool = False) -> (xr.Dataset, str):
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
    :return: The aggregated dataset and a local data source identifier
    """
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

    # Check if the data source temporal resolution is known
    known_res = ('day', '8-days', 'mon', 'yr')

    fq = data_source.meta_info['time_frequency']
    if (not fq) or (fq not in known_res):
        raise ValueError("The given data source features unknown time\
                         resolution: {}".format(fq))

    # Check if the operation supports the desired aggregation step
    valid_steps = list()
    valid_steps.append(('day', 'mon'))
    if (fq, level) not in valid_steps:
        raise ValueError("Currently the operation does not support aggregation\
                         from {} to {}".format(fq, level))


    # Set up the monitor

    # Process the data source period by period
        # Determine if the data for the given period are already downloaded

        # If at least one file of the given time range is present, we
        # don't delete the data for this period, we do the syncing anyway

        # Filter the dataset

        # Do the aggregation

        # Save the dataset for this period into local data store

        # Close and delete the files if needed

    # Tear down the monitor

    # Open the dataset from local data store

    # Return the dataset and local data source id
    return None, None
