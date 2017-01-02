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

    with monitor.starting('Long Term Average', total_work=total_work):
        monitor.progress(work=0)
        # Download and process the datasource year by year
        year = year_min
        step = total_work*0.9/n_years
        while year != year_max+1:
            # Download the dataset
            tmin = "{}-01-01".format(year)
            tmax = "{}-12-31".format(year)
            # If daily dataset, has to be converted to monthly first
            ds = open_dataset(source, tmin, tmax, sync=True,
                              monitor=monitor.child(work=step*0.9))

            # Filter the dataset
            ds = select_var(ds, var)

            try:
                res = res + ds/n_years
            except TypeError:
                raise TypeError('One or more data arrays feature a dtype that\
                                can not be divided. Consider using the var\
                                parameter to filter the dataset.')

            # delete data for the current year, if it should be deleted and it
            # was not already downloaded.
            if not save:
                # Delete data
                pass

            monitor.progress(work=step*0.1)

            year = year + 1

        monitor.progress(msg='Save the LTA dataset')
        save_dataset(res, file)
        monitor.progress(total_work*0.1)
    return res


@op(tags=['temporal', 'mean', 'aggregation'])
@op_input('level', value_set=['monthly'])
@op_input('method', value_set=['mean'])
def temporal_agg(ds: xr.Dataset,
                 level: str,
                 method: str) -> xr.Dataset:
    """
    Perform temporal aggregation to the given level using the given method.

    :param ds: Dataset to aggregate
    :param level: Aggregation level
    :param method: Aggregation method
    """
    pass
