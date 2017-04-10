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

Dataset harmonization operation.

Components
==========
"""

import xarray as xr

from jdcal import jd2gcal, MJD_0
from datetime import datetime

from cate.core.op import op
from cate.core.cdm import get_lon_dim_name, get_lat_dim_name


@op(tags=['harmonize'])
def harmonize(ds: xr.Dataset) -> xr.Dataset:
    """
    Harmonize the given dataset. Rename latitude and longitude names to
    lat and lon, if it is not already the case. Decode time if neccessary

    :param ds: The dataset to harmonize
    :return: The harmonized dataset
    """
    lat_name = get_lat_dim_name(ds)
    lon_name = get_lon_dim_name(ds)

    name_dict = dict()
    if lat_name:
        name_dict[lat_name] = 'lat'

    if lon_name:
        name_dict[lon_name] = 'lon'

    ds = ds.rename(name_dict)

    # Handle Julian Day Time
    try:
        if ds.time.long_name.lower().strip() == 'time in julian days':
            return _jd2datetime(ds)
    except AttributeError:
        pass

    return ds


def _jd2datetime(ds: xr.Dataset) -> xr.Dataset:
    """
    Convert the time dimension of the given dataset from Julian date to
    datetime.

    :param ds: Dataset on which to run conversion
    """
    ds = ds.copy()
    # Decode JD time
    tuples = [jd2gcal(x-MJD_0, MJD_0) for x in ds.time.values]
    # Replace JD time with datetime
    ds.time.values = [datetime(x[0], x[1], x[2]) for x in tuples]
    # Adjust attributes
    ds.time.attrs['long_name'] = 'time'
    ds.time.attrs['calendar'] = 'standard'

    return ds
