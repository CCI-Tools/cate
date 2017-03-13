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

Anomaly calculation operations

Functions
=========
"""

from cate.core.op import op
from cate.ops.subset import subset_spatial, subset_temporal
import xarray as xr


@op(tags=['anomaly', 'climatology'])
def anomaly_climatology(ds: xr.Dataset,
                        file: str) -> xr.Dataset:
    """
    Calculate anomaly with a given climatology as reference data. The given
    climatology dataset is expected to consist of 12 time slices and contain
    the same number of data arrays with the same names as for data arrays in
    the given dataset.

    The calculated anomaly will be against the corresponding month of the
    reference data. E.g. January against January, etc.

    :param ds: The dataset to calculate anomalies from
    :param file: Path to reference data file
    :return: The anomaly dataset
    """
    clim = xr.open_dataset(file)
    return ds - clim


@op(tags=['anomaly'])
def anomaly_internal(ds: xr.Dataset,
                     start_date: str = None,
                     end_date: str = None,
                     lat_min: float = None,
                     lat_max: float = None,
                     lon_min: float = None,
                     lon_max: float = None) -> xr.Dataset:
    """
    Calculate anomaly using as reference data an optional region and time slice
    from the given dataset. If no time slice/spatial region is given, the
    operation will calculate anomaly using the mean of the whole dataset
    as the reference.

    This is done for each data array in the dataset.
    :param ds: The dataset to calculate anomalies from
    :param start_date: Reference data start date
    :param end_date: Reference data end date
    :param lat_min: Reference data minimum latitude
    :param lat_max: Reference data maximum latitude
    :param lon_min: Reference data minimum latitude
    :param lon_max: Reference data maximum latitude
    :return: The anomaly dataset
    """
    ref = ds
    if start_date and end_date:
        ref = subset_temporal(ref, start_date, end_date)
    if lat_min and lat_max and lon_min and lon_max:
        ref = subset_spatial(ref, lat_min, lat_max, lon_min, lon_max)
    ref = ref.mean(keep_attrs=True, skipna=True)
    return ds - ref
