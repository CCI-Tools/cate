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
import xarray as xr

from cate.core.op import op, op_return, op_input
from cate.util.monitor import Monitor
from cate.ops.subset import subset_spatial, subset_temporal
from cate.ops.arithmetics import diff, ds_arithmetics
from cate.core.types import TimeRangeLike, PolygonLike, ValidationError
from cate.ops.normalize import adjust_spatial_attrs


_ALL_FILE_FILTER = dict(name='All Files', extensions=['*'])


@op(tags=['anomaly'], version='1.0')
@op_input('file', file_open_mode='r', file_filters=[dict(name='NetCDF', extensions=['nc']), _ALL_FILE_FILTER])
@op_return(add_history=True)
def anomaly_external(ds: xr.Dataset,
                     file: str,
                     transform: str = None,
                     monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Calculate anomaly with external reference data, for example, a climatology.
    The given reference dataset is expected to consist of 12 time slices, one
    for each month.

    The returned dataset will contain the variable names found in both - the
    reference and the given dataset. Names found in the given dataset, but not in
    the reference, will be dropped from the resulting dataset. The calculated
    anomaly will be against the corresponding month of the reference data.
    E.g. January against January, etc.

    In case spatial extents differ between the reference and the given dataset,
    the anomaly will be calculated on the intersection.

    :param ds: The dataset to calculate anomalies from
    :param file: Path to reference data file
    :param transform: Apply the given transformation before calculating the anomaly.
                      For supported operations see help on 'ds_arithmetics' operation.
    :param monitor: a progress monitor.
    :return: The anomaly dataset
    """
    # Check if the time coordinate is of dtype datetime
    try:
        if ds.time.dtype != 'datetime64[ns]':
            raise ValidationError('The dataset provided for anomaly calculation'
                                  ' is required to have a time coordinate of'
                                  ' dtype datetime64[ns]. Running the normalize'
                                  ' operation on this dataset might help.')
    except AttributeError:
        raise ValidationError('The dataset provided for anomaly calculation'
                              ' is required to have a time coordinate.')

    clim = xr.open_dataset(file)
    ret = ds.copy()
    if transform:
        ret = ds_arithmetics(ds, transform)
    # Group by months, subtract the appropriate slice from the reference
    # Note that this requires that 'time' coordinate labels are of type
    # datetime64[ns]
    total_work = 100
    step = 100 / 12

    with monitor.starting('Anomaly', total_work=total_work):
        monitor.progress(work=0)
        kwargs = {'ref': clim, 'monitor': monitor, 'step': step}
        ret = ret.groupby(ds['time.month']).apply(_group_anomaly,
                                                  **kwargs)

    # Running groupby results in a redundant 'month' variable being added to
    # the dataset
    ret = ret.drop('month')
    ret.attrs = ds.attrs
    # The dataset may be cropped
    return adjust_spatial_attrs(ret)


def _group_anomaly(group: xr.Dataset,
                   ref: xr.Dataset,
                   monitor: Monitor = Monitor.NONE,
                   step: float = None):
    """
    Calculate anomaly for the given group.

    :param group: Result of a groupby('time.month') operation
    :param ref: Reference dataset
    :param monitor: Monitor of the parent method
    :param step: Step to add to monitor progress
    :return: Group dataset with anomaly calculation applied
    """
    # Retrieve the month of the current group
    month = group['time.month'][0].values
    ret = diff(group, ref.isel(time=month - 1))
    monitor.progress(work=step)
    return ret


@op(tags=['anomaly'], version='1.0')
@op_input('time_range', data_type=TimeRangeLike)
@op_input('region', data_type=PolygonLike)
@op_return(add_history=True)
def anomaly_internal(ds: xr.Dataset,
                     time_range: TimeRangeLike.TYPE = None,
                     region: PolygonLike.TYPE = None,
                     monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Calculate anomaly using as reference data the mean of an optional region
    and time slice from the given dataset. If no time slice/spatial region is
    given, the operation will calculate anomaly using the mean of the whole
    dataset as the reference.

    This is done for each data array in the dataset.
    :param ds: The dataset to calculate anomalies from
    :param time_range: Time range to use for reference data
    :param region: Spatial region to use for reference data
    :param monitor: a progress monitor.
    :return: The anomaly dataset
    """
    ref = ds.copy()
    if time_range:
        time_range = TimeRangeLike.convert(time_range)
        ref = subset_temporal(ref, time_range)
    if region:
        region = PolygonLike.convert(region)
        ref = subset_spatial(ref, region)
    with monitor.observing("Calculating anomaly"):
        ref = ref.mean(keep_attrs=True, skipna=True)
        diff = ds - ref
    return diff
