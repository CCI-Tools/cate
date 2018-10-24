
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

Index calculation operations

Functions
=========
"""
import xarray as xr
import pandas as pd

from cate.core.op import op, op_input
from cate.ops.select import select_var
from cate.ops.subset import subset_spatial
from cate.ops.anomaly import anomaly_external
from cate.core.types import PolygonLike, VarName, ValidationError
from cate.util.monitor import Monitor


_ALL_FILE_FILTER = dict(name='All Files', extensions=['*'])


@op(tags=['index'])
@op_input('file', file_open_mode='r', file_filters=[dict(name='NetCDF', extensions=['nc']), _ALL_FILE_FILTER])
@op_input('var', value_set_source='ds', data_type=VarName)
def enso_nino34(ds: xr.Dataset,
                var: VarName.TYPE,
                file: str,
                threshold: float = None,
                monitor: Monitor = Monitor.NONE) -> pd.DataFrame:
    """
    Calculate nino34 index, which is defined as a five month running mean of
    anomalies of monthly means of SST data in Nino3.4 region:: lon_min=-170
    lat_min=-5 lon_max=-120 lat_max=5.

    :param ds: A monthly SST dataset
    :param file: Path to the reference data file e.g. a climatology. A suitable reference dataset
    can be generated using the long_term_average operation
    :param var: Dataset variable (geophysial quantity) to use for index
    calculation.
    :param threshold: If given, boolean El Nino/La Nina timeseries will be
    calculated and added to the output dataset according to the given
    threshold. Where anomaly larger than the positive value of the threshold
    indicates El Nino and anomaly smaller than the negative of the given
    threshold indicates La Nina.
    :param monitor: a progress monitor.
    :return: A dataset that contains the index timeseries.
    """
    n34 = '-170, -5, -120, 5'
    name = 'ENSO N3.4 Index'
    return _generic_index_calculation(ds, var, n34, 5, file, name, threshold, monitor)


@op(tags=['index'])
@op_input('var', value_set_source='ds', data_type=VarName)
@op_input('file', file_open_mode='r', file_filters=[dict(name='NetCDF', extensions=['nc']), _ALL_FILE_FILTER])
@op_input('region', value_set=['N1+2', 'N3', 'N34', 'N4', 'custom'])
@op_input('custom_region', data_type=PolygonLike)
def enso(ds: xr.Dataset,
         var: VarName.TYPE,
         file: str,
         region: str = 'n34',
         custom_region: PolygonLike.TYPE = None,
         threshold: float = None,
         monitor: Monitor = Monitor.NONE) -> pd.DataFrame:
    """
    Calculate ENSO index, which is defined as a five month running mean of
    anomalies of monthly means of SST data in the given region.

    :param ds: A monthly SST dataset
    :param file: Path to the reference data file e.g. a climatology. A suitable reference dataset
    can be generated using the long_term_average operation
    :param var: Dataset variable to use for index calculation
    :param region: Region for index calculation, the default is Nino3.4
    :param custom_region: If 'custom' is chosen as the 'region', this parameter
    has to be provided to set the desired region.
    :param threshold: If given, boolean El Nino/La Nina timeseries will be
    calculated and added to the output dataset, according to the given
    threshold. Where anomaly larger than then positive value of the threshold
    indicates El Nino and anomaly smaller than the negative of the given
    threshold indicates La Nina.
    :param monitor: a progress monitor.
    :return: A dataset that contains the index timeseries.
    """
    regions = {'N1+2': '-90, -10, -80, 0',
               'N3': '-150, -5, -90, 5',
               'N3.4': '-170, -5, -120, 5',
               'N4': '160, -5, -150, 5',
               'custom': custom_region}
    converted_region = PolygonLike.convert(regions[region])
    if not converted_region:
        raise ValidationError('No region has been provided to ENSO index calculation')

    name = 'ENSO ' + region + ' Index'
    if 'custom' == region:
        name = 'ENSO Index over ' + PolygonLike.format(converted_region)

    return _generic_index_calculation(ds, var, converted_region, 5, file, name, threshold, monitor)


@op(tags=['index'])
@op_input('var', value_set_source='ds', data_type=VarName)
@op_input('file', file_open_mode='r', file_filters=[dict(name='NetCDF', extensions=['nc']), _ALL_FILE_FILTER])
def oni(ds: xr.Dataset,
        var: VarName.TYPE,
        file: str,
        threshold: float = None,
        monitor: Monitor = Monitor.NONE) -> pd.DataFrame:
    """
    Calculate ONI index, which is defined as a three month running mean of
    anomalies of monthly means of SST data in the Nino3.4 region.

    :param ds: A monthly SST dataset
    :param file: Path to the reference data file e.g. a climatology. A suitable reference dataset
    can be generated using the long_term_average operation
    :param var: Dataset variable to use for index calculation
    :param threshold: If given, boolean El Nino/La Nina timeseries will be
    calculated and added to the output dataset, according to the given
    threshold. Where anomaly larger than then positive value of the threshold
    indicates El Nino and anomaly smaller than the negative of the given
    threshold indicates La Nina.
    :param monitor: a progress monitor.
    :return: A dataset that containts the index timeseries
    """
    n34 = '-170, -5, -120, 5'
    name = 'ONI Index'
    return _generic_index_calculation(ds, var, n34, 3, file, name, threshold, monitor)


def _generic_index_calculation(ds: xr.Dataset,
                               var: VarName.TYPE,
                               region: PolygonLike.TYPE,
                               window: int,
                               file: str,
                               name: str,
                               threshold: float = None,
                               monitor: Monitor = Monitor.NONE) -> pd.DataFrame:
    """
    A generic index calculation. Where an index is defined as an anomaly
    against the given reference of a moving average of the given window size of
    the given given region of the given variable of the given dataset.

    :param ds: Dataset from which to calculate the index
    :param var: Variable from which to calculate index
    :param region: Spatial subset from which to calculate the index
    :param window: Window size for the moving average
    :param file: Path to the reference file
    :param threshold: Absolute threshold that indicates an ENSO event
    :param name: Name of the index
    :param monitor: a progress monitor.
    :return: A dataset that contains the index timeseries
    """
    var = VarName.convert(var)
    region = PolygonLike.convert(region)

    with monitor.starting("Calculate the index", total_work=2):
        ds = select_var(ds, var)
        ds_subset = subset_spatial(ds, region)
        anom = anomaly_external(ds_subset, file, monitor=monitor.child(1))
        with monitor.child(1).observing("Calculate mean"):
            ts = anom.mean(dim=['lat', 'lon'])
        df = pd.DataFrame(data=ts[var].values, columns=[name], index=ts.time)
        retval = df.rolling(window=window, center=True).mean().dropna()

    if threshold is None:
        return retval

    retval['El Nino'] = pd.Series((retval[name] > threshold),
                                  index=retval.index)
    retval['La Nina'] = pd.Series((retval[name] < -threshold),
                                  index=retval.index)
    return retval
