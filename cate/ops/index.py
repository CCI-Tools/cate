
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
from cate.core.types import PolygonLike


_ALL_FILE_FILTER = dict(name='All Files', extensions=['*'])


@op(tags=['index', 'nino34'])
@op_input('file', file_open_mode='w', file_filters=[dict(name='NetCDF',
                                                         extensions=['nc']),
                                                         _ALL_FILE_FILTER])
def enso_nino34(ds: xr.Dataset, var:str, file: str, threshold: float=False):
    """
    Calculate nino34 index, which is defined as a five month running mean of
    anomalies of monthly means of SST data in Nino3.4 region.

    :param ds: A monthly SST dataset
    :param file: Path to the reference data file
    :param var: Dataset variable (geophysial quantity) to use for index
    calculation.
    :param threshold: If given, boolean El Nino/La Nina timeseries will be
    calculated and added to the output dataset according to the given
    threshold. Where anomaly larger than the positive value of the threshold
    indicates El Nino and anomaly smaller than the negative of the given
    threshold indicates La Nina.
    :return: A dataset that contains the index timeseries.
    """
    ds = select_var(ds, var)
    ref = xr.open_dataset(file)
    ref = select_var(ref, var)
    ref.load()
    print(ref-ds)
    print(ds-ref)
    n34 = '-170, -5, -120, 5'
    ds_n34 = subset_spatial(ds, n34)
    ref_n34 = subset_spatial(ref, n34)
    n34_anom = ds_n34 - ref_n34
    n34_ts = n34_anom.mean(dim=['lat', 'lon'])
    n34_data_frame = pd.DataFrame(data=n34_ts[var].values, columns=['Index'],
                                  index=n34_ts.time)
    n34_running_mean = pd.rolling_mean(n34_data_frame, 5)
    return n34_running_mean


@op(tags=['index'])
@op_input('file', file_open_mode='w', file_filters=[dict(name='NetCDF',
                                                         extensions=['nc']),
                                                         _ALL_FILE_FILTER])
@op_input('region', value_set=['n1+2', 'n3', 'n34', 'n4', 'custom'])
@op_input('custom_region', cate_type='polygon')
def enso_index(ds: xr.Dataset,
               var: str,
               file: str,
               region: str='n34',
               custom_region: PolygonLike.TYPE=None,
               threshold: float=False):
    """
    Calculate ENSO index, which is defined as a five month running mean of
    anomalies of monthly means of SST data in the given region.

    :param ds: A monthly SST dataset
    :param file: Path to the reference data file
    :param var: Dataset variable to use for index calculation
    :param region: Region for index calculation, the default is Nino3.4
    :param custom_region: If 'custom' is chosen as the 'region', this parameter
    has to be provided to set the desired region.
    :param threshold: If given, boolean El Nino/La Nina timeseries will be
    calculated and added to the output dataset, according to the given
    threshold. Where anomaly larger than then positive value of the threshold
    indicates El Nino and anomaly smaller than the negative of the given
    threshold indicates La Nina.
    :return: A dataset that contains the index timeseries.
    """
    regions = {'n1+2': '-90, -10, -80, 0',
               'n3': '-150, -5, -90, 5',
               'n34': '-170, -5, -120, 5',
               'n4': '160, -5, -150, 5',
               'custom': custom_region}

    region = PolygonLike.convert(regions[region])
    pass


@op(tags=['index'])
@op_input('file', file_open_mode='w', file_filters=[dict(name='NetCDF',
                                                         extensions=['nc']),
                                                         _ALL_FILE_FILTER])
def oni_index(ds: xr.Dataset, var: str, file: str):
    """
    Calculate ONI index, which is defined as a three month running mean of
    anomalies of monthly means of SST data in the Nino3.4 region.

    :param ds: A monthly SST dataset
    :param file: Path to the reference data file
    :param var: Dataset variable to use for index calculation
    """
    pass
