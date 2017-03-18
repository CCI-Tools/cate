
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

from cate.core.op import op
from cate.ops.select import select_var
from cate.ops.subset import subset_spatial
import xarray as xr


@op(tags=['index', 'nino34'])
def enso_nino34(ds: xr.Dataset, file: str, var: str, threshold: float=False):
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
    n34 = '-170, -5, -120, 5'
    ds_n34 = subset_spatial(ds, n34)
    ref_n34 = subset_spatial(ref, n34)
    n34_anom = ds_n34 - ref_n34
    n34_ts = n34_anom.mean(dim=['lat', 'lon'])
    windows = {'time':5}
    i = True
    for item in n34_ts[var].rolling(**windows):
        # After the mean I probably have to give 'time' dimension
        # back to the item, so that the rolling means can then be concatenated
        # into a timeseries.
        if i:
            retset = item[0].mean()
            i = False
        print('====')
        print(item[0])
        print(item[1])
        print('====')
        retset = xr.concat([retset, item[0].mean()])
    print(retset)
    #n34_running_mean = pd.rolling_mean(n34_ts[var].values, 5)
    # Should this be a pd.Dataframe instead? Does it make sense to have it in
    # an xarray dataset? Norman assumes that those can be plotted on the globe,
    # if this is an actual 'index' table, it should be a dataframe instead.
    # It could of course be converted to xr.Dataset if needed, say, for
    # correlation.
    #return xr.Dataset(n34_running_mean)


@op(tags=['index'])
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
    regions = {'n1+2': '',
               'n3': '',
               'n34': '',
               'n4': '',
               'custom': custom_region}

    region = PolygonLike.convert(regions[region])
    pass


@op(tags=['index'])
def oni_index(ds: xr.Dataset, var: str, file: str):
    """
    Calculate ONI index, which is defined as a three month running mean of
    anomalies of monthly means of SST data in the Nino3.4 region.

    :param ds: A monthly SST dataset
    :param file: Path to the reference data file
    :param var: Dataset variable to use for index calculation
    """
    pass
