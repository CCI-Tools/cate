# The MIT License (MIT)
# Copyright (c) 2016 by the ECT Development Team and contributors
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

from ect.core.op import op_input


@op_input('datasets', description='A list of datasets to harmonize')
def harmonize(datasets: list):
    """
    Harmonize the given datasets in place. E.g. change dimension names
    if they differ from expected values.

    :param datasets: A list of datasets to harmonize
    """
    for dataset in datasets:
        _harmonize_dataset(dataset)


def _harmonize_dataset(ds: xr.Dataset):
    """
    Harmonize a single dataset

    :param Dataset: A dataset to harmonize
    """
    lat_name = _get_lat_dim_name(ds)
    lon_name = _get_lon_dim_name(ds)

    name_dict = dict()
    if lat_name:
        name_dict[lat_name] = 'lat'

    if lon_name:
        name_dict[lon_name] = 'lon'

    ds.rename(name_dict, inplace=True)


def _get_lon_dim_name(xarray: xr.Dataset) -> str:
    return _get_dim_name(xarray, ['lon', 'longitude', 'long'])


def _get_lat_dim_name(xarray: xr.Dataset) -> str:
    return _get_dim_name(xarray, ['lat', 'latitude'])


def _get_dim_name(xarray: xr.Dataset, possible_names) -> str:
    for name in possible_names:
        if name in xarray.dims:
            return name
    return None
