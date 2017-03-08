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

Select variables in a dataset.

Components
==========
"""

import fnmatch

import geopandas as gpd
import xarray as xr

from cate.core.op import op
from cate.util import to_list


@op(tags=['select', 'subset', 'filter', 'var'])
# TODO (Gailis, 27.09.16) See issues #45 and #46
#def select_var(ds: xr.Dataset, var: Union[None, str, List[str]] = None) -> xr.Dataset:
def select_var(ds: xr.Dataset, var: str = None) -> xr.Dataset:
    """
    Filter the dataset, by leaving only the desired variables in it. The original dataset
    information, including original coordinates, is preserved.

    :param ds: The dataset from which to perform selection.
    :param var: One or more variable names to select and preserve in the dataset. \
    All of these are valid 'var_name' 'var_name1,var_name2,var_name3' ['var_name1', 'var_name2']. \
    One can also use wildcards when doing the selection. E.g., choosing 'var_name*' for selection \
    will select all variables that start with 'var_name'. This can be used to select variables \
    along with their auxiliary variables, to select all uncertainty variables, and so on.
    :return: A filtered dataset
    """
    if not var:
        return ds

    var_names = to_list(var, name='var')
    dropped_var_names = list(ds.data_vars.keys())

    for pattern in var_names:
        keep = fnmatch.filter(dropped_var_names, pattern)
        for name in keep:
            dropped_var_names.remove(name)

    return ds.drop(dropped_var_names)


@op(tags=['select', 'filter', 'var'])
def select_features(df: gpd.GeoDataFrame, var: dict = None) -> gpd.GeoDataFrame:
    """
    Filter the dataframe, by leaving only the desired features in it. The original dataframe
    information, including original features, is preserved.

    :param df: The dataframe from which to perform selection.
    :param var: One or more feature names to select and preserve in the dataframe.
    :return: A filtered dataframe
    """

    if not var:
        return df

    return df.query(' | '.join(['%s == "%s"' % (key, value) for (key, value) in var.items()]))
