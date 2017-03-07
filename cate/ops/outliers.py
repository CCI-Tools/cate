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

Outlier detection operations

Functions
=========
"""
import fnmatch
import xarray as xr

from cate.core.op import op
from cate.util import to_list


# Stamp if/when pull request #143 is accepted
# @op(version='1.0')
# @op_return(add_history=True)
@op(version='1.0')
def detect_outliers(ds: xr.Dataset,
                    var: str,
                    threshold_low: float=0.05,
                    threshold_high: float=0.95,
                    quantiles: bool=True,
                    mask: bool=False) -> xr.Dataset:
    """
    Detect outliers in the given Dataset

    :param ds: The dataset for which to do outlier detection
    :param var: Variable or variables in the dataset to which to do outlier
    detection. Note that when multiple variables are selected, absolute
    threshold values might not make much sense. Wild cards can be used to
    select multiple variables matching a pattern.
    :param threshold_low: Values less or equal to this will be removed/masked
    :param threshold_high: Values greater or equal to this will be removed/masked
    :bool quantiles: If True, threshold values are treated as quantiles,
    otherwise as absolute values.
    :bool mask: If True, an ancillary variable containing flag values for
    outliers will be added to the dataset. Otherwise, outliers will be replaced
    with nan directly in the data variables.
    :return: The dataset with outliers masked or replaced with nan
    """
    # Create a list of variable names on which to perform outlier detection
    # based on the input comma separated list that can contain wildcards
    var_patterns = to_list(var, name='var')
    all_vars = list(ds.data_vars.keys())
    variables = list()
    for pattern in var_patterns:
        leave = fnmatch.filter(all_vars, pattern)
        variables = variables + leave

    # For each array in the dataset for which we should detect outliers, detect
    # outliers
    ret_ds = ds.copy()
    for var_name in variables:
        if quantiles:
            # Get threshold values
            threshold_low = ret_ds[var_name].quantile(threshold_low)
            threshold_high = ret_ds[var_name].quantile(threshold_high)

        # If not mask, put nans in the data arrays for min/max outliers
        if not mask:
            arr = ret_ds[var_name]
            attrs = arr.attrs
            ret_ds[var_name] = arr.where((arr > threshold_low) &
                                         (arr < threshold_high))
            ret_ds[var_name].attrs = attrs
        else:
            ret_ds[var_name] = _mask_outliers(ret_ds,
                                              var_name,
                                              threshold_low,
                                              threshold_high)

    return ret_ds


def _mask_outliers(ds: xr.Dataset, var_name: str, threshold_low: float,
                   threshold_high: float) -> xr.Dataset:
    # Otherwise, copy array, put 1 for outliers, and zero for every other
    # value, add proper attributes to make this into a mask, append this to
    # the ancillary dataset attribute of the original array.
    pass
