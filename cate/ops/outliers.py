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
import numpy as np

from cate.core.op import op, op_return
from cate.util import to_list
from cate import __version__


@op(version='1.0')
@op_return(add_history=True)
def detect_outliers(ds: xr.Dataset,
                    var: str,
                    threshold_low: float=0.05,
                    threshold_high: float=0.95,
                    quantiles: bool=True,
                    mask: bool=False) -> xr.Dataset:
    """
    Detect outliers in the given Dataset.

    When mask=True the input dataset should not contain nan values, otherwise
    all existing nan values will be marked as 'outliers' in the mask data array
    added to the output dataset.

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
            # Create and add a data variable containing the mask for this data
            # variable
            _mask_outliers(ret_ds, var_name, threshold_low, threshold_high)

    return ret_ds


def _mask_outliers(ds: xr.Dataset, var_name: str, threshold_low: float,
                   threshold_high: float) -> xr.Dataset:
    """
    Create a mask data array for the given variable of the dataset and given
    absolute threshold values. Add the mask data array as an ancillary data
    array to the original array as per CF conventions.

    For explanation about the relevant attributes, see::
    http://cfconventions.org/cf-conventions/v1.6.0/cf-conventions.html#flags
    http://cfconventions.org/cf-conventions/v1.6.0/cf-conventions.html#ancillary-data

    :param ds: The dataset (will be mutated)
    :param var_name: variable name
    :param threshold_low: absolute threshold bottom value
    :param threshold_high: absolute threshold top value
    """
    arr = ds[var_name]

    # Create a boolean mask where True denotes an outlier, convert it to 8-bit
    # integer dtype, as to_netcdf will complain about a boolean dtype
    mask = arr.where((arr > threshold_low) & (arr < threshold_high))
    mask = mask.isnull()
    mask = mask.astype('i1')

    # According to CF conventions, the actual variable name in the netCDF can
    # be whatever, but appending things after an underscore is a reasonable
    # convention
    mask_name = var_name + '_outlier_mask'

    # Set the flag data array attributes as per CF conventions
    try:
        mask.attrs['long_name'] = arr.attrs['long_name'] + ' outlier mask'
    except KeyError:
        # The dataset is not CF compliant, add the attribute anyway
        mask.attrs['long_name'] = 'Outlier mask'
    try:
        mask.attrs['standard_name'] = arr.attrs['standard_name'] + ' status_flag'
    except KeyError:
        # The dataset is not CF compliant, add the attribute anyway
        mask.attrs['standard_name'] = 'status_flag'
    mask.attrs['_FillValue'] = 0
    mask.attrs['valid_range'] = np.array([1.0, 1.0], dtype='i1')
    mask.attrs['flag_values'] = np.array([1], dtype='i1')
    mask.attrs['flag_meanings'] = "is_outlier"
    mask.attrs['source'] = "Cate v" + __version__

    # Add the mask array to the dataset
    ds[mask_name] = mask

    # Create an ancillary variable link between the parent data array and the
    # mask array
    try:
        anc_var = ds[var_name].attrs['ancillary_variables']
    except KeyError:
        # No ancillary variables associated with this variable yet
        anc_var = ''
    ds[var_name].attrs['ancillary_variables'] = anc_var + ' ' + mask_name
