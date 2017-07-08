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

Correlation operations

Functions
=========
"""

import xarray as xr
import numpy as np

from scipy.stats import pearsonr
from scipy.special import betainc

from cate.core.op import op, op_input, op_output
from cate.core.types import VarName


@op(tags=['utility'])
@op_input('var_x', value_set_source='ds_x', data_type=VarName)
@op_input('var_y', value_set_source='ds_y', data_type=VarName)
@op_output('corr_coef', output_type=float)
@op_output('p_value', output_type=float)
def pearson_correlation_simple(ds_x: xr.Dataset,
                               ds_y: xr.Dataset,
                               var_x: VarName.TYPE,
                               var_y: VarName.TYPE):
    """
    Do product moment `Pearson's correlation <http://www.statsoft.com/Textbook/Statistics-Glossary/P/button/p#Pearson%20Correlation>`_ analysis.

    Performs a simple correlation analysis on the provided datasets and returns
    a correlation coefficient and the corresponding p_value for a correlation
    of all values in the given variables.

    The provided variables have to have the same shape, but not neccessarily
    the same definition on all axes. The usual use case would be performing
    correlation analysis of two timeseries.

    Positive correlation implies that as x grows, so does y. Negative
    correlation implies that as x increases, y decreases.

    For more information how to interpret the results, see
    `here <http://support.minitab.com/en-us/minitab-express/1/help-and-how-to/modeling-statistics/regression/how-to/correlation/interpret-the-results/>`_,
    and `here <https://docs.scipy.org/doc/scipy-0.14.0/reference/generated/scipy.stats.pearsonr.html>`_.

    :param ds_x: The 'x' dataset
    :param ds_y: The 'y' dataset
    :param var_x: Dataset variable to use for correlation analysis in the 'variable' dataset
    :param var_y: Dataset variable to use for correlation analysis in the 'dependent' dataset
    :returns: {'corr_coef': correlation coefficient, 'p_value': probability value}
    """
    var_x = VarName.convert(var_x)
    var_y = VarName.convert(var_y)

    array_y = ds_y[var_y]
    array_x = ds_x[var_x]

    if array_x.values.shape != array_y.values.shape:
        raise ValueError('The provided variables {} and {} do not have the same shape, '
                         'Pearson correlation can not be performed. Please '
                         'review operation documentation'.format(var_x, var_y))

    cc, pv = pearsonr(array_x.values, array_y.values)
    return {'corr_coef': cc, 'p_value': pv}


@op(tags=['utility'])
@op_input('var_x', value_set_source='ds_x', data_type=VarName)
@op_input('var_y', value_set_source='ds_y', data_type=VarName)
def pearson_correlation_map(ds_x: xr.Dataset,
                            ds_y: xr.Dataset,
                            var_x: VarName.TYPE,
                            var_y: VarName.TYPE) -> xr.Dataset:
    """
    Do product moment `Pearson's correlation <http://www.statsoft.com/Textbook/Statistics-Glossary/P/button/p#Pearson%20Correlation>`_ analysis.

    Perform Pearson correlation analysis on two time/lat/lon datasets and
    produce a lat/lon map of correlation coefficients and p_values of
    underlying timeseries in the provided datasets.

    The lat/lon definition of both datasets has to be the same. The length of
    the time dimension should be equal, but not neccessarily have the same
    definition. E.g., it is possible to correlate different times of the same
    area.

    There are 'x' and 'y' datasets. Positive correlations imply that as x
    grows, so does y. Negative correlations imply that as x increases, y
    decreases.

    For more information how to interpret the results, see
    `here <http://support.minitab.com/en-us/minitab-express/1/help-and-how-to/modeling-statistics/regression/how-to/correlation/interpret-the-results/>`_,
    and `here <https://docs.scipy.org/doc/scipy-0.14.0/reference/generated/scipy.stats.pearsonr.html>`_.

    :param ds_x: The 'x' dataset
    :param ds_y: The 'y' dataset
    :param var_x: Dataset variable to use for correlation analysis in the 'variable' dataset
    :param var_y: Dataset variable to use for correlation analysis in the 'dependent' dataset
    :returns: a dataset containing a map of correlation coefficients and p_values
    """
    var_x = VarName.convert(var_x)
    var_y = VarName.convert(var_y)

    array_y = ds_y[var_y]
    array_x = ds_x[var_x]

    if len(array_x.dims) != 3 or len(array_y.dims) != 3:
        raise ValueError('pearson_correlation_map works only on 3D'
                         ' datasets. Please review operation'
                         ' documentation.')

    if array_x.values.shape != array_y.values.shape:
        raise ValueError('The provided variables {} and {} do not have the same shape, '
                         'Pearson correlation can not be performed. Please '
                         'review operation documentation'.format(var_x, var_y))

    if (not ds_x['lat'].equals(ds_y['lat']) or
            not ds_x['lon'].equals(ds_y['lon'])):
        raise ValueError('When performing a pixel by pixel correlation the datasets have to have the same '
                         'lat/lon definition. Consider running coregistration first')

    if len(array_x['time']) < 3:
        raise ValueError('The length of the time dimension should not be less'
                         ' than three to run the calculation.')

    # Do pixel by pixel correlation
    retset = _pearsonr(array_x, array_y)
    retset.attrs['Cate_Description'] = 'Correlation between {} {}'.format(var_y, var_x)

    return retset


def _pearsonr(x: xr.DataArray, y: xr.DataArray) -> xr.Dataset:
    """
    Calculates Pearson correlation coefficients and p-values for testing
    non-correlation of lon/lat/time xarray datasets for each lon/lat point.

    Heavily influenced by scipy.stats.pearsonr

    The Pearson correlation coefficient measures the linear relationship
    between two datasets. Strictly speaking, Pearson's correlation requires
    that each dataset be normally distributed, and not necessarily zero-mean.
    Like other correlation coefficients, this one varies between -1 and +1
    with 0 implying no correlation. Correlations of -1 or +1 imply an exact
    linear relationship. Positive correlations imply that as x increases, so
    does y. Negative correlations imply that as x increases, y decreases.

    The p-value roughly indicates the probability of an uncorrelated system
    producing datasets that have a Pearson correlation at least as extreme
    as the one computed from these datasets. The p-values are not entirely
    reliable but are probably reasonable for datasets larger than 500 or so.

    :param x: lon/lat/time xr.DataArray
    :param y: xr.DataArray of the same spatiotemporal extents and resolution as
    x.
    :return: A dataset containing the correlation coefficients and p_values on
    the lon/lat grid of x and y.

    References
    ----------
    http://www.statsoft.com/textbook/glosp.html#Pearson%20Correlation
    """
    n = len(x['time'])

    xm, ym = x - x.mean(dim='time'), y - y.mean(dim='time')
    xm_ym = xm * ym
    r_num = xm_ym.sum(dim='time')
    xm_squared = xr.ufuncs.square(xm)
    ym_squared = xr.ufuncs.square(ym)
    r_den = xr.ufuncs.sqrt(xm_squared.sum(dim='time') *
                           ym_squared.sum(dim='time'))
    r_den = r_den.where(r_den != 0)
    r = r_num / r_den

    # Presumably, if abs(r) > 1, then it is only some small artifact of floating
    # point arithmetic.
    # At this point r should be a lon/lat dataArray, so it should be safe to
    # load it in memory explicitly. This may take time as it will kick-start
    # deferred processing.
    # Comparing with NaN produces warnings that can be safely ignored
    default_warning_settings = np.seterr(invalid='ignore')
    r.values[r.values < -1.0] = -1.0
    r.values[r.values > 1.0] = 1.0
    np.seterr(**default_warning_settings)
    r.attrs = {'description': 'Correlation coefficients between'
               ' {} and {}.'.format(x.name, y.name)}

    df = n - 2
    t_squared = xr.ufuncs.square(r) * (df / ((1.0 - r.where(r != 1)) *
                                             (1.0 + r.where(r != -1))))
    prob = df / (df + t_squared)
    prob.values = betainc(0.5 * df, 0.5, prob.values)
    prob.attrs = {'description': 'Rough indicator of probability of an'
                  ' uncorrelated system producing datasets that have a Pearson'
                  ' correlation at least as extreme as the one computed from'
                  ' these datsets. Not entirely reliable, but reasonable for'
                  ' datasets larger than 500 or so.'}

    retset = xr.Dataset({'corr_coef': r,
                         'p_value': prob})
    return retset
