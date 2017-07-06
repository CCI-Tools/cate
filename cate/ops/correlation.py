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

from cate.core.op import op, op_input
from cate.core.types import VarName


_ALL_FILE_FILTER = dict(name='All Files', extensions=['*'])


@op(tags=['correlation'])
@op_input('var_x', value_set_source='ds_x', data_type=VarName)
@op_input('var_y', value_set_source='ds_y', data_type=VarName)
def pearson_correlation(ds_x: xr.Dataset,
                        ds_y: xr.Dataset,
                        var_x: VarName.TYPE,
                        var_y: VarName.TYPE) -> xr.Dataset:
    """
    Do product moment `Pearson's correlation <http://www.statsoft.com/Textbook/Statistics-Glossary/P/button/p#Pearson%20Correlation>`_ analysis.

    For more information how to interpret the results, see
    `here <http://support.minitab.com/en-us/minitab-express/1/help-and-how-to/modeling-statistics/regression/how-to/correlation/interpret-the-results/>`_,
    and `here <https://docs.scipy.org/doc/scipy-0.14.0/reference/generated/scipy.stats.pearsonr.html>`_.

    The provided variables have to have the same shape, but depending on the
    type of variables and chosen correlation type, not necessarily the same
    definition for all dimensions. E.g., it is possible to correlate two
    datasets of the same area at different times.

    If two 1D or 2D variables are provided, a single pair of correlation
    coefficient and p_value will be calculated and returned.

    In case 3D time/lat/lon variables are provided, pixel by pixel correlation
    will be performed.  The datasets have to have the same lat/lon
    definition, so that a 2D lat/lon map of correlation coefficients, as well
    as p_values can be constructed.

    There are 'x' and 'y' datasets. Positive correlations imply that as x
    grows, so does y. Negative correlations imply that as x increases, y
    decreases.

    :param ds_x: The 'x' dataset
    :param ds_y: The 'y' dataset
    :param var_x: Dataset variable to use for correlation analysis in the 'variable' dataset
    :param var_y: Dataset variable to use for correlation analysis in the 'dependent' dataset
    """
    var_x = VarName.convert(var_x)
    var_y = VarName.convert(var_y)

    array_y = ds_y[var_y]
    array_x = ds_x[var_x]

    if len(array_x.dims) > 3 or len(array_y.dims) > 3:
        raise NotImplementedError('Pearson correlation for multi-dimensional variables is not implemented.')

    if array_x.values.shape != array_y.values.shape:
        raise ValueError('The provided variables {} and {} do not have the same shape, '
                         'Pearson correlation can not be performed. Please '
                         'review operation documentation'.format(var_x, var_y))

    # Perform a simple Pearson correlation that returns just a coefficient and
    # a p_value.
    if len(array_x.dims) < 3:
        return _pearson_simple(ds_x, ds_y, var_x, var_y)

    if (not ds_x['lat'].equals(ds_y['lat']) or
            not ds_x['lon'].equals(ds_y['lon'])):
        raise ValueError('When performing a pixel by pixel correlation the datasets have to have the same '
                         'lat/lon definition. Consider running coregistration first')

    # Do pixel by pixel correlation
    retset = _pearsonr(array_x, array_y)
    retset.attrs['Cate_Description'] = 'Correlation between {} {}'.format(var_y, var_x)

    return retset


def _pearson_simple(ds_x: xr.Dataset,
                    ds_y: xr.Dataset,
                    var_x: str,
                    var_y: str) -> xr.Dataset:
    """
    Perform a simple Pearson correlation that gets just the coefficient and
    the p_value.
    """
    corr_coef, p_value = pearsonr(ds_x[var_x].values,
                                  ds_y[var_y].values)

    retset = xr.Dataset()
    retset.attrs['Cate_Description'] = 'Correlation between {} {}'.format(var_y, var_x)
    retset['corr_coef'] = corr_coef
    retset['p_value'] = p_value

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
