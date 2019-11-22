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


import numpy as np
import pandas as pd
import xarray as xr

# If CTRL-C is pressed on console on Windows, we get
#   forrtl: error (200): program aborting due to control-C event
# Setting FOR_DISABLE_CONSOLE_CTRL_HANDLER=1 should actually avoid this,
# see https://stackoverflow.com/questions/15457786/ctrl-c-crashes-python-after-importing-scipy-stats
# import os
# os.environ['FOR_DISABLE_CONSOLE_CTRL_HANDLER'] = '1'
# Unfortunately, if the above is uncommented cate-webapi doesn't handle CTRL-C anymore even though
# a SIGINT handler is registered.
from scipy.stats import pearsonr
from scipy.special import betainc

from cate.core.op import op, op_input, op_return
from cate.core.types import VarName, DatasetLike, ValidationError
from cate.util.monitor import Monitor

from cate.ops.normalize import adjust_spatial_attrs


@op(tags=['utility', 'correlation'])
@op_input('ds_x', data_type=DatasetLike)
@op_input('ds_y', data_type=DatasetLike)
@op_input('var_x', value_set_source='ds_x', data_type=VarName)
@op_input('var_y', value_set_source='ds_y', data_type=VarName)
def pearson_correlation_scalar(ds_x: DatasetLike.TYPE,
                               ds_y: DatasetLike.TYPE,
                               var_x: VarName.TYPE,
                               var_y: VarName.TYPE,
                               monitor: Monitor = Monitor.NONE) -> pd.DataFrame:
    """
    Do product moment `Pearson's correlation <http://www.statsoft.com/Textbook/Statistics-Glossary/P/button/p#Pearson%20Correlation>`_ analysis.

    Performs a simple correlation analysis on two data variables and returns
    a correlation coefficient and the corresponding p_value.

    Positive correlation implies that as x grows, so does y. Negative
    correlation implies that as x increases, y decreases.

    For more information how to interpret the results, see
    `here <http://support.minitab.com/en-us/minitab-express/1/help-and-how-to/modeling-statistics/regression/how-to/correlation/interpret-the-results/>`_,
    and `here <https://docs.scipy.org/doc/scipy-0.14.0/reference/generated/scipy.stats.pearsonr.html>`_.

    :param ds_x: The 'x' dataset
    :param ds_y: The 'y' dataset
    :param var_x: Dataset variable to use for correlation analysis in the 'variable' dataset
    :param var_y: Dataset variable to use for correlation analysis in the 'dependent' dataset
    :param monitor: a progress monitor.
    :return: Data frame {'corr_coef': correlation coefficient, 'p_value': probability value}
    """
    ds_x = DatasetLike.convert(ds_x)
    ds_y = DatasetLike.convert(ds_y)
    var_x = VarName.convert(var_x)
    var_y = VarName.convert(var_y)

    array_y = ds_y[var_y]
    array_x = ds_x[var_x]

    if (array_x.dims != array_y.dims):
        raise ValidationError('Both datasets should feature the same'
                              ' dimensionality. Currently provided ds_x[var_x] '
                              f'has {array_x.dims}, provided ds_y[var_y]'
                              f' has {array_y.dims}')

    for dim in array_x.dims:
        if len(array_x[dim]) != len(array_y[dim]):
            raise ValidationError('All dimensions of both provided data variables'
                                  f' must be the same length. Currently {dim} of ds_x[var_x]'
                                  f' has {len(array_x[dim])} values, while'
                                  f' {dim} of ds_y[var_y] has {len(array_y[dim])} values.'
                                  ' You may want to try to coregister the datasets beforehand.')

    n_vals = 1
    for dim in array_x.dims:
        n_vals = n_vals * len(array_x[dim])

    if n_vals < 3:
        raise ValidationError('There should be no less than 3 values in both data variables'
                              f' to perform the correlation. Currently there are {n_vals} values')

    with monitor.observing("Calculate Pearson correlation"):
        cc, pv = pearsonr(array_x.stack(z=array_x.dims), array_y.stack(z=array_y.dims))

    return pd.DataFrame({'corr_coef': [cc], 'p_value': [pv]})


@op(tags=['utility', 'correlation'], version='1.0')
@op_input('ds_x', data_type=DatasetLike)
@op_input('ds_y', data_type=DatasetLike)
@op_input('var_x', value_set_source='ds_x', data_type=VarName)
@op_input('var_y', value_set_source='ds_y', data_type=VarName)
@op_return(add_history=True)
def pearson_correlation(ds_x: DatasetLike.TYPE,
                        ds_y: DatasetLike.TYPE,
                        var_x: VarName.TYPE,
                        var_y: VarName.TYPE,
                        monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Do product moment `Pearson's correlation <http://www.statsoft.com/Textbook/Statistics-Glossary/P/button/p#Pearson%20Correlation>`_ analysis.

    Perform Pearson correlation on two datasets and produce a lon/lat map of
    correlation coefficients and the correspoding p_values.

    In case two 3D lon/lat/time datasets are provided, pixel by pixel
    correlation will be performed. It is also possible two pro
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
    :param monitor: a progress monitor.
    :return: a dataset containing a map of correlation coefficients and p_values
    """
    ds_x = DatasetLike.convert(ds_x)
    ds_y = DatasetLike.convert(ds_y)
    var_x = VarName.convert(var_x)
    var_y = VarName.convert(var_y)

    array_y = ds_y[var_y]
    array_x = ds_x[var_x]

    # Further validate inputs
    if array_x.dims == array_y.dims:
        if len(array_x.dims) != 3 or len(array_y.dims) != 3:
            raise ValidationError('A correlation coefficient map can only be produced'
                                  ' if both provided datasets are 3D datasets with'
                                  ' lon/lat/time dimensionality, or if a combination'
                                  ' of a 3D lon/lat/time dataset and a 1D timeseries'
                                  ' is provided.')

        if array_x.values.shape != array_y.values.shape:
            raise ValidationError(f'The provided variables {var_x} and {var_y} do not have the'
                                  ' same shape, Pearson correlation can not be'
                                  ' performed. Please review operation'
                                  ' documentation')

        if (not ds_x['lat'].equals(ds_y['lat']) or not ds_x['lon'].equals(ds_y['lon'])):
            raise ValidationError('When performing a pixel by pixel correlation the'
                                  ' datasets have to have the same lat/lon'
                                  ' definition. Consider running coregistration'
                                  ' first')

    elif (((len(array_x.dims) == 3) and (len(array_y.dims) != 1))
          or ((len(array_x.dims) == 1) and (len(array_y.dims) != 3))
          or ((len(array_x.dims) != 3) and (len(array_y.dims) == 1))
          or ((len(array_x.dims) != 1) and (len(array_y.dims) == 3))):
        raise ValidationError('A correlation coefficient map can only be produced'
                              ' if both provided datasets are 3D datasets with'
                              ' lon/lat/time dimensionality, or if a combination'
                              ' of a 3D lon/lat/time dataset and a 1D timeseries'
                              ' is provided.')

    if len(array_x['time']) != len(array_y['time']):
        raise ValidationError('The length of the time dimension differs between'
                              ' the given datasets. Can not perform the calculation'
                              ', please review operation documentation.')

    if len(array_x['time']) < 3:
        raise ValidationError('The length of the time dimension should not be less'
                              ' than three to run the calculation.')

    # Do pixel by pixel correlation
    retset = _pearsonr(array_x, array_y, monitor)
    retset.attrs['Cate_Description'] = f'Correlation between {var_y} {var_x}'

    return adjust_spatial_attrs(retset)


def _pearsonr(x: xr.DataArray, y: xr.DataArray, monitor: Monitor) -> xr.Dataset:
    """
    Calculate Pearson correlation coefficients and p-values for testing
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
    :param y: xr.DataArray of the same spatiotemporal extents and resolution as x.
    :param monitor: Monitor to use for monitoring the calculation
    :return: A dataset containing the correlation coefficients and p_values on
    the lon/lat grid of x and y.

    References
    ----------
    http://www.statsoft.com/textbook/glosp.html#Pearson%20Correlation
    """
    with monitor.starting("Calculate Pearson correlation", total_work=6):
        n = len(x['time'])

        xm, ym = x - x.mean(dim='time'), y - y.mean(dim='time')
        xm['time'] = [i for i in range(0, len(xm.time))]
        ym['time'] = [i for i in range(0, len(ym.time))]
        xm_ym = xm * ym
        r_num = xm_ym.sum(dim='time')
        xm_squared = np.square(xm)
        ym_squared = np.square(ym)
        r_den = np.sqrt(xm_squared.sum(dim='time') * ym_squared.sum(dim='time'))
        r_den = r_den.where(r_den != 0)
        r = r_num / r_den

        # Presumably, if abs(r) > 1, then it is only some small artifact of floating
        # point arithmetic.
        # At this point r should be a lon/lat dataArray, so it should be safe to
        # load it in memory explicitly. This may take time as it will kick-start
        # deferred processing.
        # Comparing with NaN produces warnings that can be safely ignored
        default_warning_settings = np.seterr(invalid='ignore')
        with monitor.child(1).observing("task 1"):
            negativ_r = r.values < -1.0
        with monitor.child(1).observing("task 2"):
            r.values[negativ_r] = -1.0
        with monitor.child(1).observing("task 3"):
            positiv_r = r.values > 1.0
        with monitor.child(1).observing("task 4"):
            r.values[positiv_r] = 1.0
        np.seterr(**default_warning_settings)
        r.attrs = {'description': 'Correlation coefficients between'
                   ' {} and {}.'.format(x.name, y.name)}

        df = n - 2
        t_squared = np.square(r) * (df / ((1.0 - r.where(r != 1)) * (1.0 + r.where(r != -1))))

        prob = df / (df + t_squared)
        with monitor.child(1).observing("task 5"):
            prob_values_in = prob.values
        with monitor.child(1).observing("task 6"):
            prob.values = betainc(0.5 * df, 0.5, prob_values_in)
        prob.attrs = {'description': 'Rough indicator of probability of an'
                      ' uncorrelated system producing datasets that have a Pearson'
                      ' correlation at least as extreme as the one computed from'
                      ' these datsets. Not entirely reliable, but reasonable for'
                      ' datasets larger than 500 or so.'}

        retset = xr.Dataset({'corr_coef': r,
                             'p_value': prob})
    return retset
