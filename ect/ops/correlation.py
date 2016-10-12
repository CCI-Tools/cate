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

Correlation operations

Functions
=========
"""

from ect.core.op import op, op_input
import xarray as xr
import numpy as np
from scipy.stats import pearsonr


@op(tags=['correlation'])
@op_input('corr_type', value_set=['pixel_by_pixel'])
def pearson_correlation(ds_x: xr.Dataset,
                        ds_y: xr.Dataset,
                        var_x: str,
                        var_y: str,
                        file: str = None,
                        corr_type: str = 'pixel_by_pixel') -> xr.Dataset:
    """
    Do product moment `Pearson's correlation <http://www.statsoft.com/Textbook/Statistics-Glossary/P/button/p#Pearson%20Correlation>`_ analysis.

    For more information how to interpret the results, see
    `here <http://support.minitab.com/en-us/minitab-express/1/help-and-how-to/modeling-statistics/regression/how-to/correlation/interpret-the-results/>`_.

    The provided variables have to have the same shape, but depending on the
    type of variables and chosen correlation type, not necessarily the same
    definition for all dimensions. E.g., it is possible to correlate two
    datasets of the same area at different times.

    If two 1D or 2D variables are provided, a single pair of correlation
    coefficient and p_value will be calculated and returned, as well as
    optionally saved in a text file.

    In case 3D time/lat/lon variables are provided, a correlation will be
    perfomed according to the given correlation type. In case a pixel_by_pixel
    correlation is chosen, the datasets have to have the same lat/lon
    definition, so that a 2D lat/lon map of correlation coefficients, as well
    as p_values can be constructed.

    :param ds_y: The 'dependent' dataset
    :param ds_x: The 'variable' dataset
    :param var_y: Dataset variable to use for correlation analysis in the 'dependent' dataset
    :param var_x: Dataset variable to use for correlation analysis in the 'variable' dataset
    :param file: Filepath variable. If given, this is where the results will be saved in a text file.
    :param corr_type: Correlation type to use for 3D time/lat/lon variables.
    """
    array_y = ds_y[var_y]
    array_x = ds_x[var_x]

    if len(array_x.dims) > 3 or len(array_y.dims) > 3:
        raise NotImplementedError('Pearson correlation for multi-dimensional variables is not yet implemented.')

    if array_x.values.shape != array_y.values.shape:
        raise ValueError('The provided variables {} and {} do not have the same shape, '
                         'Pearson correlation can not be performed.'.format(var_x, var_y))

    # Perform a simple Pearson correlation that returns just a coefficient and
    # a p_value.
    if len(array_x.dims) < 3:
        return _pearson_simple(ds_x, ds_y, var_x, var_y, file)

    if corr_type != 'pixel_by_pixel':
        raise NotImplementedError('Only pixel by pixel Pearson correlation is currently implemented for '
                                  'time/lat/lon dataset variables.')

    if (not ds_x['lat'].equals(ds_y['lat']) or
            not ds_x['lon'].equals(ds_y['lon'])):
        raise ValueError('When performing a pixel by pixel correlation the datasets have to have the same '
                         'lat/lon definition.')

    # Do pixel by pixel correlation

    lat = ds_x['lat']
    lon = ds_y['lon']

    corr_coef = np.zeros([len(lat), len(lon)])
    p_value = np.zeros([len(lat), len(lon)])

    for lai in range(0, len(lat)):
        for loi in range(0, len(lon)):
            x = array_x.isel(lat=lai, lon=loi).values
            y = array_y.isel(lat=lai, lon=loi).values
            corr_coef[lai, loi], p_value[lai, loi] = pearsonr(x, y)

    retset = xr.Dataset({
        'corr_coef': (['lat', 'lon'], corr_coef),
        'p_value': (['lat', 'lon'], p_value),
        'lat': lat,
        'lon': lon})
    retset.attrs['ECT_Description'] = 'Correlation between {} {}'.format(var_y, var_x)

    if file:
        with open(file, "w") as text_file:
            print(retset, file=text_file)
            print(retset['corr_coef'], file=text_file)
            print(retset['p_value'], file=text_file)

    return retset


def _pearson_simple(ds_x: xr.Dataset,
                    ds_y: xr.Dataset,
                    var_x: str,
                    var_y: str,
                    file: str = None) -> xr.Dataset:
    """
    Perform a simple Pearson correlation that gets just the coefficient and
    the p_value, as well as creates a dataset to return, and saves the result
    in the given file.
    """
    corr_coef, p_value = pearsonr(ds_x[var_x].values,
                                  ds_y[var_y].values)

    retset = xr.Dataset()
    retset.attrs['ECT_Description'] = 'Correlation between {} {}'.format(var_y, var_x)
    retset['corr_coef'] = corr_coef
    retset['p_value'] = p_value

    # Save the result if file path is given
    if file:
        with open(file, "w") as text_file:
            print(retset, file=text_file)

    return retset
