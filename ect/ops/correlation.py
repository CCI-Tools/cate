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

Components
==========
"""

from ect.core.op import op
import xarray as xr
from scipy.stats import pearsonr


@op(tags=['correlation'])
def pearson_correlation(ds_y: xr.Dataset, ds_x: xr.Dataset, var_y: str, var_x:
                        str, file: str = None) -> xr.Dataset:
    """
    Do product moment Pearson's correlation analysis. See

    http://www.statsoft.com/Textbook/Statistics-Glossary/P/button/p#Pearson%20Correlation
    http://support.minitab.com/en-us/minitab-express/1/help-and-how-to/modeling-statistics/regression/how-to/correlation/interpret-the-results/

    :param ds_y: The 'dependent' dataset
    :param ds_x: The 'variable' dataset
    :param var_y: Dataset variable to use for correlation analysis in the
    'dependent' dataset
    :param var_x: Dataset variable to use for correlation analysis in the
    'variable' dataset
    :param file: Filepath variable. If given, this is where the results will
    be saved in a text file.
    """
    if len(ds_y[var_y].dims) != 1 or len(ds_x[var_x].dims) != 1:
        raise ValueError('Person correlation for multi-dimensional variables\
 is not yet implemented.')

    array_y = ds_y[var_y]
    array_x = ds_x[var_x]

    corr_coef, p_value = pearsonr(array_x, array_y)

    # Save the result if file path is given
    if file:
        with open(file, "w") as text_file:
            print("Correlation coefficient: {}".format(corr_coef),
                  file=text_file)
            print("P value: {}".format(p_value), file=text_file)

    retset = xr.Dataset()
    retset.attrs['ECT_Description'] = 'Correlation between {} {}'.format(var_y,
                                                                         var_x)
    retset['correlation_coefficient'] = corr_coef
    retset['p_value'] = p_value

    return retset
