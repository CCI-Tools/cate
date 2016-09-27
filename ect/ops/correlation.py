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
import math

from ect.core.op import op
import xarray as xr


@op(tags=['correlation'])
def pearson_correlation(ds_y: xr.Dataset, ds_x: xr.Dataset, var_y: str, var_x: str, file: str = None) -> dict:
    """
    Do product moment Pearson's correlation analysis.

    :param ds_y: The 'dependent' dataset
    :param ds_x: The 'variable' dataset
    :param var_y: Dataset variable to use for correlation analysis in the 'dependent' dataset
    :param var_x: Dataset variable to use for correlation analysis in the 'variable' dataset
    :param file: Filepath variable. If given, this is where the results will be saved in a text file.
    """
    if len(ds_y[var_y].dims) != 1 or len(ds_x[var_x].dims) != 1:
        raise ValueError('Person correlation for multi-dimensional variables is not yet implemented.')

    array_y = ds_y[var_y]
    array_x = ds_x[var_x]

    y_mean = array_y.mean().data
    x_mean = array_x.mean().data

    a = 0.
    b = 0.
    c = 0.

    for i in range(0, len(array_y.data)):
        a = a + ((array_x[i] - x_mean) * (array_y[i] - y_mean))
        b = b + (array_x[i] - pow(x_mean, 2))
        c = c + (array_y[i] - pow(y_mean, 2))

    corr_coef = a / (math.sqrt(b * c))
    test = corr_coef * math.sqrt((len(array_y.data) - 2) / (1 - pow(corr_coef, 2)))

    # Save the result if file path is given
    if file:
        with open(file, "w") as text_file:
            print("Correlation coefficient: {}".format(corr_coef.values), file=text_file)
            print("Test value: {}".format(test.values), file=text_file)

    return {'correlation_coefficient': corr_coef, 'test_value': test}
