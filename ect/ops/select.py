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

Select variables in a dataset.

Components
==========
"""

import xarray as xr
import re

from ect.core.op import op, op_input, op_return


@op(tags=['select', 'subset'])
@op_input('ds')
@op_input('variable_names')
@op_return()
def select_variables(ds: xr.Dataset, variable_names: str = None) -> xr.Dataset:
    """
    Filter the dataset, by leaving only the desired variables. The original dataset
    information, including original coordinates, is preserved.

    If the selected variable has meta-variables associated with it, these will
    be preserved in the database as well.

    .. _regex: https://docs.python.org/3.5/library/re.html
    .. regexr.com

    :param ds: The dataset.
    :param variable_names: A regex pattern that identifies the variables to keep.
    For example, to simply select two variables to keep, use regex OR operator
    'variable_name|variable_name2'. Selection 'variable_name' will select the given
    variable, along with any auxiliary variables 'variable_name_xxxx'. To select a single
    variable explicitly one can use 'variable_name\Z'.
    :return: A filtered dataset
    """
    if not variable_names:
        return ds

    dropped_var_names = list(ds.data_vars.keys())

    prog = re.compile(variable_names)
    for dropped_var_name in list(dropped_var_names):
        if prog.match(dropped_var_name):
            dropped_var_names.remove(dropped_var_name)

    return ds.drop(dropped_var_names)
