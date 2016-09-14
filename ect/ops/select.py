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

from ect.core.op import op, op_input, op_return

@op(tags=['select'])
@op_input('ds', description='Input dataset')
@op_input('variable_names', description='List of regex patterns that identify the variables to keep')
@op_input('regex', description='If True, variable names are expected to contain regex_ patterns')
@op_input('copy', decription='If True, the returned dataset will likely contain data copies of the original data')
@op_return(description='A dataset that contains the selected variables.')
def select_variables(ds: xr.Dataset, variable_names: tuple = None, regex = False, copy: bool = False) -> xr.Dataset:
# TODO (mz, 201607211): parameter 'copy' is not implemented
    """
    Filter the dataset, by leaving only desired variables.

    Whether the ``select`` method returns a view or a copy of the underlying data depends on the concrete format and
    nature of the data.

    .. _regex: https://docs.python.org/3.5/library/re.html

    :param ds: The dataset.
    :param variable_names: List of regex patterns that identify the variables to keep.
    :param regex: If ``True``, *variable_names* is expected to contain regex_ patterns.
    :param copy: If ``True``, the returned dataset will likely contain data copies of the original dataset
    :return: a new, filtered dataset of type :py:class:`xr.Dataset`
    """
    if not variable_names:
        return ds

    dropped_var_names = list(ds.data_vars.keys())

    if not regex:
        for var_name in variable_names:
            if var_name in dropped_var_names:
                dropped_var_names.remove(var_name)
    else:
        import re
        for var_name_pattern in variable_names:
            prog = re.compile(var_name_pattern)
            for dropped_var_name in list(dropped_var_names):
                if prog.match(dropped_var_name):
                    dropped_var_names.remove(dropped_var_name)

    return ds.drop(dropped_var_names)
