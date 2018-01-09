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

Arithmetic operations

Functions
=========
"""

import xarray as xr
from xarray import ufuncs as xu

from cate.core.op import op, op_input, op_return
from cate.core.types import DatasetLike, VarName
from cate.util.monitor import Monitor
from cate.util.safe import safe_eval


@op(tags=['arithmetic'], version='1.0')
@op_input('ds', data_type=DatasetLike)
@op_return(add_history=True)
def ds_arithmetics(ds: DatasetLike.TYPE,
                   op: str,
                   monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Do arithmetic operations on the given dataset by providing a list of
    arithmetic operations and the corresponding constant. The operations will
    be applied to the dataset in the order in which they appear in the list.
    For example:
    'log,+5,-2,/3,*2'

    Currently supported arithmetic operations:
    log,log10,log2,log1p,exp,+,-,/,*

    where:
        log - natural logarithm
        log10 - base 10 logarithm
        log2 - base 2 logarithm
        log1p - log(1+x)
        exp - the exponential

    The operations will be applied element-wise to all arrays of the dataset.

    :param ds: The dataset to which to apply arithmetic operations
    :param op: A comma separated list of arithmetic operations to apply
    :param monitor: a progress monitor.
    :return: The dataset with given arithmetic operations applied
    """
    ds = DatasetLike.convert(ds)
    retset = ds
    with monitor.starting('Calculate result', total_work=len(op.split(','))):
        for item in op.split(','):
            with monitor.child(1).observing("Calculate"):
                item = item.strip()
                if item[0] == '+':
                    retset = retset + float(item[1:])
                elif item[0] == '-':
                    retset = retset - float(item[1:])
                elif item[0] == '*':
                    retset = retset * float(item[1:])
                elif item[0] == '/':
                    retset = retset / float(item[1:])
                elif item[:] == 'log':
                    retset = xu.log(retset)
                elif item[:] == 'log10':
                    retset = xu.log10(retset)
                elif item[:] == 'log2':
                    retset = xu.log2(retset)
                elif item[:] == 'log1p':
                    retset = xu.log1p(retset)
                elif item[:] == 'exp':
                    retset = xu.exp(retset)
                else:
                    raise ValueError('Arithmetic operation {} not'
                                     ' implemented.'.format(item[0]))

    return retset


@op(tags=['arithmetic'], version='1.0')
@op_return(add_history=True)
def diff(ds: xr.Dataset,
         ds2: xr.Dataset,
         monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Calculate the difference of two datasets (ds - ds2). This is done by
    matching variable names in the two datasets against each other and taking
    the difference of matching variables.

    If lat/lon/time extents differ between the datasets, the default behavior
    is to take the intersection of the datasets and run subtraction on that.
    However, broadcasting is possible. E.g. ds(lat/lon/time) - ds(lat/lon) is
    valid. In this case the subtrahend will be stretched to the size of
    ds(lat/lon/time) so that it can be subtracted. This also works if the
    subtrahend is a single time slice of arbitrary temporal position. In this
    case, the time dimension will be squeezed out leaving a lat/lon dataset.

    :param ds: The minuend dataset
    :param ds2: The subtrahend dataset
    :param monitor: a progress monitor.
    :return: The difference dataset
    """
    try:
        # Times do not intersect
        if 0 == len(ds.time - ds2.time) and \
                len(ds.time) == len(ds2.time):  # Times are the same length
            # If the datasets don't intersect in time dimension, a naive difference
            # would return empty data variables. Hence, the time coordinate has to
            # be dropped beforehand
            ds = ds.drop('time')
            ds2 = ds2.drop('time')
            return ds - ds2
    except AttributeError:
        # It is likely that the one operand is a lat/lon array that can be
        # broadcast against the other operand
        pass

    try:
        if 1 == len(ds2.time):
            # The subtrahend is a single time-slice -> squeeze 'time' dimension to
            # be able to broadcast is along minuend
            ds2 = ds2.squeeze('time', drop=True)
    except AttributeError:
        # Doesn't have a time dimension already
        pass
    except TypeError as e:
        if 'unsized object' in str(e):
            # The 'time' variable is a scalar
            pass
        else:
            raise TypeError(str(e))

    with monitor.observing("Subtract datasets"):
        diff = ds - ds2

    return diff


# noinspection PyIncorrectDocstring
@op(tags=['arithmetic'], version='1.0')
@op_input('ds', data_type=DatasetLike, nullable=True)
@op_input('expr')
@op_input('var', data_type=VarName)
@op_input('copy')
@op_input('_ctx', context=True)
@op_return(add_history=True)
def compute(ds: DatasetLike.TYPE,
            expr: str,
            var: VarName.TYPE,
            copy: bool = False,
            _ctx: dict = None,
            monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """

    :param ds: The primary dataset. If omitted, all variables need to be prefixed by their dataset resource names.
    :param expr: Math expression in which all *ds* variables may be used by name.
    :param var: The new variable's name.
    :param copy: Whether to copy all variables from *ds*.
    :param monitor: An optional progress monitor.
    :return: A new dataset with the new variable.
    """

    if _ctx is not None and 'value_cache' in _ctx:
        local_namespace = dict(_ctx['value_cache'])
    else:
        local_namespace = dict()

    if ds is not None:
        local_namespace.update(ds.data_vars)

    with monitor.observing("Computing variable"):
        data_array = safe_eval(expr, local_namespace=local_namespace)
        data_array.name = var

    if ds is not None and copy:
        new_ds = ds.copy()
        new_ds[var] = data_array
    else:
        new_ds = xr.Dataset(data_vars={var: data_array})
    return new_ds
