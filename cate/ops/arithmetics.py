# The MIT License (MIT)
# Copyright (c) 2016 by the Cate Development Team and contributors
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

from cate.core.op import op
from xarray import ufuncs as xu
import xarray as xr


@op(tags=['arithmetic'])
def ds_arithmetics(ds: xr.Dataset,
                op: str) -> xr.Dataset:
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
    :return: The dataset with given arithmetic operations applied
    """
    retset = ds
    for item in op.split(','):
        if item[0] == '+':
            retset = retset + item[1:]
        elif item[0] == '-':
            retset = retset - item[1:]
        elif item[0] == '*':
            retset = retset * item[1:]
        elif item[0] == '/':
            retset = retset / item[1:]
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
                             'implemented.'.format(item[0]))

        return retset
