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
This module provides general utility operations that wrap specific ``pandas`` functions.

The intention is to make available parts of the ``pandas`` API as a set of general,
domain-independent utility functions.

All operations in this module are tagged with the ``"utility"`` tag.

"""

import pandas as pd

from cate.core.op import op_input, op


@op(tags=['utility'])
@op_input('method', value_set=['backfill', 'bfill', 'pad', 'ffill'])
def fillna(df: pd.DataFrame,
           value: float=None,
           method: str=None,
           limit: int=None,
           **kwargs) -> pd.DataFrame:
    """
    Return a new dataframe with NaN values filled according to the given value
    or method.

    This is a wrapper for the ``pandas.fillna()`` function For additional
    keyword arguments and information refer to pandas documentation at
    http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.fillna.html

    :param df: The dataframe to fill
    :param value: Value to fill
    :param method: Method according to which to fill NaN. ffill/pad will
    propagate the last valid observation to the next valid observation.
    backfill/bfill will propagate the next valid observation back to the last
    valid observation.
    :param limit: Maximum number of NaN values to forward/backward fill.
    """
    return pd.fillna(value, method, limit, **kwargs)
