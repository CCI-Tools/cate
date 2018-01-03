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

Operations for Feature Attribute Tables (FAT).

Functions
=========
"""
import pandas as pd

from cate.core.op import op, op_input
from cate.core.types import VarName, DataFrameLike


@op(tags=['filter', 'fat'], version='1.0')
@op_input('df', data_type=DataFrameLike)
@op_input('var', value_set_source='df', data_type=VarName)
def fat_min(df: DataFrameLike.TYPE, var: VarName.TYPE) -> pd.DataFrame:
    """
    Get a one-record data frame with the minimum value in the given variable.

    :param df: The data frame or dataset.
    :param var: The variable.
    :return: A new one-record dataframe.
    """
    data_frame = DataFrameLike.convert(df)
    var_name = VarName.convert(var)
    idx = data_frame[var_name].idxmin()
    return data_frame.loc[idx].to_frame().transpose()


@op(tags=['filter', 'fat'], version='1.0')
@op_input('df', data_type=DataFrameLike)
@op_input('var', value_set_source='df', data_type=VarName)
def fat_max(df: DataFrameLike.TYPE, var: VarName.TYPE) -> pd.DataFrame:
    """
    Get a one-record data frame with the maximum value in the given variable.

    :param df: The data frame or dataset.
    :param var: The variable.
    :return: A new one-record dataframe.
    """
    data_frame = DataFrameLike.convert(df)
    var_name = VarName.convert(var)
    idx = data_frame[var_name].idxmax()
    return data_frame.loc[idx].to_frame().transpose()


@op(tags=['filter', 'fat'], version='1.0')
@op_input('df', data_type=DataFrameLike)
@op_input('query_expr')
def fat_query(df: DataFrameLike.TYPE, query_expr: str) -> pd.DataFrame:
    pass

