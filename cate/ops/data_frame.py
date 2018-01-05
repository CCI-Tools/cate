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

Operations for resources of type pandas.DataFrame, geopandas.GeoDataFrame and cate.core.types.GeoDataFrame which all
form (Feature) Attribute Tables (FAT).

Functions
=========
"""
import geopandas as gpd
import pandas as pd

from cate.core.op import op, op_input
from cate.core.types import VarName, DataFrameLike


@op(tags=['filter'], version='1.0')
@op_input('df', data_type=DataFrameLike)
@op_input('var', value_set_source='df', data_type=VarName)
def data_frame_min(df: DataFrameLike.TYPE, var: VarName.TYPE) -> pd.DataFrame:
    """
    Select the first record of a data frame for which the given variable value is minimal.

    :param df: The data frame or dataset.
    :param var: The variable.
    :return: A new, one-record data frame.
    """
    data_frame = DataFrameLike.convert(df)
    var_name = VarName.convert(var)
    row_index = data_frame[var_name].idxmin()
    row_frame = data_frame.loc[[row_index]]
    return _maybe_convert_to_geo_data_frame(data_frame, row_frame)


@op(tags=['filter'], version='1.0')
@op_input('df', data_type=DataFrameLike)
@op_input('var', value_set_source='df', data_type=VarName)
def data_frame_max(df: DataFrameLike.TYPE, var: VarName.TYPE) -> pd.DataFrame:
    """
    Select the first record of a data frame for which the given variable value is maximal.

    :param df: The data frame or dataset.
    :param var: The variable.
    :return: A new, one-record data frame.
    """
    data_frame = DataFrameLike.convert(df)
    var_name = VarName.convert(var)
    row_index = data_frame[var_name].idxmax()
    row_frame = data_frame.loc[[row_index]]
    return _maybe_convert_to_geo_data_frame(data_frame, row_frame)


@op(tags=['filter'], version='1.0')
@op_input('df', data_type=DataFrameLike)
@op_input('query_expr')
def data_frame_query(df: DataFrameLike.TYPE, query_expr: str) -> pd.DataFrame:
    pass


def _maybe_convert_to_geo_data_frame(data_frame, data_frame_2):
    if isinstance(data_frame, gpd.GeoDataFrame) and not isinstance(data_frame_2, gpd.GeoDataFrame):
        return gpd.GeoDataFrame(data_frame_2, crs=data_frame.crs)
    else:
        return data_frame_2
