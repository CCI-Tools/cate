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
This module provides general utility operations that wrap specific ``xarray`` functions.

The intention is to make available the ``xarray`` API as a set of general, domain-independent
utility functions.

All operations in this module are tagged with the ``"utility"`` tag.

"""

import pandas as pd
import xarray as xr

from cate.core.op import op, op_input, op_return
from cate.core.types import DatasetLike, PointLike, TimeLike, DictLike, Arbitrary, Literal


@op(tags=['utility', 'internal'])
@op_input('ds', data_type=DatasetLike)
@op_input('point', data_type=PointLike, units='degree')
@op_input('time', data_type=TimeLike)
@op_input('indexers', data_type=DictLike)
@op_input('method', value_set=['nearest', 'ffill', 'bfill'])
def sel(ds: DatasetLike.TYPE,
        point: PointLike.TYPE = None,
        time: TimeLike.TYPE = None,
        indexers: DictLike.TYPE = None,
        method: str = 'nearest') -> xr.Dataset:
    """
    Return a new dataset with each array indexed by tick labels along the specified dimension(s).

    This is a wrapper for the ``xarray.sel()`` function.

    For documentation refer to xarray documentation at
    http://xarray.pydata.org/en/stable/generated/xarray.Dataset.sel.html#xarray.Dataset.sel

    :param ds: The dataset from which to select.
    :param point: Optional geographic point given by longitude and latitude
    :param time: Optional time
    :param indexers: Keyword arguments with names matching dimensions and values given by scalars,
           slices or arrays of tick labels. For dimensions with multi-index, the indexer may also be
           a dict-like object with keys matching index level names.
    :param method: Method to use for inexact matches:
           * None: only exact matches
           * ``pad`` / ``ffill``: propagate last valid index value forward
           * ``backfill`` / ``bfill``: propagate next valid index value backward
           * ``nearest`` (default): use nearest valid index value
    :return: A new Dataset with the same contents as this dataset, except each variable and dimension
             is indexed by the appropriate indexers. In general, each variable's data will be a view of the
             variable's data in this dataset.
    """
    ds = DatasetLike.convert(ds)
    point = PointLike.convert(point)
    time = TimeLike.convert(time)
    indexers = DictLike.convert(indexers)
    indexers = dict(indexers or {})
    if point is not None:
        indexers.setdefault('lon', point.x)
        indexers.setdefault('lat', point.y)
    if time is not None:
        indexers.setdefault('time', time)
    # Filter out non-existent coordinates
    indexers = {name: value for name, value in indexers.items() if name in ds.coords}
    return ds.sel(method=method, **indexers)


@op(tags=['utility'])
def from_dataframe(df: pd.DataFrame) -> xr.Dataset:
    """
    Convert the given dataframe to an xarray dataset.

    This is a wrapper for the ``xarray.from_dataframe()`` function.

    For documentation refer to xarray documentation at
    http://xarray.pydata.org/en/stable/generated/xarray.Dataset.from_dataframe.html#xarray.Dataset.from_dataframe

    :param df: Dataframe to convert
    :return: A dataset created from the given dataframe
    """
    return xr.Dataset.from_dataframe(df)


@op(tags=['utility'])
@op_input('value', data_type=Arbitrary)
@op_return(data_type=Arbitrary)
def identity(value: Arbitrary.TYPE) -> Arbitrary.TYPE:
    """
    Return the given value.
    This operation can be useful to create constant resources to be used as input for other operations.

    :param value: An arbitrary (Python) value.
    """
    return value


@op(tags=['utility'])
@op_input('value', data_type=Literal)
@op_return(data_type=Arbitrary)
def literal(value: Literal.TYPE) -> Arbitrary.TYPE:
    """
    Return the given value.
    This operation can be useful to create constant resources to be used as input for other operations.

    :param value: An arbitrary (Python) literal.
    """
    return Literal.convert(value)
