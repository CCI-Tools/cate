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
from cate.util.monitor import Monitor


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


@op(tags=['utility'])
@op_input('step_duration', units='seconds')
def no_op(num_steps: int = 10,
          step_duration: float = 0.5,
          fail_before: bool = False,
          fail_after: bool = False,
          monitor: Monitor = Monitor.NONE) -> bool:
    """
    An operation that basically does nothing but spending configurable time.
    It may be useful for testing purposes.

    :param num_steps: Number of steps to iterate.
    :param step_duration: How much time to spend in each step in seconds.
    :param fail_before: If the operation should fail before spending time doing nothing.
    :param fail_after: If the operation should fail after spending time doing nothing.
    :param monitor: A progress monitor.
    :return: Always True
    """
    import time
    monitor.start('Computing nothing', num_steps)
    if fail_before:
        raise ValueError('Intentionally failed before doing anything.')
    for i in range(num_steps):
        time.sleep(step_duration)
        monitor.progress(1.0, 'Step %s of %s doing nothing' % (i + 1, num_steps))
    if fail_after:
        raise ValueError('Intentionally failed after doing nothing.')
    monitor.done()
    return True


@op(tags=['utility', 'internal'])
@op_input('method', value_set=['backfill', 'bfill', 'pad', 'ffill'])
def pandas_fillna(df: pd.DataFrame,
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
    :return: A dataframe with nan values filled with the given value or according to the given method.
    """
    # The following code is needed, because Pandas treats any kw given in kwargs as being set, even if just None.
    kwargs = dict(kwargs)
    if value:
        kwargs.update(value=value)
    if method:
        kwargs.update(method=method)
    if limit:
        kwargs.update(limit=limit)

    return df.fillna(**kwargs)
