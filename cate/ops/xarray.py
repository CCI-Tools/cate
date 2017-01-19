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
This module provides general utility operations that wrap specific ``xarray`` functions.

The intention is to make available the ``xarray`` API as a set of general, domain-independent
utility functions.

All operations in this module are tagged with the ``"utility"`` tag.

"""

import xarray as xr

from cate.core.op import op_input, op


@op(tags=['internal', 'utility'])
@op_input('ds')
@op_input('lat', units='degree', value_range=[-90, 90])
@op_input('lon', units='degree', value_range=[-180, 180])
@op_input('time')
@op_input('indexers')
@op_input('method', value_set=['nearest', 'ffill', 'bfill'])
@op_input('indexers')
def sel(ds: xr.Dataset,
        lat: float = None,
        lon: float = None,
        time: str = None,
        indexers: dict = None,
        method: str = 'nearest') -> xr.Dataset:
    """
    Return a new dataset with each array indexed by tick labels along the specified dimension(s).

    This is a wrapper for the ``xarray.sel()`` function.

    For documentation refer to xarray documentation at
    http://xarray.pydata.org/en/stable/generated/xarray.Dataset.sel.html#xarray.Dataset.sel

    :param ds: The dataset from which to select.
    :param lat: Latitude value or range.
    :param lon: Longitude value or range.
    :param time: Time value or range.
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
    indexers = dict(indexers) if indexers is not None else {}
    if lat is not None:
        indexers.setdefault('lat', lat)
    if lon is not None:
        indexers.setdefault('lon', lon)
    if time is not None:
        indexers.setdefault('time', time)
    # Filter out non-existent coordinates
    indexers = {name: value for name, value in indexers.items() if name in ds.coords}
    return ds.sel(method=method, **indexers)
