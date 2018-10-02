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
import math
from typing import Dict, Any, Mapping, Tuple

import geopandas
import geopandas as gpd
import numpy
import numpy as np
import pandas
import pandas as pd
import scipy
import scipy as sp
import xarray
import xarray as xr

from cate.core.op import op, op_input, op_return
from cate.core.types import DatasetLike, ValidationError, DataFrameLike
from cate.util.monitor import Monitor
from cate.util.safe import safe_exec


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
                    retset = np.log(retset)
                elif item[:] == 'log10':
                    retset = np.log10(retset)
                elif item[:] == 'log2':
                    retset = np.log2(retset)
                elif item[:] == 'log1p':
                    retset = np.log1p(retset)
                elif item[:] == 'exp':
                    retset = np.exp(retset)
                else:
                    raise ValidationError('Arithmetic operation {} not'
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
@op_input('df', data_type=DataFrameLike, nullable=True, default_value=None)
@op_input('script', script_lang="python")
@op_input('copy')
@op_input('_ctx', context=True)
def compute_data_frame(df: DataFrameLike.TYPE,
                       script: str,
                       copy: bool = False,
                       _ctx: dict = None,
                       monitor: Monitor = Monitor.NONE) -> pd.DataFrame:
    """
    Compute a new data frame from the given Python *script*.
    The argument *script* must be valid Python code or a single expression comprising at least one
    value assignment of the form {name} = {expr}. Multiple assignments can be done on multiple lines
    or on a single line separated by semicolons.

    {expr} may reference variables in the given context dataset *ds* or other resources and their variables
    from the current workflow.
    In the latter case, use the dot operator to select a variable from a dataset resource.

    Any new variable in *script* whose name does not begin with and underscore ('_') and
    that has an appropriate data type will be added to the new data frame.

    The following packages are available in the *script*:

    * ``geopandas``, ``gpd``: The ``geopandas`` top-level package (http://geopandas.org/)
    * ``math``: The standard Python ``math`` library (https://docs.python.org/3/library/math.html)
    * ``numpy``, ``np``: The ``numpy`` top-level package (https://docs.scipy.org/doc/numpy/reference/)
    * ``pandas``, ``pd``: The ``pandas`` top-level package (http://pandas.pydata.org/pandas-docs/stable/api.html)
    * ``scipy``, ``sp``: The ``scipy`` top-level package (https://docs.scipy.org/doc/scipy/reference/)
    * ``xarray``, ``xr``: The ``xarray`` top-level package (http://xarray.pydata.org/en/stable/api.html)

    :param df: Optional context data frame. If provided, all series of this data frame are
           directly accessible in the *script*.
           If omitted, all series (variables) of other data frame (dataset) resources need to be prefixed
           by their resource name.
    :param script: Valid Python expression comprising at least one assignment of the form {name} = {expr}.
    :param copy: Whether to copy all series from *df*.
    :param monitor: An optional progress monitor.
    :return: A new data frame object.
    """
    series = _exec_script(script, (pd.Series, np.ndarray, float, int), _ctx, df, monitor)

    if df is not None and copy:
        new_df = df.copy()
        for name, data in series.items():
            new_df[name] = data
    else:
        max_size = 0
        for data in series.values():
            try:
                size = len(data)
            except TypeError:
                size = 1
            max_size = max(max_size, size)
        index = pd.Int64Index(data=np.arange(max_size), name='id')
        new_df = pd.DataFrame(data=series, index=index)

    return new_df


# noinspection PyIncorrectDocstring
@op(tags=['arithmetic'], version='1.0')
@op_input('ds', data_type=DatasetLike, nullable=True, default_value=None)
@op_input('script', script_lang="python")
@op_input('copy')
@op_input('_ctx', context=True)
def compute_dataset(ds: DatasetLike.TYPE,
                    script: str,
                    copy: bool = False,
                    _ctx: dict = None,
                    monitor: Monitor = Monitor.NONE) -> xr.Dataset:
    """
    Compute a new dataset from the given Python *script*.
    The argument *script* must be valid Python code or a single expression comprising at least one
    value assignment of the form {name} = {expr}. Multiple assignments can be done on multiple lines
    or on a single line separated by semicolons.

    {expr} may reference variables in the given context dataset *ds* or other resources and their variables
    from the current workflow.
    In the latter case, use the dot operator to select a variable from a dataset resource.

    Any new variable in *script* whose name does not begin with and underscore ('_') and
    that has an appropriate data type will be added to the new dataset.

    The following packages are available in the *script*:

    * ``geopandas``, ``gpd``: The ``geopandas`` top-level package (http://geopandas.org/)
    * ``math``: The standard Python ``math`` library (https://docs.python.org/3/library/math.html)
    * ``numpy``, ``np``: The ``numpy`` top-level package (https://docs.scipy.org/doc/numpy/reference/)
    * ``pandas``, ``pd``: The ``pandas`` top-level package (http://pandas.pydata.org/pandas-docs/stable/api.html)
    * ``scipy``, ``sp``: The ``scipy`` top-level package (https://docs.scipy.org/doc/scipy/reference/)
    * ``xarray``, ``xr``: The ``xarray`` top-level package (http://xarray.pydata.org/en/stable/api.html)

    :param ds: Optional context dataset. If provided, all variables of this dataset are
           directly accessible in the *script*.
           If omitted, all variables (series) of other dataset (data frame) resources need to be prefixed
           by their resource name.
    :param script: Valid Python expression comprising at least one assignment of the form {name} = {expr}.
    :param copy: Whether to copy all variables from *ds*.
    :param monitor: An optional progress monitor.
    :return: A new dataset object.
    """
    data_vars = _exec_script(script, (xr.DataArray, np.ndarray, float, int), _ctx, ds, monitor)

    if ds is not None and copy:
        new_ds = ds.copy()
        for name, data in data_vars.items():
            new_ds[name] = data
    else:
        new_ds = xr.Dataset(data_vars=data_vars)

    return new_ds


def _exec_script(script: str,
                 element_types: Tuple[type, ...],
                 operation_context: Mapping[str, Any] = None,
                 context_object: Mapping[str, Any] = None,
                 monitor: Monitor = Monitor.NONE) -> Dict[str, Any]:
    """
    Helper for compute_dataset() and compute_data_frame().
    """
    if not script:
        raise ValidationError(f'Python script must not be empty')

    # Include common libraries
    orig_namespace = dict(
        gpd=gpd,
        geopandas=geopandas,
        math=math,
        np=np,
        numpy=numpy,
        pd=pd,
        pandas=pandas,
        sp=sp,
        scipy=scipy,
        xr=xr,
        xarray=xarray,
    )

    if operation_context is not None and 'value_cache' in operation_context:
        orig_namespace.update(operation_context['value_cache'])

    if context_object is not None:
        orig_namespace.update(context_object)

    local_namespace = dict(orig_namespace)

    with monitor.observing("Executing script"):
        try:
            safe_exec(script, local_namespace=local_namespace)
        except BaseException as e:
            raise ValidationError(f'Error in Python script: {e}') from e

    elements = dict()
    for name, element in local_namespace.items():
        if not name.startswith('_'):
            if isinstance(element, element_types):
                if name not in orig_namespace or element is not orig_namespace[name]:
                    elements[name] = element

    return elements
