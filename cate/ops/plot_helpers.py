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

Helpers for data visualization operations (plot and animate).

Components
==========

"""


def check_bounding_box(lat_min: float,
                       lat_max: float,
                       lon_min: float,
                       lon_max: float) -> bool:
    """
    Check if the provided [lat_min, lat_max, lon_min, lon_max] extents
    are sane.
    """
    if lat_min >= lat_max:
        return False

    if lon_min >= lon_max:
        return False

    if lat_min < -90.0:
        return False

    if lat_max > 90.0:
        return False

    if lon_min < -180.0:
        return False

    if lon_max > 180.0:
        return False

    return True


def in_notebook():
    """
    Return ``True`` if the module is running in IPython kernel,
    ``False`` if in IPython shell or other Python shell.
    """
    import sys
    ipykernel_in_sys_modules = 'ipykernel' in sys.modules
    # print('###########################################', ipykernel_in_sys_modules)
    return ipykernel_in_sys_modules


def get_var_data(var, indexers: dict, time=None, remaining_dims=None):
    """Select an arbitrary piece of an xarray dataset by using indexers."""
    if time is not None:
        indexers = dict(indexers) if indexers else dict()
        indexers['time'] = time

    if indexers:
        var = var.sel(method='nearest', **indexers)

    if remaining_dims:
        isel_indexers = {dim_name: 0 for dim_name in var.dims if dim_name not in remaining_dims}
        var = var.isel(**isel_indexers)

    return var
