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

CLI/API data visualization operations

Components
==========

## plot_map

Plots a geospatial data slice on a world map. General python prompt usage:

```python
import matplotlib.pyplot as plt
from cate.ops.plot import plot_map

plot_map(dset)
plt.show()
```

General jupyter notebook usage:
```python
import matplotlib.pyplot as plt
from cate.ops.plot import plot_map

%matplotlib inline
plot_map(dset)
```
If a file path is given, the plot is saved.
Supported formats: eps, jpeg, jpg, pdf, pgf, png, ps, raw, rgba, svg,
svgz, tif, tiff

"""

import matplotlib

has_qt5agg = False
# noinspection PyBroadException
try:
    if not matplotlib.__version__.startswith('1.'):
        matplotlib.use('Qt5Agg')
        has_qt5agg = True
except:
    pass
if not has_qt5agg:
    matplotlib.use('Qt4Agg')

import matplotlib.pyplot as plt
from matplotlib.figure import Figure

import xarray as xr
import pandas as pd
import cartopy.crs as ccrs

from cate.core.op import op, op_input
from cate.core.types import VarName, DictLike, PolygonLike, TimeLike

PLOT_FILE_EXTENSIONS = ['eps', 'jpeg', 'jpg', 'pdf', 'pgf',
                        'png', 'ps', 'raw', 'rgba', 'svg',
                        'svgz', 'tif', 'tiff']
PLOT_FILE_FILTER = dict(name='Plot Outputs', extensions=PLOT_FILE_EXTENSIONS)


@op(tags=['plot', 'map'], no_cache=True)
@op_input('ds')
@op_input('var', value_set_source='ds', data_type=VarName)
@op_input('index', data_type=DictLike)
@op_input('time', data_type=TimeLike)
@op_input('region', data_type=PolygonLike)
@op_input('projection', value_set=['PlateCarree', 'LambertCylindrical', 'Mercator', 'Miller',
                                   'Mollweide', 'Orthographic', 'Robinson', 'Sinusoidal',
                                   'NorthPolarStereo', 'SouthPolarStereo'])
@op_input('central_lon', units='degrees', value_range=[-180, 180])
@op_input('file', file_open_mode='w', file_filters=[PLOT_FILE_FILTER])
def plot_map(ds: xr.Dataset,
             var: VarName.TYPE = None,
             index: DictLike.TYPE = None,
             time: TimeLike.TYPE = None,
             region: PolygonLike.TYPE = None,
             projection: str = 'PlateCarree',
             central_lon: float = 0.0,
             file: str = None) -> None:
    """
    Plot the given variable from the given dataset on a map with coastal lines.
    In case no variable name is given, the first encountered variable in the
    dataset is plotted. In case no time index is given, the first time slice
    is taken. It is also possible to set extents of the plot. If no extents
    are given, a global plot is created.

    The plot can either be shown using pyplot functionality, or saved,
    if a path is given. The following file formats for saving the plot
    are supported: eps, jpeg, jpg, pdf, pgf, png, ps, raw, rgba, svg,
    svgz, tif, tiff

    :param ds: xr.Dataset to plot
    :param var: variable name in the dataset to plot
    :param index: Optional index into the variable's data array. The *index* is a dictionary
                  that maps the variable's dimension names to constant labels. For example,
                  ``lat`` and ``lon`` are given in decimal degrees, while a ``time`` value may be provided as
                  datetime object or a date string. *index* may also be a comma-separated string of key-value pairs,
                  e.g. "lat=12.4, time='2012-05-02'".
    :param time: time slice index to plot
    :param region: Region to plot
    :param projection: name of a global projection, see http://scitools.org.uk/cartopy/docs/v0.15/crs/projections.html
    :param central_lon: central longitude of the projection in degrees
    :param file: path to a file in which to save the plot
    """
    if not isinstance(ds, xr.Dataset):
        raise NotImplementedError('Only raster datasets are currently supported')

    var_name = None
    if not var:
        for key in ds.data_vars.keys():
            var_name = key
            break
    else:
        var_name = VarName.convert(var)

    var = ds[var_name]
    index = DictLike.convert(index)

    # Validate time
    time = TimeLike.convert(time)

    sel_method = None
    if time is not None:
        if 'time' not in var.coords:
            raise ValueError('"time" is not a coordinate variable')
        sel_method = 'nearest'

    for dim_name in var.dims:
        if dim_name not in ('lat', 'lon'):
            if not index:
                index = dict()
            if dim_name not in index:
                if dim_name in var.coords:
                    index[dim_name] = var.coords[dim_name][0]
                else:
                    index[dim_name] = 0

    extents = None
    region = PolygonLike.convert(region)
    if region:
        lon_min, lat_min, lon_max, lat_max = region.bounds
        if not _check_bounding_box(lat_min, lat_max, lon_min, lon_max):
            raise ValueError('Provided plot extents do not form a valid bounding box '
                             'within [-180.0,+180.0,-90.0,+90.0]')
        extents = [lon_min, lon_max, lat_min, lat_max]

    # See http://scitools.org.uk/cartopy/docs/v0.15/crs/projections.html#
    if projection == 'PlateCarree':
        proj = ccrs.PlateCarree(central_longitude=central_lon)
    elif projection == 'LambertCylindrical':
        proj = ccrs.LambertCylindrical(central_longitude=central_lon)
    elif projection == 'Mercator':
        proj = ccrs.Mercator(central_longitude=central_lon)
    elif projection == 'Miller':
        proj = ccrs.Miller(central_longitude=central_lon)
    elif projection == 'Mollweide':
        proj = ccrs.Mollweide(central_longitude=central_lon)
    elif projection == 'Orthographic':
        proj = ccrs.Orthographic(central_longitude=central_lon)
    elif projection == 'Robinson':
        proj = ccrs.Robinson(central_longitude=central_lon)
    elif projection == 'Sinusoidal':
        proj = ccrs.Sinusoidal(central_longitude=central_lon)
    elif projection == 'NorthPolarStereo':
        proj = ccrs.NorthPolarStereo(central_longitude=central_lon)
    elif projection == 'SouthPolarStereo':
        proj = ccrs.SouthPolarStereo(central_longitude=central_lon)
    else:
        raise ValueError('illegal projection: "%s"' % projection)

    try:
        if index:
            var_data = var.sel(method=sel_method, **index)
        else:
            var_data = var
    except ValueError as e:
        print(e)
        var_data = var

    fig = plt.figure(figsize=(16, 8))
    ax = plt.axes(projection=proj)
    if extents:
        ax.set_extent(extents)
    else:
        ax.set_global()

    ax.coastlines()
    var_data.plot.contourf(ax=ax, transform=proj)
    if file:
        fig.savefig(file)


@op(tags=['plot'], no_cache=True)
@op_input('plot_type', value_set=['line', 'bar', 'barh', 'hist', 'box', 'kde',
                                  'area', 'pie', 'scatter', 'hexbin'])
@op_input('file', file_open_mode='w', file_filters=[PLOT_FILE_FILTER])
def plot_dataframe(df: pd.DataFrame,
                   plot_type: str = 'line',
                   file: str = None,
                   **kwargs) -> None:
    """
    Plot a dataframe.

    This is a wrapper of pandas.DataFrame.plot() function.

    For further documentation please see
    http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.plot.html

    :param df: A pandas dataframe to plot
    :param plot_type: Plot type
    :param file: path to a file in which to save the plot
    :param kwargs: Keyword arguments to pass to the underlying
    pandas.DataFrame.plot function
    """
    if not isinstance(df, pd.DataFrame):
        raise NotImplementedError('Only pandas dataframes are currently'
                                  ' supported')

    ax = df.plot(kind=plot_type, figsize=(16, 8), **kwargs)
    if file:
        fig = ax.get_figure()
        fig.savefig(file)


@op(tags=['plot'])
@op_input('var', value_set_source='ds', data_type=VarName)
@op_input('index', data_type=DictLike)
@op_input('file', file_open_mode='w', file_filters=[PLOT_FILE_FILTER])
def plot(ds: xr.Dataset,
         var: VarName.TYPE,
         index: DictLike.TYPE = None,
         fig: Figure = None,
         file: str = None) -> Figure:
    """
    Plot a variable, optionally save the figure in a file.

    The plot can either be shown using pyplot functionality, or saved,
    if a path is given. The following file formats for saving the plot
    are supported: eps, jpeg, jpg, pdf, pgf, png, ps, raw, rgba, svg,
    svgz, tif, tiff

    :param ds: Dataset that contains the variable named by *var*.
    :param var: The name of the variable to plot
    :param index: Optional index into the variable's data array. The *index* is a dictionary
                  that maps the variable's dimension names to constant labels. For example,
                  ``lat`` and ``lon`` are given in decimal degrees, while a ``time`` value may be provided as
                  datetime object or a date string. *index* may also be a comma-separated string of key-value pairs,
                  e.g. "lat=12.4, time='2012-05-02'".
    :param fig: optional figure from a previous ``plot`` call
    :param file: path to a file in which to save the plot
    """

    var = VarName.convert(var)
    var = ds[var]

    index = DictLike.convert(index)

    try:
        if index:
            var_data = var.sel(method='nearest', **index)
        else:
            var_data = var
    except ValueError:
        var_data = var

    fig = fig or plt.figure(figsize=(16, 8))
    var_data.plot()
    if file:
        fig.savefig(file)

    return fig if not in_notebook() else None


def _check_bounding_box(lat_min: float,
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
    Returns ``True`` if the module is running in IPython kernel,
    ``False`` if in IPython shell or other Python shell.
    """
    import sys
    ipykernel_in_sys_modules = 'ipykernel' in sys.modules
    print('###########################################', ipykernel_in_sys_modules)
    return ipykernel_in_sys_modules
