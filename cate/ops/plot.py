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
except Exception:
    pass
if not has_qt5agg:
    matplotlib.use('Qt4Agg')

import matplotlib.pyplot as plt
from matplotlib.figure import Figure

import xarray as xr
import pandas as pd
import cartopy.crs as ccrs

from cate.core.op import op, op_input
from cate.core.types import VarName, DictLike, PolygonLike, TimeLike, DatasetLike

from cate.ops.plot_helpers import get_var_data
from cate.ops.plot_helpers import in_notebook
from cate.ops.plot_helpers import handle_plot_polygon

PLOT_FILE_EXTENSIONS = ['eps', 'jpeg', 'jpg', 'pdf', 'pgf',
                        'png', 'ps', 'raw', 'rgba', 'svg',
                        'svgz', 'tif', 'tiff']
PLOT_FILE_FILTER = dict(name='Plot Outputs', extensions=PLOT_FILE_EXTENSIONS)


@op(tags=['plot'], res_pattern='plot_{index}')
@op_input('ds')
@op_input('var', value_set_source='ds', data_type=VarName)
@op_input('indexers', data_type=DictLike)
@op_input('time', data_type=TimeLike)
@op_input('region', data_type=PolygonLike)
@op_input('projection', value_set=['PlateCarree', 'LambertCylindrical', 'Mercator', 'Miller',
                                   'Mollweide', 'Orthographic', 'Robinson', 'Sinusoidal',
                                   'NorthPolarStereo', 'SouthPolarStereo'])
@op_input('central_lon', units='degrees', value_range=[-180, 180])
@op_input('title')
@op_input('properties', data_type=DictLike)
@op_input('file', file_open_mode='w', file_filters=[PLOT_FILE_FILTER])
def plot_map(ds: xr.Dataset,
             var: VarName.TYPE = None,
             indexers: DictLike.TYPE = None,
             time: TimeLike.TYPE = None,
             region: PolygonLike.TYPE = None,
             projection: str = 'PlateCarree',
             central_lon: float = 0.0,
             title: str = None,
             properties: DictLike.TYPE = None,
             file: str = None) -> object:
    """
    Create a geographic map plot for the variable given by dataset *ds* and variable name *var*.

    Plots the given variable from the given dataset on a map with coastal lines.
    In case no variable name is given, the first encountered variable in the
    dataset is plotted. In case no *time* is given, the first time slice
    is taken. It is also possible to set extents of the plot. If no extents
    are given, a global plot is created.

    The plot can either be shown using pyplot functionality, or saved,
    if a path is given. The following file formats for saving the plot
    are supported: eps, jpeg, jpg, pdf, pgf, png, ps, raw, rgba, svg,
    svgz, tif, tiff

    :param ds: the dataset containing the variable to plot
    :param var: the variable's name
    :param indexers: Optional indexers into data array of *var*. The *indexers* is a dictionary
           or a comma-separated string of key-value pairs that maps the variable's dimension names
           to constant labels. e.g. "layer=4".
    :param time: time slice index to plot, can be a string "YYYY-MM-DD" or an integer number
    :param region: Region to plot
    :param projection: name of a global projection, see http://scitools.org.uk/cartopy/docs/v0.15/crs/projections.html
    :param central_lon: central longitude of the projection in degrees
    :param title: an optional title
    :param properties: optional plot properties for Python matplotlib,
           e.g. "bins=512, range=(-1.5, +1.5)"
           For full reference refer to
           https://matplotlib.org/api/lines_api.html and
           https://matplotlib.org/api/_as_gen/matplotlib.axes.Axes.contourf.html
    :param file: path to a file in which to save the plot
    :return: a matplotlib figure object or None if in IPython mode
    """
    if not isinstance(ds, xr.Dataset):
        raise NotImplementedError('Only gridded datasets are currently supported')

    var_name = None
    if not var:
        for key in ds.data_vars.keys():
            var_name = key
            break
    else:
        var_name = VarName.convert(var)
    var = ds[var_name]

    time = TimeLike.convert(time)
    indexers = DictLike.convert(indexers) or {}
    properties = DictLike.convert(properties) or {}

    extents = None
    bounds = handle_plot_polygon(region)
    if bounds:
        lon_min, lat_min, lon_max, lat_max = bounds
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

    figure = plt.figure(figsize=(8, 4))
    ax = plt.axes(projection=proj)
    if extents:
        ax.set_extent(extents, ccrs.PlateCarree())
    else:
        ax.set_global()

    ax.coastlines()
    var_data = get_var_data(var, indexers, time=time, remaining_dims=('lon', 'lat'))

    # transform keyword is for the coordinate our data is in, which in case of a
    # 'normal' lat/lon dataset is PlateCarree.
    var_data.plot.pcolormesh(ax=ax, transform=ccrs.PlateCarree(), subplot_kws={'projection': proj}, **properties)

    if title:
        ax.set_title(title)

    figure.tight_layout()

    if file:
        figure.savefig(file)

    return figure if not in_notebook() else ax


@op(tags=['plot'], res_pattern='plot_{index}')
@op_input('var', value_set_source='ds', data_type=VarName)
@op_input('time', data_type=TimeLike)
@op_input('indexers', data_type=DictLike)
@op_input('title')
@op_input('filled')
@op_input('properties', data_type=DictLike)
@op_input('file', file_open_mode='w', file_filters=[PLOT_FILE_FILTER])
def plot_contour(ds: xr.Dataset,
                 var: VarName.TYPE,
                 time: TimeLike.TYPE = None,
                 indexers: DictLike.TYPE = None,
                 title: str = None,
                 filled: bool = True,
                 properties: DictLike.TYPE = None,
                 file: str = None) -> Figure:
    """
    Create a contour plot of a variable given by dataset *ds* and variable name *var*.

    :param ds: the dataset containing the variable to plot
    :param var: the variable's name
    :param time: time slice index to plot, can be a string "YYYY-MM-DD" or an integer number
    :param indexers: Optional indexers into data array of *var*. The *indexers* is a dictionary
           or a comma-separated string of key-value pairs that maps the variable's dimension names
           to constant labels. e.g. "layer=4".
    :param title: an optional title
    :param filled: whether the regions between two contours shall be filled
    :param properties: optional plot properties for Python matplotlib,
           e.g. "bins=512, range=(-1.5, +1.5), label='Sea Surface Temperature'"
           For full reference refer to
           https://matplotlib.org/api/lines_api.html and
           https://matplotlib.org/devdocs/api/_as_gen/matplotlib.patches.Patch.html#matplotlib.patches.Patch
    :param file: path to a file in which to save the plot
    :return: a matplotlib figure object or None if in IPython mode
    """
    var_name = VarName.convert(var)
    if not var_name:
        raise ValueError("Missing value for 'var'")
    var = ds[var_name]

    time = TimeLike.convert(time)
    indexers = DictLike.convert(indexers) or {}
    properties = DictLike.convert(properties) or {}

    figure = plt.figure(figsize=(8, 4))
    ax = figure.add_subplot(111)

    var_data = get_var_data(var, indexers, time=time)
    if filled:
        var_data.plot.contourf(ax=ax, **properties)
    else:
        var_data.plot.contour(ax=ax, **properties)

    if title:
        ax.set_title(title)

    figure.tight_layout()

    if file:
        figure.savefig(file)

    return figure if not in_notebook() else None


@op(tags=['plot'], res_pattern='plot_{index}')
@op_input('ds', data_type=DatasetLike)
@op_input('var', value_set_source='ds', data_type=VarName)
@op_input('indexers', data_type=DictLike)
@op_input('title')
@op_input('properties', data_type=DictLike)
@op_input('file', file_open_mode='w', file_filters=[PLOT_FILE_FILTER])
def plot(ds: DatasetLike.TYPE,
         var: VarName.TYPE,
         indexers: DictLike.TYPE = None,
         title: str = None,
         properties: DictLike.TYPE = None,
         file: str = None) -> Figure:
    """
    Create a 1D/line or 2D/image plot of a variable given by dataset *ds* and variable name *var*.

    :param ds: Dataset or Dataframe that contains the variable named by *var*.
    :param var: The name of the variable to plot
    :param indexers: Optional indexers into data array of *var*. The *indexers* is a dictionary
           or a comma-separated string of key-value pairs that maps the variable's dimension names
           to constant labels. e.g. "lat=12.4, time='2012-05-02'".
    :param title: an optional plot title
    :param properties: optional plot properties for Python matplotlib,
           e.g. "bins=512, range=(-1.5, +1.5), label='Sea Surface Temperature'"
           For full reference refer to
           https://matplotlib.org/api/lines_api.html and
           https://matplotlib.org/devdocs/api/_as_gen/matplotlib.patches.Patch.html#matplotlib.patches.Patch
    :param file: path to a file in which to save the plot
    :return: a matplotlib figure object or None if in IPython mode
    """
    ds = DatasetLike.convert(ds)

    var_name = VarName.convert(var)
    if not var_name:
        raise ValueError("Missing value for 'var'")
    var = ds[var_name]

    indexers = DictLike.convert(indexers)
    properties = DictLike.convert(properties) or {}

    figure = plt.figure()
    ax = figure.add_subplot(111)

    var_data = get_var_data(var, indexers)
    var_data.plot(ax=ax, **properties)

    if title:
        ax.set_title(title)

    figure.tight_layout()

    if file:
        figure.savefig(file)

    return figure if not in_notebook() else None


@op(tags=['plot'], res_pattern='plot_{index}')
@op_input('ds1')
@op_input('ds2')
@op_input('var1', value_set_source='ds1', data_type=VarName)
@op_input('var2', value_set_source='ds2', data_type=VarName)
@op_input('indexers1', data_type=DictLike)
@op_input('indexers2', data_type=DictLike)
@op_input('title')
@op_input('properties', data_type=DictLike)
@op_input('file', file_open_mode='w', file_filters=[PLOT_FILE_FILTER])
def plot_scatter(ds1: xr.Dataset,
                 ds2: xr.Dataset,
                 var1: VarName.TYPE,
                 var2: VarName.TYPE,
                 indexers1: DictLike.TYPE = None,
                 indexers2: DictLike.TYPE = None,
                 title: str = None,
                 properties: DictLike.TYPE = None,
                 file: str = None) -> Figure:
    """
    Create a scatter plot of two variables of two variables given by datasets *ds1*, *ds2* and the
    variable names *var1*, *var2*.

    :param ds1: Dataset that contains the variable named by *var1*.
    :param ds2: Dataset that contains the variable named by *var2*.
    :param var1: The name of the first variable to plot
    :param var2: The name of the second variable to plot
    :param indexers1: Optional indexers into data array *var1*. The *indexers1* is a dictionary
           or comma-separated string of key-value pairs that maps the variable's dimension names
           to constant labels. e.g. "lat=12.4, time='2012-05-02'".
    :param indexers2: Optional indexers into data array *var2*.
    :param title: optional plot title
    :param properties: optional plot properties for Python matplotlib,
           e.g. "bins=512, range=(-1.5, +1.5), label='Sea Surface Temperature'"
           For full reference refer to
           https://matplotlib.org/api/lines_api.html and
           https://matplotlib.org/devdocs/api/_as_gen/matplotlib.patches.Patch.html#matplotlib.patches.Patch
    :param file: path to a file in which to save the plot
    :return: a matplotlib figure object or None if in IPython mode
    """
    var_name1 = VarName.convert(var1)
    var_name2 = VarName.convert(var2)
    if not var_name1:
        raise ValueError("Missing value for 'var1'")
    if not var_name2:
        raise ValueError("Missing value for 'var2'")
    var1 = ds1[var_name1]
    var2 = ds2[var_name2]

    indexers1 = DictLike.convert(indexers1) or {}
    indexers2 = DictLike.convert(indexers2) or {}
    properties = DictLike.convert(properties) or {}

    try:
        if indexers1:
            var_data1 = var1.sel(method='nearest', **indexers1)
            if not indexers2:
                indexers2 = indexers1

            var_data2 = var2.sel(method='nearest', **indexers2)
            remaining_dims = list(set(var1.dims) ^ set(indexers1.keys()))
            min_dim = max(var_data1[remaining_dims[0]].min(), var_data2[remaining_dims[0]].min())
            max_dim = min(var_data1[remaining_dims[0]].max(), var_data2[remaining_dims[0]].max())
            print(min_dim, max_dim)
            var_data1 = var_data1.where((var_data1[remaining_dims[0]] >= min_dim) & (var_data1[remaining_dims[0]] <= max_dim),
                                        drop=True)
            var_data2 = var_data2.where(
                (var_data2[remaining_dims[0]] >= min_dim) & (var_data2[remaining_dims[0]] <= max_dim),
                drop=True)
            print(var_data1)
            print(var_data2)
            if len(remaining_dims) is 1:
                print(remaining_dims)
                indexer3 = {remaining_dims[0]: var_data1[remaining_dims[0]].data}
                var_data2.reindex(method='nearest', **indexer3)
            else:
                print("Err!")
        else:
            var_data1 = var1
            var_data2 = var2
    except ValueError:
        var_data1 = var1
        var_data2 = var2

    figure = plt.figure(figsize=(12, 8))
    ax = figure.add_subplot(111)

    # var_data1.plot(ax = ax, **properties)
    ax.plot(var_data1.values, var_data2.values, '.', **properties)
    # var_data1.plot(ax=ax, **properties)
    xlabel_txt = "".join(", " + str(key) + " = " + str(value) for key, value in indexers1.items())
    xlabel_txt = var_name1 + xlabel_txt
    ylabel_txt = "".join(", " + str(key) + " = " + str(value) for key, value in indexers2.items())
    ylabel_txt = var_name2 + ylabel_txt
    ax.set_xlabel(xlabel_txt)
    ax.set_ylabel(ylabel_txt)
    figure.tight_layout()

    if title:
        ax.set_title(title)

    if file:
        figure.savefig(file)

    return figure if not in_notebook() else None


@op(tags=['plot'], res_pattern='plot_{index}')
@op_input('var', value_set_source='ds', data_type=VarName)
@op_input('indexers', data_type=DictLike)
@op_input('title')
@op_input('properties', data_type=DictLike)
@op_input('file', file_open_mode='w', file_filters=[PLOT_FILE_FILTER])
def plot_hist(ds: xr.Dataset,
              var: VarName.TYPE,
              indexers: DictLike.TYPE = None,
              title: str = None,
              properties: DictLike.TYPE = None,
              file: str = None) -> Figure:
    """
    Plot a variable, optionally save the figure in a file.

    The plot can either be shown using pyplot functionality, or saved,
    if a path is given. The following file formats for saving the plot
    are supported: eps, jpeg, jpg, pdf, pgf, png, ps, raw, rgba, svg,
    svgz, tif, tiff

    :param ds: Dataset that contains the variable named by *var*.
    :param var: The name of the variable to plot
    :param indexers: Optional indexers into data array of *var*. The *indexers* is a dictionary
           or a comma-separated string of key-value pairs that maps the variable's dimension names
           to constant labels. e.g. "lon=12.6, layer=3, time='2012-05-02'".
    :param title: an optional title
    :param properties: optional histogram plot properties for Python matplotlib,
           e.g. "bins=512, range=(-1.5, +1.5), label='Sea Surface Temperature'"
           For full reference refer to
           https://matplotlib.org/devdocs/api/_as_gen/matplotlib.pyplot.hist.html and
           https://matplotlib.org/devdocs/api/_as_gen/matplotlib.patches.Patch.html#matplotlib.patches.Patch
    :param file: path to a file in which to save the plot
    :return: a matplotlib figure object or None if in IPython mode
    """
    var_name = VarName.convert(var)
    if not var_name:
        raise ValueError("Missing value for 'var'")

    var = ds[var]

    indexers = DictLike.convert(indexers)
    properties = DictLike.convert(properties) or {}

    figure = plt.figure(figsize=(8, 4))
    ax = figure.add_subplot(111)
    figure.tight_layout()

    var_data = get_var_data(var, indexers)
    var_data.plot.hist(ax=ax, **properties)

    if title:
        ax.set_title(title)

    figure.tight_layout()

    if file:
        figure.savefig(file)

    return figure if not in_notebook() else None


@op(tags=['plot'],
    res_pattern='plot_{index}',
    deprecated="This operation is deprecated and will be removed in future versions. User plot() instead.")
@op_input('plot_type', value_set=['line', 'bar', 'barh', 'hist', 'box', 'kde',
                                  'area', 'pie', 'scatter', 'hexbin'])
@op_input('file', file_open_mode='w', file_filters=[PLOT_FILE_FILTER])
def plot_data_frame(df: pd.DataFrame,
                    plot_type: str = 'line',
                    file: str = None,
                    **kwargs) -> Figure:
    """
    Plot a data frame.
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
        raise NotImplementedError('"df" must be of type "pandas.DataFrame"')

    ax = df.plot(kind=plot_type, figsize=(8, 4), **kwargs)
    figure = ax.get_figure()
    if file:
        figure.savefig(file)

    return figure if not in_notebook() else None
