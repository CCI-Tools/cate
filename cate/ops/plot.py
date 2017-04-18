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
import xarray as xr

matplotlib.use('Qt4Agg')
import matplotlib.pyplot as plt

import cartopy.crs as ccrs

from typing import Optional, Union

from cate.core.op import op, op_input
from cate.core.types import VarName, DictLike

PLOT_FILE_EXTENSIONS = ['eps', 'jpeg', 'jpg', 'pdf', 'pgf',
                        'png', 'ps', 'raw', 'rgba', 'svg',
                        'svgz', 'tif', 'tiff']
PLOT_FILE_FILTER = dict(name='Plot Outputs', extensions=PLOT_FILE_EXTENSIONS)


@op(tags=['plot', 'map'], no_cache=True)
@op_input('ds')
@op_input('var', value_set_source='ds', data_type=VarName)
@op_input('index', data_type=DictLike)
@op_input('lat_min', units='degrees', value_range=[-90, 90])
@op_input('lat_max', units='degrees', value_range=[-90, 90])
@op_input('lon_min', units='degrees', value_range=[-180, 180])
@op_input('lon_max', units='degrees', value_range=[-180, 180])
@op_input('projection', value_set=['PlateCarree', 'LambertCylindrical', 'Mercator', 'Miller',
                                   'Mollweide', 'Orthographic', 'Robinson', 'Sinusoidal',
                                   'NorthPolarStereo', 'SouthPolarStereo'])
@op_input('central_lon', units='degrees', value_range=[-180, 180])
@op_input('file', file_open_mode='w', file_filters=[PLOT_FILE_FILTER])
def plot_map(ds: xr.Dataset,
             var: VarName.TYPE = None,
             index: DictLike.TYPE = None,
             time: Union[str, int] = None,
             lat_min: float = None,
             lat_max: float = None,
             lon_min: float = None,
             lon_max: float = None,
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
    :param lat_min: minimum latitude extent to plot
    :param lat_max: maximum latitude extent to plot
    :param lon_min: minimum longitude extent to plot
    :param lon_max: maximum longitude extent to plot
    :param projection: name of a global projection, see http://scitools.org.uk/cartopy/docs/v0.15/crs/projections.html
    :param central_lon: central longitude of the projection in degrees
    :param file: path to a file in which to save the plot
    """
    if not isinstance(ds, xr.Dataset):
        raise NotImplementedError('Only raster datasets are currently '
                                  'supported')

    var_name = None
    if not var:
        for key in ds.data_vars.keys():
            var_name = key
            break
    else:
        var_name = VarName.convert(var)

    var = ds[var_name]
    index = DictLike.convert(index)

    if time and isinstance(time, int) and 'time' in var.coords:
        time = var.coords['time'][time]

    if time:
        if not index:
            index = dict()
        index['time'] = time

    for dim_name in var.dims:
        if dim_name not in ('lat', 'lon'):
            if not index:
                index = dict()
            if dim_name not in index:
                index[dim_name] = 0

    extents = None
    if not (lat_min is None and lat_max is None and lon_min is None and lon_max is None):
        if lat_min is None:
            lat_min = -90.0
        if lat_max is None:
            lat_max = 90.0
        if lon_min is None:
            lon_min = -180.0
        if lon_max is None:
            lon_max = 180.0
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
        raise ValueError('illegal projection')

    try:
        if index:
            var_data = var.sel(**index)
        else:
            var_data = var
    except ValueError:
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

        # TODO (Gailis, 03.10.16) Returning a figure results in two plots in
        # Jupyter
        # return fig


@op(tags=['plot'], no_cache=True)
@op_input('var', value_set_source='ds', data_type=VarName)
@op_input('index', data_type=DictLike)
@op_input('file', file_open_mode='w', file_filters=[PLOT_FILE_FILTER])
def plot(ds: xr.Dataset,
         var: VarName.TYPE,
         index: DictLike.TYPE = None,
         file: str = None) -> None:
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
    :param file: path to a file in which to save the plot
    """

    var = VarName.convert(var)
    var = ds[var]

    index = DictLike.convert(index)

    try:
        if index:
            var_data = var.sel(**index)
        else:
            var_data = var
    except ValueError:
        var_data = var

    fig = plt.figure(figsize=(16, 8))
    var_data.plot()
    if file:
        fig.savefig(file)


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
