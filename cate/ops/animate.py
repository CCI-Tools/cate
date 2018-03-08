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

CLI/API data animation operations

Components
==========

## animate_map

Animates a geospatial data slice on a world map.

General jupyter notebook usage:
```python
import cate.ops as ops
from IPython.core.display import display, HTML
dset = ops.open_dataset('local.dataset_name')
display(HTML(ops.animate_map(cc, var='var_name')))
```

If a file path is given, the plot is saved.
Supported formats: html

"""
import os
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

import matplotlib.animation as animation
import matplotlib.pyplot as plt

import cartopy.crs as ccrs
import xarray as xr
import numpy as np

from cate.core.op import op, op_input
from cate.core.types import VarName, DictLike, PolygonLike, HTML
from cate.util.monitor import Monitor

from cate.ops.plot_helpers import (get_var_data,
                                   handle_plot_polygon,
                                   determine_cmap_params)

ANIMATION_FILE_FILTER = dict(name='Animation Outputs', extensions=['html', ])


@op(tags=['plot'], res_pattern='animation_{index}')
@op_input('ds')
@op_input('var', value_set_source='ds', data_type=VarName)
@op_input('indexers', data_type=DictLike)
@op_input('region', data_type=PolygonLike)
@op_input('projection', value_set=['PlateCarree', 'LambertCylindrical', 'Mercator', 'Miller',
                                   'Mollweide', 'Orthographic', 'Robinson', 'Sinusoidal',
                                   'NorthPolarStereo', 'SouthPolarStereo'])
@op_input('central_lon', units='degrees', value_range=[-180, 180])
@op_input('title')
@op_input('cmap_params', data_type=DictLike)
@op_input('plot_properties', data_type=DictLike)
@op_input('file', file_open_mode='w', file_filters=[ANIMATION_FILE_FILTER])
def animate_map(ds: xr.Dataset,
                var: VarName.TYPE = None,
                animate_dim: str = 'time',
                true_range: bool = False,
                indexers: DictLike.TYPE = None,
                region: PolygonLike.TYPE = None,
                projection: str = 'PlateCarree',
                central_lon: float = 0.0,
                title: str = None,
                cmap_params: DictLike.TYPE = None,
                plot_properties: DictLike.TYPE = None,
                file: str = None,
                monitor: Monitor = Monitor.NONE) -> HTML:
    """
    Create a geographic map animation for the variable given by dataset *ds* and variable name *var*.

    Creates an animation of the given variable from the given dataset on a map with coastal lines.
    In case no variable name is given, the first encountered variable in the
    dataset is animated.
    It is also possible to set extents of the animation. If no extents
    are given, a global animation is created.

    The following file formats for saving the animation are supported: html

    :param ds: the dataset containing the variable to animate
    :param var: the variable's name
    :param animate_dim: Dimension to animate, if none given defaults to time.
    :param true_range: If True, calculates colormap and colorbar configuration parameters from the
    whole dataset. Can potentially take a lot of time. Defaults to False, in which case the colormap
    is calculated from the first frame.
    :param indexers: Optional indexers into data array of *var*. The *indexers* is a dictionary
           or a comma-separated string of key-value pairs that maps the variable's dimension names
           to constant labels. e.g. "layer=4".
    :param region: Region to animate
    :param projection: name of a global projection, see http://scitools.org.uk/cartopy/docs/v0.15/crs/projections.html
    :param central_lon: central longitude of the projection in degrees
    :param title: an optional title
    :param cmap_params: optional additional colormap configuration parameters,
           e.g. "vmax=300, cmap='magma'"
           For full reference refer to
           http://xarray.pydata.org/en/stable/generated/xarray.plot.contourf.html
    :param plot_properties: optional plot properties for Python matplotlib,
           e.g. "bins=512, range=(-1.5, +1.5)"
           For full reference refer to
           https://matplotlib.org/api/lines_api.html and
           https://matplotlib.org/api/_as_gen/matplotlib.axes.Axes.contourf.html
    :param file: path to a file in which to save the animation
    :param monitor: A progress monitor.
    :return: An animation in HTML format
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

    try:
        var = ds[var_name]
    except KeyError:
        raise ValueError('Provided variable name "{}" does not exist in the given dataset'.format(var_name))

    indexers = DictLike.convert(indexers) or {}
    properties = DictLike.convert(plot_properties) or {}
    cmap_params = DictLike.convert(cmap_params) or {}

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

    if not animate_dim:
        animate_dim = 'time'

    indexers[animate_dim] = var[animate_dim][0]

    var_data = get_var_data(var, indexers, remaining_dims=('lon', 'lat'))

    with monitor.starting("animate", len(var[animate_dim]) + 3):
        if true_range:
            data_min, data_max = _get_min_max(var, monitor=monitor)
        else:
            data_min, data_max = _get_min_max(var_data, monitor=monitor)

        cmap_params = determine_cmap_params(data_min, data_max, **cmap_params)
        plot_kwargs = {**properties, **cmap_params}

        # Plot the first frame to set-up the axes with the colorbar properly
        var_data.plot.contourf(ax=ax, transform=ccrs.PlateCarree(), subplot_kws={'projection': proj},
                               add_colorbar=True, **plot_kwargs)
        if title:
            ax.set_title(title)
        figure.tight_layout()
        monitor.progress(1)

        def run(value):
            ax.clear()
            if extents:
                ax.set_extent(extents, ccrs.PlateCarree())
            else:
                ax.set_global()
            ax.coastlines()
            indexers[animate_dim] = value
            var_data = get_var_data(var, indexers, remaining_dims=('lon', 'lat'))
            var_data.plot.contourf(ax=ax, transform=ccrs.PlateCarree(), subplot_kws={'projection': proj},
                                   add_colorbar=False, **plot_kwargs)
            if title:
                ax.set_title(title)
            monitor.progress(1)
            return ax
        anim = animation.FuncAnimation(figure, run, [i for i in var[animate_dim]],
                                       interval=25, blit=False, repeat=False)
        anim_html = anim.to_jshtml()

        # Prevent the animation for running after it's finished
        del anim

        # Delete the rogue temp-file
        try:
            os.remove('None0000000.png')
        except FileNotFoundError:
            pass

        if file:
            with open(file, 'w') as outfile:
                outfile.write(anim_html)
                monitor.progress(1)

    return anim_html


def _get_min_max(data, monitor=None):
    """
    Get min and max of a dataset, while accounting for all-NaN
    datasets and observing it with the monitor.
    """
    with monitor.child(1).observing("find minimum"):
        data_min = data.min()
    if np.isnan(data_min):
        # Handle all-NaN dataset
        raise ValueError('Can not create an animation of a dataset containing only NaN values.')
    else:
        with monitor.child(1).observing("find maximum"):
            data_max = data.max()

    return (data_min, data_max)
