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

from cate.core.op import op, op_input
from cate.core.types import VarName, DictLike, PolygonLike

from cate.ops.plot_helpers import get_var_data
from cate.ops.plot_helpers import check_bounding_box

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
@op_input('properties', data_type=DictLike)
@op_input('file', file_open_mode='w', file_filters=[ANIMATION_FILE_FILTER])
def animate_map(ds: xr.Dataset,
                var: VarName.TYPE = None,
                indexers: DictLike.TYPE = None,
                region: PolygonLike.TYPE = None,
                projection: str = 'PlateCarree',
                central_lon: float = 0.0,
                title: str = None,
                properties: DictLike.TYPE = None,
                file: str = None) -> object:
    """
    Create a geographic map animation for the variable given by dataset *ds* and variable name *var*.

    Creates an animation of the given variable from the given dataset on a map with coastal lines.
    In case no variable name is given, the first encountered variable in the
    dataset is plotted.
    It is also possible to set extents of the plot. If no extents
    are given, a global plot is created.

    The animation can either be shown using pyplot functionality, or saved,
    if a path is given. The following file formats for saving the animation
    are supported: html

    :param ds: the dataset containing the variable to plot
    :param var: the variable's name
    :param indexers: Optional indexers into data array of *var*. The *indexers* is a dictionary
           or a comma-separated string of key-value pairs that maps the variable's dimension names
           to constant labels. e.g. "layer=4".
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

    indexers = DictLike.convert(indexers) or {}
    properties = DictLike.convert(properties) or {}

    extents = None
    region = PolygonLike.convert(region)
    if region:
        lon_min, lat_min, lon_max, lat_max = region.bounds
        if not check_bounding_box(lat_min, lat_max, lon_min, lon_max):
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

    figure = plt.figure(figsize=(8, 4))
    ax = plt.axes(projection=proj)
    if extents:
        ax.set_extent(extents)
    else:
        ax.set_global()

    ax.coastlines()
    var_data = get_var_data(var, indexers, time=var.time[0], remaining_dims=('lon', 'lat'))
    var_data.plot.contourf(ax=ax, transform=proj, add_colorbar=True, **properties)

    if title:
        ax.set_title(title)

    figure.tight_layout()

    def run(time):
        # Add monitor here.
        # Make it cancellable.
        # Add the possibility to calculate 'true' vmin and vmax.
        # Make sure we can see the animation in a Jupyter notebook
        # display(HTML(ops.animate_map(cc, var='cfc_low')))
        ax.clear()
        if title:
            ax.set_title(title)
        if extents:
            ax.set_extent(extents)
        else:
            ax.set_global()
        ax.coastlines()
        var_data = get_var_data(var, indexers, time=time,
                                remaining_dims=('lon', 'lat'))
        var_data.plot.contourf(ax=ax, transform=proj, add_colorbar=False, **properties)
        return ax

    anim = animation.FuncAnimation(figure, run, [t for t in var.time],
                                   interval=25, blit=False)

    anim_html = anim.to_jshtml()

    if file:
        with open(file, 'w') as outfile:
            outfile.write(anim_html)

    return anim_html
