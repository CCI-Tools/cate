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
from cate.core.types import PolygonLike, ValidationError
from cate.core.opimpl import get_extents
from cate.util.im import ensure_cmaps_loaded


def handle_plot_polygon(region: PolygonLike.TYPE = None):
    """
    Return extents of the given PolygonLike.

    :param region: PolygonLike to introspect
    :return: extents
    """
    if region is None:
        return None

    extents, explicit_coords = get_extents(region)

    lon_min, lat_min, lon_max, lat_max = extents

    if not check_bounding_box(lat_min, lat_max, lon_min, lon_max):
        raise ValidationError('Provided plot extents do not form a valid bounding box '
                              'within [-180.0,+180.0,-90.0,+90.0]')
    return extents


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


def get_var_data(var, indexers: dict, remaining_dims=None):
    """Select an arbitrary piece of an xarray dataset by using indexers."""
    if indexers:
        if remaining_dims:
            for dim in remaining_dims:
                if dim not in var.dims:
                    raise ValidationError(f'The specified dataset does not have a dimension called \'{dim}\'.')
                if dim in indexers:
                    raise ValidationError(f'Dimension \'{dim}\' is also specified as indexers. Please ensure that a '
                                          f'dimension is used exclusively either as indexers or as the selected '
                                          f'dimension.')
        for dim in indexers:
            if dim not in var.dims:
                raise ValidationError(f'The specified dataset does not have a dimension called \'{dim}\'.')
        var = var.sel(method='nearest', **indexers)

    if remaining_dims:
        isel_indexers = {dim_name: 0 for dim_name in var.dims if dim_name not in remaining_dims}
        var = var.isel(**isel_indexers)

    return var


def get_vars_data(ds, indexers: dict, remaining_dims=None):
    """Select an arbitrary piece of an xarray dataset by using indexers."""
    # to avoid the original dataset being affected (especially useful in unit tests)
    ds = ds.copy()

    if indexers:
        invalid_indexers = list(indexers)
        for var_name in ds:
            if ds[var_name].name in ds[var_name].dims:
                continue
            var_indexers = {}
            if remaining_dims:
                for dim in remaining_dims:
                    if dim not in ds[var_name].dims:
                        raise ValidationError(f'The specified dataset does not have a dimension called \'{dim}\'.')
                    if dim in indexers:
                        raise ValidationError(
                            f'Dimension \'{dim}\' is also specified as indexers. Please ensure that a '
                            f'dimension is used exclusively either as indexers or as the selected '
                            f'dimension.')
            for dim in ds[var_name].dims:
                if dim in indexers:
                    var_indexers[dim] = indexers[dim]
            for dim in invalid_indexers:
                if dim in ds[var_name].dims:
                    invalid_indexers.remove(dim)
            ds[var_name] = ds[var_name].sel(method='nearest', **var_indexers)

            if remaining_dims:
                isel_indexers = {dim_name: 0 for dim_name in ds[var_name].dims if dim_name not in remaining_dims}
                ds[var_name] = ds[var_name].isel(**isel_indexers)

        if len(invalid_indexers) > 0:
            raise ValidationError(f'There are dimensions specified in indexers but do not match dimensions in '
                                  f'any variables: {invalid_indexers}')

    return ds


# determine_cmap_params is adapted from Xarray through Seaborn:
# https://github.com/pydata/xarray/blob/master/xarray/plot/utils.py#L151
# https://github.com/mwaskom/seaborn/blob/v0.6/seaborn/matrix.py#L158
# Used under the terms of Seaborn's license:
# https://github.com/mwaskom/seaborn/blob/v0.8.1/LICENSE
#
# _determine_extend, _build_discrete_cmap, _color_palette, _is_scalar are
# adapted from Xarray and used under Xarray license:
# https://github.com/pydata/xarray/blob/v0.10.0/LICENSE

ROBUST_QUANTILE = 0.02


def determine_cmap_params(data_min, data_max, vmin=None, vmax=None, cmap=None,
                          center=None, robust=False, extend=None,
                          levels=None, filled=True, norm=None):
    """
    Use some heuristics to set good defaults for colorbar and range.

    :param plot_data: Data to use to determine colormap parameters
    :return: A dictionary containing calculated parameters
    """
    import matplotlib as mpl
    import numpy as np

    # Setting center=False prevents a divergent cmap
    possibly_divergent = center is not False

    # Set center to 0 so math below makes sense but remember its state
    center_is_none = False
    if center is None:
        center = 0
        center_is_none = True

    # Setting both vmin and vmax prevents a divergent cmap
    if (vmin is not None) and (vmax is not None):
        possibly_divergent = False

    # Setting vmin or vmax implies linspaced levels
    user_minmax = (vmin is not None) or (vmax is not None)

    # vlim might be computed below
    vlim = None

    if vmin is None:
        vmin = data_min
    elif possibly_divergent:
        vlim = abs(vmin - center)

    if vmax is None:
        vmax = data_max
    elif possibly_divergent:
        vlim = abs(vmax - center)

    if possibly_divergent:
        # kwargs not specific about divergent or not: infer defaults from data
        divergent = ((vmin < 0) and (vmax > 0)) or not center_is_none
    else:
        divergent = False

    # A divergent map should be symmetric around the center value
    if divergent:
        if vlim is None:
            vlim = max(abs(vmin - center), abs(vmax - center))
        vmin, vmax = -vlim, vlim

    # Now add in the centering value and set the limits
    vmin += center
    vmax += center

    # Choose default colormaps if not provided
    if cmap is None:
        if divergent:
            cmap = "RdBu_r"
        else:
            cmap = "viridis"

    # Handle discrete levels
    if levels is not None:
        if _is_scalar(levels):
            if user_minmax or levels == 1:
                levels = np.linspace(vmin, vmax, levels)
            else:
                # N in MaxNLocator refers to bins, not ticks
                ticker = mpl.ticker.MaxNLocator(levels - 1)
                levels = ticker.tick_values(vmin, vmax)
        vmin, vmax = levels[0], levels[-1]

    if extend is None:
        extend = _determine_extend(data_min, data_max, vmin, vmax)

    if levels is not None:
        cmap, norm = _build_discrete_cmap(cmap, levels, extend, filled)

    return dict(vmin=vmin, vmax=vmax, cmap=cmap, extend=extend,
                levels=levels, norm=norm)


def _determine_extend(data_min, data_max, vmin, vmax):
    extend_min = data_min < vmin
    extend_max = data_max > vmax
    if extend_min and extend_max:
        extend = 'both'
    elif extend_min:
        extend = 'min'
    elif extend_max:
        extend = 'max'
    else:
        extend = 'neither'
    return extend


def _build_discrete_cmap(cmap, levels, extend, filled):
    """Build a discrete colormap and normalization of the data."""
    import matplotlib as mpl

    if not filled:
        # non-filled contour plots
        extend = 'max'

    if extend == 'both':
        ext_n = 2
    elif extend in ['min', 'max']:
        ext_n = 1
    else:
        ext_n = 0

    n_colors = len(levels) + ext_n - 1
    pal = _color_palette(cmap, n_colors)

    new_cmap, cnorm = mpl.colors.from_levels_and_colors(
        levels, pal, extend=extend)
    # copy the old cmap name, for easier testing
    new_cmap.name = getattr(cmap, 'name', cmap)

    return new_cmap, cnorm


def _color_palette(cmap, n_colors):
    import matplotlib.pyplot as plt
    from matplotlib.colors import ListedColormap
    import numpy as np

    colors_i = np.linspace(0, 1., n_colors)
    if isinstance(cmap, (list, tuple)):
        # we have a list of colors
        cmap = ListedColormap(cmap, N=n_colors)
        pal = cmap(colors_i)
    elif isinstance(cmap, str):
        # we have some sort of named palette
        try:
            # is this a matplotlib cmap?
            ensure_cmaps_loaded()
            cmap = plt.get_cmap(cmap)
        except ValueError:
            # or maybe we just got a single color as a string
            cmap = ListedColormap([cmap], N=n_colors)
        pal = cmap(colors_i)
    else:
        # cmap better be a LinearSegmentedColormap (e.g. viridis)
        pal = cmap(colors_i)

    return pal


def _is_scalar(value):
    """Whether to treat a value as a scalar.
    Any non-iterable, string, or 0-D array
    """
    from collections.abc import Iterable

    return (getattr(value, 'ndim', None) == 0
            or isinstance(value, (str, bytes))
            or not isinstance(value, (Iterable,)))
