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
from ect.ops.plot import plot_map

plot_map(dset)
plt.show()
```

General jupyter notebook usage:
```python
import matplotlib.pyplot as plt
from ect.ops.plot import plot_map

%matplotlib inline
plot_map(dset)
```
If a file path is given, the plot is saved.
Supported formats: eps, jpeg, jpg, pdf, pgf, png, ps, raw, rgba, svg, svgz, tif, tiff

"""
import matplotlib
matplotlib.use('agg')
# https://github.com/matplotlib/matplotlib/issues/3466/#issuecomment-213678376
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import xarray as xr

from ect.core.op import op_input, op_output


@op_input('ds', description="A dataset from which to create the plot", required=True)
@op_input('variable', description="The geophysical quantity (dataset variable) to plot")
@op_input('time', description="Point in time to plot")
@op_input('extents', description="[lat,lat,lon,lon] extents to plot")
@op_input('path', description="Full path where to save the plot")
def plot_map(ds:xr.Dataset, variable:str=None, time=None, extents:list=None, path:str=None):
    if not isinstance(ds, xr.Dataset):
        raise NotImplementedError('Only raster datasets are currently supported')

    if not variable:
        for key in ds.data_vars.keys():
            variable = key
            break

    if not time:
        time = 0

    array_slice = ds[variable].isel(time=time)
    fig = plt.figure(figsize=(16,8))
    ax = plt.axes(projection=ccrs.PlateCarree())
    if extents:
        ax.set_extent(extents, ccrs.PlateCarree())
    else:
        ax.set_global()

    ax.coastlines()
    array_slice.plot.contourf(ax=ax, transform=ccrs.PlateCarree())

    if path:
        fig.savefig(path)
