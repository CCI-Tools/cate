"""
Description
===========

CLI/API data visualization operations

Components
==========
"""

import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import xarray as xr

from ect.core.op import op_input, op_output


@op_input('ds', description="A dataset from which to create the plot")
@op_input('variable', description="The ECV variable to plot")
@op_input('time', description="Point in time to plot")
@op_input('extents', description="[lat,lat,lon,lon] extents to plot")
@op_input('path', description="Full path where to save the plot")
def plot_map(ds:xr.Dataset, variable:str=None, time=None, extents:list=None, path:str=None):
    if not variable:
        for key in ds.data_vars.keys():
            varible = key
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
