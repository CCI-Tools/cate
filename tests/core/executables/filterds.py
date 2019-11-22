import sys

import xarray as xr

args = sys.argv
if len(args) < 1 + 3:
    print("usage: filterds.py <ifile> <ofile> <var> ...", file=sys.stderr)
    exit(1)

ifile = args[1]
ofile = args[2]
vars = args[3:]

ds = xr.open_dataset(ifile)
ds = ds.drop(vars)
ds.to_netcdf(ofile)
