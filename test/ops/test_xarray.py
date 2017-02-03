"""
Test the IO operations
"""

import unittest

import numpy as np
import pandas as pd
import xarray as xr

from cate.ops.xarray import sel as sel_op


def new_ds():
    lon = [10.1, 10.2, 10.3, 10.4]
    lat = [34.5, 34.6]
    time = pd.date_range('2014-09-06', periods=10)
    reference_time = pd.Timestamp('2014-09-05')

    time_res = len(time)
    lon_res = len(lon)
    lat_res = len(lat)

    temperature = (15 + 8 * np.random.randn(lon_res, lat_res, time_res)).round(decimals=1)
    precipitation = (10 * np.random.rand(lon_res, lat_res, time_res)).round(decimals=1)

    ds = xr.Dataset({'temperature': (['lon', 'lat', 'time'], temperature),
                     'precipitation': (['lon', 'lat', 'time'], precipitation)
                     },
                    coords={'lon': lon,
                            'lat': lat,
                            'time': time,
                            'reference_time': reference_time
                            })
    return ds


class TestIO(unittest.TestCase):
    def test_sel_op(self):
        ds = new_ds()

        # ds.to_netcdf('precip_and_temp.nc')

        sel_ds = sel_op(ds=ds, time='2014-09-06')
        self.assertEqual(set(sel_ds.coords.keys()), {'lon', 'lat', 'time', 'reference_time'})
        self.assertEqual(sel_ds.dims['lon'], 4)
        self.assertEqual(sel_ds.dims['lat'], 2)
        self.assertNotIn('time', sel_ds.dims)

        sel_ds = sel_op(ds=ds, lat=10.25, lon=34.51)
        self.assertEqual(set(sel_ds.coords.keys()), {'lon', 'lat', 'time', 'reference_time'})
        self.assertNotIn('lon', sel_ds.dims)
        self.assertNotIn('lat', sel_ds.dims)
        self.assertEqual(sel_ds.dims['time'], 10)
