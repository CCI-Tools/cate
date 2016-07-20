from unittest import TestCase

import numpy as np
import pandas as pd
import xarray as xr

from ect.ops.timeseries import timeseries


class TimeSeriesTest(TestCase):
    def test_nearest(self):
        temp = 15 + 8 * np.arange(4 * 2 * 3).reshape((4, 2, 3))
        lat = [45, -45]
        lon = [-135, -45, 45, 135]

        vars_dict = {'temperature': (['lon', 'lat', 'time'], temp)}
        coords_dict = {'lon': lon, 'lat': lat, 'time': pd.date_range('2014-09-06', periods=3)}

        ds = xr.Dataset(data_vars=vars_dict, coords=coords_dict)
        ts = timeseries(ds, lat=33, lon=22)

        self.assertIsNotNone(ts)
        self.assertIsInstance(ts, xr.Dataset)
        np.testing.assert_array_equal([111, 119, 127], ts.temperature.values)
