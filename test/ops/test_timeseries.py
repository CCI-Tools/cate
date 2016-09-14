"""
Tests for timeseries operations
"""

from unittest import TestCase

import numpy as np
import pandas as pd
import xarray as xr

from ect.ops.timeseries import timeseries, timeseries_mean


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

    def test_timeseries_mean(self):
        # Test general functionality
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180,360,6])),
            'second': (['lat', 'lon', 'time'], np.ones([180,360,6])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360),
            'time': ['2000-01-01','2000-02-01','2000-03-01','2000-04-01','2000-05-01','2000-06-01']})
        actual = timeseries_mean(dataset)
        expected = xr.Dataset({
            'first': ('time', np.ones([6])),
            'second': ('time', np.ones([6])),
            'time': ['2000-01-01','2000-02-01','2000-03-01','2000-04-01','2000-05-01','2000-06-01']})
        self.assertDatasetEqual(expected, actual)

    def test_incompatible_ds(self):
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'slime'], np.ones([180,360,6])),
            'second': (['lat', 'lon', 'slime'], np.ones([180,360,6])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360),
            'slime': ['2000-01-01','2000-02-01','2000-03-01','2000-04-01','2000-05-01','2000-06-01']})

        with self.assertRaises(ValueError):
            ts = timeseries(dataset, lat=33, lon=22)

        with self.assertRaises(ValueError):
            ts = timeseries_mean(dataset)

    def assertDatasetEqual(self, expected, actual):
        # this method is functionally equivalent to `assert expected == actual`, but it
        # checks each aspect of equality separately for easier debugging
        assert expected.equals(actual), (expected, actual)
