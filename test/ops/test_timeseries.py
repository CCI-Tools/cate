"""
Tests for timeseries operations
"""

from unittest import TestCase

import numpy as np
import xarray as xr

from ect.ops.timeseries import tseries_point, tseries_mean


class TimeSeriesTest(TestCase):
    def test_tseries_point(self):
        # Test general functionality
        dataset = xr.Dataset({
            'abs': (['lat', 'lon', 'time'], np.ones([4, 8, 6])),
            'bbs': (['lat', 'lon', 'time'], np.ones([4, 8, 6])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': ['2000-01-01', '2000-02-01', '2000-03-01', '2000-04-01',
                     '2000-05-01', '2000-06-01']})

        actual = tseries_point(dataset, lat=5, lon=10, var='*bs')
        expected = xr.Dataset({
            'abs': (['time'], np.ones(6)),
            'bbs': (['time'], np.ones(6)),
            'lat': 22.5,
            'lon': 22.5,
            'time': ['2000-01-01', '2000-02-01', '2000-03-01', '2000-04-01',
                     '2000-05-01', '2000-06-01']})
        expected = expected.set_coords(['lat', 'lon'])
        self.assertDatasetEqual(expected, actual)

        actual = tseries_point(dataset, lat=5, lon=10, var='')
        self.assertDatasetEqual(expected, actual)

    def test_tseries_mean(self):
        # Test general functionality
        dataset = xr.Dataset({
            'abs': (['lat', 'lon', 'time'], np.ones([4, 8, 6])),
            'bbs': (['lat', 'lon', 'time'], np.ones([4, 8, 6])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': ['2000-01-01', '2000-02-01', '2000-03-01', '2000-04-01',
                     '2000-05-01', '2000-06-01']})
        actual = tseries_mean(dataset, var='*bs')
        expected = xr.Dataset({
            'abs': (['lat', 'lon', 'time'], np.ones([4, 8, 6])),
            'bbs': (['lat', 'lon', 'time'], np.ones([4, 8, 6])),
            'abs_ts_mean': (['time'], np.ones([6])),
            'bbs_ts_mean': (['time'], np.ones([6])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': ['2000-01-01', '2000-02-01', '2000-03-01', '2000-04-01',
                     '2000-05-01', '2000-06-01']})
        self.assertDatasetEqual(expected, actual)

        # Test damage control
        actual = tseries_mean(dataset, var='')
        self.assertDatasetEqual(actual, dataset)

    def assertDatasetEqual(self, expected, actual):
        # this method is functionally equivalent to
        # `assert expected == actual`, but it checks each aspect of
        # equality separately for easier debugging
        assert expected.equals(actual), (expected, actual)
