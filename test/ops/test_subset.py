"""
Tests for subsetting operations
"""

from unittest import TestCase

import numpy as np
import xarray as xr

from cate.ops import subset


class TestSubset(TestCase):
    def test_subset_spatial(self):
        # Test general functionality
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360)})
        actual = subset.subset_spatial(dataset, -10, 10, -20, 20)
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([20, 40, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([20, 40, 6])),
            'lat': np.linspace(-9.5, 9.5, 20),
            'lon': np.linspace(-19.5, 19.5, 40)})
        self.assertDatasetEqual(expected, actual)

    def test_subset_temporal(self):
        # Test general functionality
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360),
            'time': ['2000-01-01',
                     '2000-02-01',
                     '2000-03-01',
                     '2000-04-01',
                     '2000-05-01',
                     '2000-06-01']})
        actual = subset.subset_temporal(dataset, '2000-01-10', '2000-04-01')
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 3])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360),
            'time': ['2000-02-01', '2000-03-01', '2000-04-01']})
        self.assertDatasetEqual(expected, actual)

    def test_subset_temporal_mjd(self):
        # Test subsetting for MJD timed datsets
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360),
            'time': [2451544.5,
                     2451575.5,
                     2451604.5,
                     2451635.5,
                     2451665.5,
                     2451696.5]})
        actual = subset.subset_temporal(dataset, '2000-01-10', '2000-04-01')
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 3])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360),
            'time': [2451575.5, 2451604.5, 2451635.5]})
        self.assertDatasetEqual(expected, actual)

    def test_subset_temporal_index(self):
        # Test general functionality
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360),
            'time': ['2000-01-01',
                     '2000-02-01',
                     '2000-03-01',
                     '2000-04-01',
                     '2000-05-01',
                     '2000-06-01']})
        actual = subset.subset_temporal_index(dataset, 2, 4)
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 3])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360),
            'time': ['2000-03-01', '2000-04-01', '2000-05-01']})
        self.assertDatasetEqual(expected, actual)

    def assertDatasetEqual(self, expected, actual):
        # this method is functionally equivalent to
        # `assert expected == actual`, but it checks each aspect
        # of equality separately for easier debugging
        assert expected.equals(actual), (expected, actual)
