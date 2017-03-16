"""
Tests for subsetting operations
"""

from unittest import TestCase

import numpy as np
import xarray as xr

from cate.ops import subset


class TestSubsetSpatial(TestCase):
    def test_nominal(self):
        """
        Test general 'most expected' use case functionality.
        """
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360)})
        actual = subset.subset_spatial(dataset, "-20, -10, 20, 10")
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([20, 40, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([20, 40, 6])),
            'lat': np.linspace(-9.5, 9.5, 20),
            'lon': np.linspace(-19.5, 19.5, 40)})
        self.assertDatasetEqual(expected, actual)

    def test_inverted_dims(self):
        """
        Test if the implementation is dimension order agnostic.
        """
        dataset = xr.Dataset({
            'first': (['lon', 'lat', 'time'], np.ones([360, 180, 6])),
            'second': (['lon', 'lat', 'time'], np.ones([360, 180, 6])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360)})
        actual = subset.subset_spatial(dataset, "-20, -10, 20, 10")
        expected = xr.Dataset({
            'first': (['lon', 'lat', 'time'], np.ones([40, 20, 6])),
            'second': (['lon', 'lat', 'time'], np.ones([40, 20, 6])),
            'lat': np.linspace(-9.5, 9.5, 20),
            'lon': np.linspace(-19.5, 19.5, 40)})
        self.assertDatasetEqual(expected, actual)

    def test_generic_masked(self):
        """
        Test using a generic Polygon and masking
        """
        a = str('POLYGON((-10.8984375 35.60371874069731,-19.16015625 '
        '23.885837699861995,-20.56640625 17.14079039331665,-18.6328125 '
        '7.536764322084079,-10.72265625 0.7031073524364783,10.37109375 '
        '0.3515602939922709,10.37109375 -22.268764039073965,22.8515625 '
        '-42.29356419217007,37.79296875 -27.21555620902968,49.39453125 '
        '-3.5134210456400323,54.4921875 14.093957177836236,18.984375 '
        '35.88905007936091,-10.8984375 35.60371874069731))')

        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360)})
        actual = subset.subset_spatial(dataset, a)

    def test_generic_not_masked(self):
        """
        Test using a generic Polygon without masking
        """
        a = str('POLYGON((-10.8984375 35.60371874069731,-19.16015625 '
        '23.885837699861995,-20.56640625 17.14079039331665,-18.6328125 '
        '7.536764322084079,-10.72265625 0.7031073524364783,10.37109375 '
        '0.3515602939922709,10.37109375 -22.268764039073965,22.8515625 '
        '-42.29356419217007,37.79296875 -27.21555620902968,49.39453125 '
        '-3.5134210456400323,54.4921875 14.093957177836236,18.984375 '
        '35.88905007936091,-10.8984375 35.60371874069731))')

        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360)})
        actual = subset.subset_spatial(dataset, a, mask=False)

    def test_registered(self):
        """
        Test if it runs as an operation registered in the op registry.
        """
        pass

    def assertDatasetEqual(self, expected, actual):
        # this method is functionally equivalent to
        # `assert expected == actual`, but it checks each aspect
        # of equality separately for easier debugging
        assert expected.equals(actual), (expected, actual)


class TestSubsetTemporal(TestCase):
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
