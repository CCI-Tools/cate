"""
Tests for subsetting operations
"""

from unittest import TestCase

import numpy as np
import xarray as xr

from datetime import datetime

from cate.ops import subset
from cate.core.op import OP_REGISTRY
from cate.util.misc import object_to_qualified_name


def assert_dataset_equal(expected, actual):
    # this method is functionally equivalent to
    # `assert expected == actual`, but it checks each aspect
    # of equality separately for easier debugging
    assert expected.equals(actual), (expected, actual)


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
        assert_dataset_equal(expected, actual)

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
        assert_dataset_equal(expected, actual)

    def test_generic_masked(self):
        """
        Test using a generic Polygon and masking
        """
        # Africa
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
        # Gulf of Guinea
        gog = actual.sel(method='nearest', **{'lon': 1.2, 'lat': -1.4})
        self.assertTrue(np.isnan(gog['first']).all())
        # Africa
        self.assertTrue(1 == actual.sel(method='nearest', **{'lon': 20.7, 'lat': 6.15}))

    def test_generic_not_masked(self):
        """
        Test using a generic Polygon without masking
        """
        # Africa
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
        # Gulf of Guinea
        self.assertTrue(1 == actual.sel(method='nearest', **{'lon': 1.2, 'lat': -1.4}))
        # Africa
        self.assertTrue(1 == actual.sel(method='nearest', **{'lon': 20.7, 'lat': 6.15}))

    def test_registered(self):
        """
        Test if it runs as an operation registered in the op registry.
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(subset.subset_spatial))
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360)})
        actual = reg_op(ds=dataset, region="-20, -10, 20, 10")
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([20, 40, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([20, 40, 6])),
            'lat': np.linspace(-9.5, 9.5, 20),
            'lon': np.linspace(-19.5, 19.5, 40)})
        assert_dataset_equal(expected, actual)

    def test_antimeridian_simple(self):
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360)})

        # With masking
        actual = subset.subset_spatial(dataset, '170, -5, -170, 5', mask=True)
        masked = actual.sel(method='nearest', **{'lon': 0, 'lat': 0})
        self.assertTrue(np.isnan(masked['first']).all())

        # With dropping
        actual = subset.subset_spatial(dataset, '170, -5, -170, 5', mask=False)
        self.assertEqual(20, len(actual.lon))

    def test_antimeridian_arbitrary(self):
        pol = str('POLYGON((162.0703125 39.639537564366705,-155.390625'
                  '39.774769485295465,-155.56640625 12.726084296948184,162.24609375'
                  '12.897489183755905,161.89453125 26.745610382199025,162.0703125'
                  '39.639537564366705))')
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360)})

        with self.assertRaises(Exception) as err:
            subset.subset_spatial(dataset, pol)
        self.assertEqual('cannot convert geometry to a valid Polygon: ' + pol, str(err.exception))


class TestSubsetTemporal(TestCase):
    def test_subset_temporal(self):
        # Test general functionality
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360),
            'time': [datetime(2000, x, 1) for x in range(1, 7)]})
        actual = subset.subset_temporal(dataset, '2000-01-10, 2000-04-01')
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 3])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360),
            'time': [datetime(2000, x, 1) for x in range(2, 5)]})
        assert_dataset_equal(expected, actual)

    def test_invalid_dtype(self):
        # Test passing in a MJD dataset
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
        with self.assertRaises(ValueError) as err:
            subset.subset_temporal(dataset, '2000-01-10, 2000-04-01')
        self.assertIn('type datetime', str(err.exception))

    def test_registered(self):
        """
        Test if it runs as an operation registered in the op registry.
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(subset.subset_temporal))
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360),
            'time': [datetime(2000, x, 1) for x in range(1, 7)]})
        actual = reg_op(ds=dataset, time_range='2000-01-10, 2000-04-01')
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 3])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360),
            'time': [datetime(2000, x, 1) for x in range(2, 5)]})
        assert_dataset_equal(expected, actual)



class TestSubsetTemporalIndex(TestCase):
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
        assert_dataset_equal(expected, actual)

    def test_registered(self):
        """
        Test if it runs as an operation registered in the op registry.
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(subset.subset_temporal_index))
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
        actual = reg_op(ds=dataset, time_ind_min=2, time_ind_max=4)
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 3])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360),
            'time': ['2000-03-01', '2000-04-01', '2000-05-01']})
        assert_dataset_equal(expected, actual)
