"""
Tests for subsetting operations
"""
from datetime import datetime
from unittest import TestCase

import numpy as np
import xarray as xr
import pandas as pd

from cate.core.op import OP_REGISTRY
from cate.core.opimpl import subset_spatial_impl
from cate.core.types import ValidationError
from cate.ops import subset
from cate.util.misc import object_to_qualified_name


def assert_dataset_equal(expected, actual):
    # this method is functionally equivalent to
    # `assert expected == actual`, but it checks each aspect
    # of equality separately for easier debugging
    assert expected.equals(actual), (expected, actual)


def get_test_subset_non_valid_lat_lon_dataset():
    temp = np.random.randn(2, 2, 3)
    precip = np.random.rand(2, 2, 3)
    lon = [[-40, 40], [-40, 40]]
    lat = [[-50, 50], [-50, 50]]
    return xr.Dataset({'temp': (['x', 'y', 'time'], temp),
                       'precip': (['x', 'y', 'time'], precip)},
                      coords={'lon': (['x', 'y'], lon),
                              'lat': (['x', 'y'], lat),
                              'time': pd.date_range('2014-09-06', periods=3)})


class TestSubsetSpatial(TestCase):
    def test_subset_non_valid_lat_lon(self):
        """
        Test whether lat and/or lon exist and if they exist whether they have dimension = 1
        :return: void
        """

        # Test whether lat lon exist
        dataset = xr.Dataset({
            'first': (['xc', 'yc', 'time'], np.ones([180, 360, 6])),
            'second': (['xc', 'yc', 'time'], np.ones([180, 360, 6])),
            'xc': np.linspace(-89.5, 89.5, 180),
            'yc': np.linspace(-179.5, 179.5, 360),
        })

        with self.assertRaises(ValidationError) as error:
            subset_spatial_impl(dataset, (-40, 40, -50, 50))
        self.assertIn('No (valid) geocoding found', str(error.exception))

        # test whether lat lon has the wrong dimension (!=1)
        dataset = get_test_subset_non_valid_lat_lon_dataset()

        with self.assertRaises(ValidationError) as error:
            subset_spatial_impl(dataset, (-40, 40, -50, 50))
        self.assertIn('Geocoding not recognised', str(error.exception))

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
            'first': (['lat', 'lon', 'time'], np.ones([22, 42, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([22, 42, 6])),
            'lat': np.linspace(-10.5, 10.5, 22),
            'lon': np.linspace(-20.5, 20.5, 42)})
        assert_dataset_equal(expected, actual)

    def test_inverted_dims_nominal(self):
        """
        Test if the implementation is dimension order agnostic.
        """
        # Inverted lat
        dataset = xr.Dataset({
            'first': (['lon', 'lat', 'time'], np.ones([360, 180, 6])),
            'second': (['lon', 'lat', 'time'], np.ones([360, 180, 6])),
            'lat': np.linspace(89.5, -89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360)})
        actual = subset.subset_spatial(dataset, "-20, -10, 20, 10")
        expected = xr.Dataset({
            'first': (['lon', 'lat', 'time'], np.ones([42, 22, 6])),
            'second': (['lon', 'lat', 'time'], np.ones([42, 22, 6])),
            'lat': np.linspace(10.5, -10.5, 22),
            'lon': np.linspace(-20.5, 20.5, 42)})
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

    def test_generic_masked_inverted(self):
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

        # Inverted lat
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'lat': np.linspace(89.5, -89.5, 180),
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

    def test_generic_not_masked_inverted(self):
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

        # Inverted lat
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'lat': np.linspace(89.5, -89.5, 180),
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
            'first': (['lat', 'lon', 'time'], np.ones([22, 42, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([22, 42, 6])),
            'lat': np.linspace(-10.5, 10.5, 22),
            'lon': np.linspace(-20.5, 20.5, 42)})
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

    def test_antimeridian_simple_inverted(self):
        # Inverted lat
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'lat': np.linspace(89.5, -89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360)})

        # With masking
        actual = subset.subset_spatial(dataset, '170, -5, -170, 5', mask=True)
        masked = actual.sel(method='nearest', **{'lon': 0, 'lat': 0})
        self.assertTrue(np.isnan(masked['first']).all())

    def test_antimeridian_arbitrary(self):
        antimeridian_pol = str('POLYGON(('
                               '162.0703125 39.639537564366705,'
                               '-155.390625 39.774769485295465,'
                               '-155.56640625 12.726084296948184,'
                               '162.24609375 12.897489183755905,'
                               '161.89453125 26.745610382199025,'
                               '162.0703125 39.639537564366705'
                               '))')
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360)})

        with self.assertRaises(Exception) as cm:
            subset.subset_spatial(dataset, antimeridian_pol)
        self.assertIn('anti-meridian', str(cm.exception))

    def test_antimeridian_arbitrary_inverted(self):
        antimeridian_pol = str('POLYGON(('
                               '162.0703125 39.639537564366705,'
                               '-155.390625 39.774769485295465,'
                               '-155.56640625 12.726084296948184,'
                               '162.24609375 12.897489183755905,'
                               '161.89453125 26.745610382199025,'
                               '162.0703125 39.639537564366705'
                               '))')
        # Inverted lat
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'lat': np.linspace(89.5, -89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360)})

        with self.assertRaises(Exception) as cm:
            subset.subset_spatial(dataset, antimeridian_pol)
        self.assertIn('anti-meridian', str(cm.exception))

    def test_select_single_center(self):
        """
        Test subset spatial with a polygon that completely fits
        inside pixel bounds
        """
        # Masked
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([6, 12, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([6, 12, 3])),
            'lat': np.linspace(-75, 75, 6),
            'lon': np.linspace(-165, 165, 12),
            'time': [datetime(2000, x, 1) for x in range(1, 4)]})
        poly = ((32, 2), (34, 28), (58, 29), (54, 4))
        actual = subset.subset_spatial(dataset, region=poly)
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([1, 1, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([1, 1, 3])),
            'lat': [15.],
            'lon': [45.],
            'time': [datetime(2000, x, 1) for x in range(1, 4)]})

        assert_dataset_equal(expected, actual)

        # Not masked
        actual = subset.subset_spatial(dataset, region=poly, mask=False)
        assert_dataset_equal(expected, actual)

    def test_select_single_vertice(self):
        """
        Test with a polygon that encloses a single pixel vertice,
        but no centers. That is, crosses four pixels
        """
        # Masked
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([6, 12, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([6, 12, 3])),
            'lat': np.linspace(-75, 75, 6),
            'lon': np.linspace(-165, 165, 12),
            'time': [datetime(2000, x, 1) for x in range(1, 4)]})
        poly = ((25, 2), (27, 40), (58, 38), (54, 4))
        actual = subset.subset_spatial(dataset, region=poly)
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([2, 2, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([2, 2, 3])),
            'lat': [15., 45.],
            'lon': [15., 45.],
            'time': [datetime(2000, x, 1) for x in range(1, 4)]})

        assert_dataset_equal(expected, actual)

        # Not masked
        actual = subset.subset_spatial(dataset, region=poly, mask=False)
        assert_dataset_equal(expected, actual)

    def test_select_1d_lat(self):
        """
        Test with a polyon that runs over pixel boundaries in the
        latitude direction, resulting in selecting a single column
        """
        # Masked
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([6, 12, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([6, 12, 3])),
            'lat': np.linspace(-75, 75, 6),
            'lon': np.linspace(-165, 165, 12),
            'time': [datetime(2000, x, 1) for x in range(1, 4)]})
        poly = ((25, 2), (27, 47), (12, 50), (10, 4))
        actual = subset.subset_spatial(dataset, region=poly)
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([2, 1, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([2, 1, 3])),
            'lat': [15., 45.],
            'lon': [15.],
            'time': [datetime(2000, x, 1) for x in range(1, 4)]})

        assert_dataset_equal(expected, actual)

        # Not masked
        actual = subset.subset_spatial(dataset, region=poly, mask=False)
        assert_dataset_equal(expected, actual)

    def test_select_1d_lon(self):
        """
        Test with a polygon that runs over pixel boundaries in the
        longitude direction. E.g., selects a single row
        """
        # Masked
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([6, 12, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([6, 12, 3])),
            'lat': np.linspace(-75, 75, 6),
            'lon': np.linspace(-165, 165, 12),
            'time': [datetime(2000, x, 1) for x in range(1, 4)]})
        poly = ((-28, 32), (-26, 58), (28, 52), (25, 33))
        actual = subset.subset_spatial(dataset, region=poly)
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([1, 2, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([1, 2, 3])),
            'lat': [45.],
            'lon': [-15., 15.],
            'time': [datetime(2000, x, 1) for x in range(1, 4)]})

        assert_dataset_equal(expected, actual)

        # Not masked
        actual = subset.subset_spatial(dataset, region=poly, mask=False)
        assert_dataset_equal(expected, actual)

    def test_select_from_single_pixel(self):
        """
        Test subset spatial where the input dataset has only one pixel
        """
        # Masked
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([1, 1, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([1, 1, 3])),
            'lat': [45.],
            'lon': [15.],
            'time': [datetime(2000, x, 1) for x in range(1, 4)]})
        poly = ((-28, 32), (-26, 58), (28, 52), (25, 33))
        actual = subset.subset_spatial(dataset, region=poly)

        assert_dataset_equal(dataset, actual)

        # Not masked
        actual = subset.subset_spatial(dataset, region=poly, mask=False)
        assert_dataset_equal(dataset, actual)

    def test_out_of_bounds(self):
        """
        Test that an appropriate error is raised when the polygon
        wouldn't select any data from the original dataset
        """
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([1, 1, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([1, 1, 3])),
            'lat': [45.],
            'lon': [15.],
            'time': [datetime(2000, x, 1) for x in range(1, 4)]})
        poly = ((32, 32), (34, 58), (56, 56), (58, 33))

        with self.assertRaises(ValueError) as err:
            subset.subset_spatial(dataset, region=poly)
        self.assertIn('Can not select', str(err.exception))

        # Not masked
        with self.assertRaises(ValueError) as err:
            subset.subset_spatial(dataset, region=poly, mask=False)
        self.assertIn('Can not select', str(err.exception))

    def test_non_geospatial_variable(self):
        """
        Test that subsetting a dataset that contains a non-geospatial
        dataset skips such a dataset
        """
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([180, 360, 6])),
            'third': (['time'], np.ones([6])),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360),
            'time': np.array([0, 1, 2, 3, 4, 5])})
        actual = subset.subset_spatial(dataset, "-20, -10, 20, 10")
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([22, 42, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([22, 42, 6])),
            'third': (['time'], np.ones([6])),
            'lat': np.linspace(-10.5, 10.5, 22),
            'lon': np.linspace(-20.5, 20.5, 42),
            'time': np.array([0, 1, 2, 3, 4, 5])})
        assert_dataset_equal(expected, actual)

        poly = "POLYGON ((6.609745636041873 45.391851854914776, 19.720614826531428 45.391851854914776, 19.720614826531428 37.06301149556095, 6.609745636041874 37.06301149556095, 6.609745636041873 45.391851854914776))"
        actual = subset.subset_spatial(dataset, poly, mask=True)
        xr.testing.assert_equal(expected.third, actual.third)


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


class TestExtractPoint(TestCase):
    @classmethod
    def setUpClass(cls):
        v1_data = np.arange(18).reshape((3, 3, 2))
        v2_data = np.arange(100, 118).reshape((3, 3, 2))
        v3_data = np.arange(36).reshape((3, 3, 2, 2))
        v4_data = np.arange(50, 59).reshape((3, 3))
        cls._ds = xr.Dataset(
            {
                'v1': (['lat', 'lon', 'd1'], v1_data),
                'v2': (['lat', 'lon', 'd2'], v2_data),
                'v3': (['lat', 'lon', 'd1', 'd2'], v3_data),
                'v4': (['lat', 'lon'], v4_data)
            },
            coords={
                'lon': np.array([12, 13, 14], np.dtype('float64')),
                'lat': np.array([22, 23, 24], np.dtype('float64')),
                'd1': [1, 2],
                'd2': [datetime(2000, 3, 1), datetime(2000, 4, 1)]
            }
        )

    def test_all_extra_dims(self):
        result = subset.extract_point(self._ds, (12.2, 23.2), indexers={'d1': 2, 'd2': '2000-03-01'})
        self.assertEqual({'lat': 23.0, 'lon': 12.0, 'v1': 7.0, 'v2': 106.0, 'v3': 14.0, 'v4': 53.0}, result)

    def test_one_extra_dims(self):
        result = subset.extract_point(self._ds, (12.2, 23.2), indexers={'d1': 2})
        self.assertEqual({'lat': 23.0, 'lon': 12.0, 'v1': 7.0, 'v4': 53.0}, result)

        result = subset.extract_point(self._ds, (12.2, 23.2), indexers={'d2': '2000-03-01'})
        self.assertEqual({'lat': 23.0, 'lon': 12.0, 'v2': 106.0, 'v4': 53.0}, result)

    def test_no_extra_dims(self):
        result = subset.extract_point(self._ds, (12.2, 23.2), indexers={})
        self.assertEqual({'lat': 23.0, 'lon': 12.0, 'v4': 53.0}, result)

        result = subset.extract_point(self._ds, (12.2, 23.2))
        self.assertEqual({'lat': 23.0, 'lon': 12.0, 'v4': 53.0}, result)

    def test_unknown_dim(self):
        result = subset.extract_point(self._ds, (12.2, 23.2), indexers={'x1': 42})
        self.assertEqual({'lat': 23.0, 'lon': 12.0, 'v4': 53.0}, result)

    def test_point_out_of_bounds(self):
        result = subset.extract_point(self._ds, (0, 0), indexers={'d1': 2, 'd2': '2000-03-01'})
        self.assertEqual({}, result)

        result = subset.extract_point(self._ds, (0, 0))
        self.assertEqual({}, result)

    def test_extra_dim_with_no_exact_match(self):
        # no exact match for 'd2', the same as if 'd2' is not given
        result = subset.extract_point(self._ds, (12.2, 23.2), indexers={'d1': 2, 'd2': '2000-03-02'})
        self.assertEqual({'lat': 23.0, 'lon': 12.0, 'v1': 7.0, 'v4': 53.0}, result)

        result = subset.extract_point(self._ds, (12.2, 23.2), indexers={'d1': 1.1, 'd2': '2000-03-01'})
        self.assertEqual({'lat': 23.0, 'lon': 12.0, 'v2': 106.0, 'v4': 53.0}, result)
