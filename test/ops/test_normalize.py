"""
Test for the harmonization operation
"""

from unittest import TestCase

import xarray as xr
from jdcal import gcal2jd
import numpy as np
from datetime import datetime

from cate.ops.normalize import normalize, adjust_spatial_attrs, adjust_temporal_attrs
from cate.core.op import OP_REGISTRY
from cate.util.misc import object_to_qualified_name


def assertDatasetEqual(expected, actual):
    # this method is functionally equivalent to
    # `assert expected == actual`, but it
    # checks each aspect of equality separately for easier debugging
    assert expected.equals(actual), (expected, actual)


class TestNormalize(TestCase):
    def test_normalize_lon_lat_2d(self):
        """
        Test nominal execution
        """
        dims = ('time', 'y', 'x')
        attribs = {'valid_min': 0., 'valid_max': 1.}

        t_size = 2
        y_size = 3
        x_size = 4

        a_data = np.random.random_sample((t_size, y_size, x_size))
        b_data = np.random.random_sample((t_size, y_size, x_size))
        time_data = [1, 2]
        lat_data = [[30., 30., 30., 30.],
                    [20., 20., 20., 20.],
                    [10., 10., 10., 10.]]
        lon_data = [[-10., 0., 10., 20.],
                    [-10., 0., 10., 20.],
                    [-10., 0., 10., 20.]]
        dataset = xr.Dataset({'a': (dims, a_data, attribs),
                              'b': (dims, b_data, attribs)
                              },
                             {'time': (('time',), time_data),
                              'lat': (('y', 'x'), lat_data),
                              'lon': (('y', 'x'), lon_data)
                              },
                             {'geospatial_lon_min': -15.,
                              'geospatial_lon_max': 25.,
                              'geospatial_lat_min': 5.,
                              'geospatial_lat_max': 35.
                              }
                             )

        new_dims = ('time', 'lat', 'lon')
        expected = xr.Dataset({'a': (new_dims, a_data, attribs),
                               'b': (new_dims, b_data, attribs)},
                              {'time': (('time',), time_data),
                               'lat': (('lat',), [30., 20., 10.]),
                               'lon': (('lon',), [-10., 0., 10., 20.]),
                               },
                              {'geospatial_lon_min': -15.,
                               'geospatial_lon_max': 25.,
                               'geospatial_lat_min': 5.,
                               'geospatial_lat_max': 35.})

        actual = normalize(dataset)
        xr.testing.assert_equal(actual, expected)

    def test_normalize_lon_lat(self):
        """
        Test nominal execution
        """
        dataset = xr.Dataset({'first': (['latitude',
                                         'longitude'], [[1, 2, 3],
                                                        [2, 3, 4]])})
        expected = xr.Dataset({'first': (['lat', 'lon'], [[1, 2, 3],
                                                          [2, 3, 4]])})
        actual = normalize(dataset)
        assertDatasetEqual(actual, expected)

        dataset = xr.Dataset({'first': (['lat', 'long'], [[1, 2, 3],
                                                          [2, 3, 4]])})
        expected = xr.Dataset({'first': (['lat', 'lon'], [[1, 2, 3],
                                                          [2, 3, 4]])})
        actual = normalize(dataset)
        assertDatasetEqual(actual, expected)

        dataset = xr.Dataset({'first': (['latitude',
                                         'spacetime'], [[1, 2, 3],
                                                        [2, 3, 4]])})
        expected = xr.Dataset({'first': (['lat', 'spacetime'], [[1, 2, 3],
                                                                [2, 3, 4]])})
        actual = normalize(dataset)
        assertDatasetEqual(actual, expected)

        dataset = xr.Dataset({'first': (['zef', 'spacetime'], [[1, 2, 3],
                                                               [2, 3, 4]])})
        expected = xr.Dataset({'first': (['zef', 'spacetime'], [[1, 2, 3],
                                                                [2, 3, 4]])})
        actual = normalize(dataset)
        assertDatasetEqual(actual, expected)

    def test_normalize_julian_day(self):
        """
        Test Julian Day -> Datetime conversion
        """
        tuples = [gcal2jd(2000, x, 1) for x in range(1, 13)]

        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [x[0] + x[1] for x in tuples]})
        ds.time.attrs['long_name'] = 'time in julian days'

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)]})
        expected.time.attrs['long_name'] = 'time'

        actual = normalize(ds)

        assertDatasetEqual(actual, expected)

    def test_registered(self):
        """
        Test as a registered operation
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(normalize))
        dataset = xr.Dataset({'first': (['latitude',
                                         'longitude'], [[1, 2, 3],
                                                        [2, 3, 4]])})
        expected = xr.Dataset({'first': (['lat', 'lon'], [[1, 2, 3],
                                                          [2, 3, 4]])})
        actual = reg_op(ds=dataset)
        assertDatasetEqual(actual, expected)


class TestAdjustSpatial(TestCase):
    def test_nominal(self):
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)]})

        ds.lon.attrs['units'] = 'degrees_east'
        ds.lat.attrs['units'] = 'degrees_north'

        ds1 = adjust_spatial_attrs(ds)

        # Make sure original dataset is not altered
        with self.assertRaises(KeyError):
            ds.attrs['geospatial_lat_min']

        # Make sure expected values are in the new dataset
        self.assertEqual(ds1.attrs['geospatial_lat_min'], -90)
        self.assertEqual(ds1.attrs['geospatial_lat_max'], 90)
        self.assertEqual(ds1.attrs['geospatial_lat_units'], 'degrees_north')
        self.assertEqual(ds1.attrs['geospatial_lat_resolution'], 4)
        self.assertEqual(ds1.attrs['geospatial_lon_min'], -180)
        self.assertEqual(ds1.attrs['geospatial_lon_max'], 180)
        self.assertEqual(ds1.attrs['geospatial_lon_units'], 'degrees_east')
        self.assertEqual(ds1.attrs['geospatial_lon_resolution'], 4)
        self.assertEqual(ds1.attrs['geospatial_bounds'],
                         'POLYGON((-180.0 -90.0, -180.0 90.0, 180.0 90.0,'
                         ' 180.0 -90.0, -180.0 -90.0))')

        # Test existing attributes update
        lon_min, lat_min, lon_max, lat_max = -20, -40, 60, 40
        indexers = {'lon': slice(lon_min, lon_max),
                    'lat': slice(lat_min, lat_max)}
        ds2 = ds1.sel(**indexers)
        ds2 = adjust_spatial_attrs(ds2)

        self.assertEqual(ds2.attrs['geospatial_lat_min'], -42)
        self.assertEqual(ds2.attrs['geospatial_lat_max'], 42)
        self.assertEqual(ds2.attrs['geospatial_lat_units'], 'degrees_north')
        self.assertEqual(ds2.attrs['geospatial_lat_resolution'], 4)
        self.assertEqual(ds2.attrs['geospatial_lon_min'], -20)
        self.assertEqual(ds2.attrs['geospatial_lon_max'], 60)
        self.assertEqual(ds2.attrs['geospatial_lon_units'], 'degrees_east')
        self.assertEqual(ds2.attrs['geospatial_lon_resolution'], 4)
        self.assertEqual(ds2.attrs['geospatial_bounds'],
                         'POLYGON((-20.0 -42.0, -20.0 42.0, 60.0 42.0, 60.0'
                         ' -42.0, -20.0 -42.0))')

    def test_bnds(self):
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)]})

        ds.lon.attrs['units'] = 'degrees_east'
        ds.lat.attrs['units'] = 'degrees_north'

        lat_bnds = np.empty([len(ds.lat), 2])
        lon_bnds = np.empty([len(ds.lon), 2])
        ds['nv'] = [0, 1]

        lat_bnds[:, 0] = ds.lat.values - 2
        lat_bnds[:, 1] = ds.lat.values + 2
        lon_bnds[:, 0] = ds.lon.values - 2
        lon_bnds[:, 1] = ds.lon.values + 2

        ds['lat_bnds'] = (['lat', 'nv'], lat_bnds)
        ds['lon_bnds'] = (['lon', 'nv'], lon_bnds)

        ds.lat.attrs['bounds'] = 'lat_bnds'
        ds.lon.attrs['bounds'] = 'lon_bnds'

        ds1 = adjust_spatial_attrs(ds)

        # Make sure original dataset is not altered
        with self.assertRaises(KeyError):
            ds.attrs['geospatial_lat_min']

        # Make sure expected values are in the new dataset
        self.assertEqual(ds1.attrs['geospatial_lat_min'], -90)
        self.assertEqual(ds1.attrs['geospatial_lat_max'], 90)
        self.assertEqual(ds1.attrs['geospatial_lat_units'], 'degrees_north')
        self.assertEqual(ds1.attrs['geospatial_lat_resolution'], 4)
        self.assertEqual(ds1.attrs['geospatial_lon_min'], -180)
        self.assertEqual(ds1.attrs['geospatial_lon_max'], 180)
        self.assertEqual(ds1.attrs['geospatial_lon_units'], 'degrees_east')
        self.assertEqual(ds1.attrs['geospatial_lon_resolution'], 4)
        self.assertEqual(ds1.attrs['geospatial_bounds'],
                         'POLYGON((-180.0 -90.0, -180.0 90.0, 180.0 90.0,'
                         ' 180.0 -90.0, -180.0 -90.0))')

        # Test existing attributes update
        lon_min, lat_min, lon_max, lat_max = -20, -40, 60, 40
        indexers = {'lon': slice(lon_min, lon_max),
                    'lat': slice(lat_min, lat_max)}
        ds2 = ds1.sel(**indexers)
        ds2 = adjust_spatial_attrs(ds2)

        self.assertEqual(ds2.attrs['geospatial_lat_min'], -42)
        self.assertEqual(ds2.attrs['geospatial_lat_max'], 42)
        self.assertEqual(ds2.attrs['geospatial_lat_units'], 'degrees_north')
        self.assertEqual(ds2.attrs['geospatial_lat_resolution'], 4)
        self.assertEqual(ds2.attrs['geospatial_lon_min'], -20)
        self.assertEqual(ds2.attrs['geospatial_lon_max'], 60)
        self.assertEqual(ds2.attrs['geospatial_lon_units'], 'degrees_east')
        self.assertEqual(ds2.attrs['geospatial_lon_resolution'], 4)
        self.assertEqual(ds2.attrs['geospatial_bounds'],
                         'POLYGON((-20.0 -42.0, -20.0 42.0, 60.0 42.0, 60.0'
                         ' -42.0, -20.0 -42.0))')


class TestAdjustTemporal(TestCase):
    def test_nominal(self):
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)]})

        ds1 = adjust_temporal_attrs(ds)

        # Make sure original dataset is not altered
        with self.assertRaises(KeyError):
            ds.attrs['time_coverage_start']

        # Make sure expected values are in the new dataset
        self.assertEqual(ds1.attrs['time_coverage_start'],
                         '2000-01-01T00:00:00.000000000')
        self.assertEqual(ds1.attrs['time_coverage_end'],
                         '2000-12-01T00:00:00.000000000')
        self.assertEqual(ds1.attrs['time_coverage_resolution'],
                         'P1M')
        self.assertEqual(ds1.attrs['time_coverage_duration'],
                         'P335D')

        # Test existing attributes update
        indexers = {'time': slice(datetime(2000, 2, 15), datetime(2000, 6, 15))}
        ds2 = ds1.sel(**indexers)
        ds2 = adjust_temporal_attrs(ds2)

        self.assertEqual(ds2.attrs['time_coverage_start'],
                         '2000-03-01T00:00:00.000000000')
        self.assertEqual(ds2.attrs['time_coverage_end'],
                         '2000-06-01T00:00:00.000000000')
        self.assertEqual(ds2.attrs['time_coverage_resolution'],
                         'P1M')
        self.assertEqual(ds2.attrs['time_coverage_duration'],
                         'P92D')
