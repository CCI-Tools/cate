"""
Test for the harmonization operation
"""

from unittest import TestCase

import xarray as xr
from jdcal import gcal2jd
import numpy as np
from datetime import datetime

from cate.ops.normalize import normalize
from cate.core.op import OP_REGISTRY
from cate.util.misc import object_to_qualified_name


def assertDatasetEqual(expected, actual):
    # this method is functionally equivalent to
    # `assert expected == actual`, but it
    # checks each aspect of equality separately for easier debugging
    assert expected.equals(actual), (expected, actual)


class TestHarmonize(TestCase):
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
                               'lat_2d': (('lat', 'lon'), lat_data),
                               'lon_2d': (('lat', 'lon'), lon_data),
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
