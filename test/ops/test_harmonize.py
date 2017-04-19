"""
Test for the harmonization operation
"""

from unittest import TestCase

import xarray as xr
from jdcal import gcal2jd
import numpy as np
from datetime import datetime

from cate.ops.harmonize import harmonize
from cate.core.op import OP_REGISTRY
from cate.util.misc import object_to_qualified_name


def assertDatasetEqual(expected, actual):
    # this method is functionally equivalent to
    # `assert expected == actual`, but it
    # checks each aspect of equality separately for easier debugging
    assert expected.equals(actual), (expected, actual)


class TestHarmonize(TestCase):
    def test_harmonize(self):
        """
        Test nominal execution
        """
        dataset = xr.Dataset({'first': (['latitude',
                                         'longitude'], [[1, 2, 3],
                                                        [2, 3, 4]])})
        expected = xr.Dataset({'first': (['lat', 'lon'], [[1, 2, 3],
                                                          [2, 3, 4]])})
        actual = harmonize(dataset)
        assertDatasetEqual(actual, expected)

        dataset = xr.Dataset({'first': (['lat', 'long'], [[1, 2, 3],
                                                          [2, 3, 4]])})
        expected = xr.Dataset({'first': (['lat', 'lon'], [[1, 2, 3],
                                                          [2, 3, 4]])})
        actual = harmonize(dataset)
        assertDatasetEqual(actual, expected)

        dataset = xr.Dataset({'first': (['latitude',
                                         'spacetime'], [[1, 2, 3],
                                                        [2, 3, 4]])})
        expected = xr.Dataset({'first': (['lat', 'spacetime'], [[1, 2, 3],
                                                                [2, 3, 4]])})
        actual = harmonize(dataset)
        assertDatasetEqual(actual, expected)

        dataset = xr.Dataset({'first': (['zef', 'spacetime'], [[1, 2, 3],
                                                               [2, 3, 4]])})
        expected = xr.Dataset({'first': (['zef', 'spacetime'], [[1, 2, 3],
                                                                [2, 3, 4]])})
        actual = harmonize(dataset)
        assertDatasetEqual(actual, expected)

    def test_julian_day(self):
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

        actual = harmonize(ds)

        assertDatasetEqual(actual, expected)

    def test_registered(self):
        """
        Test as a registered operation
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(harmonize))
        dataset = xr.Dataset({'first': (['latitude',
                                         'longitude'], [[1, 2, 3],
                                                        [2, 3, 4]])})
        expected = xr.Dataset({'first': (['lat', 'lon'], [[1, 2, 3],
                                                          [2, 3, 4]])})
        actual = reg_op(ds=dataset)
        assertDatasetEqual(actual, expected)
