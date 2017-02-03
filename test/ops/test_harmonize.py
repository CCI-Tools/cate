"""
Test for the harmonization operation
"""

from unittest import TestCase

import xarray as xr

from cate.ops.harmonize import harmonize


class TestHarmonize(TestCase):
    def test_harmonize(self):
        dataset = xr.Dataset({'first': (['latitude',
                                         'longitude'], [[1, 2, 3],
                                                        [2, 3, 4]])})
        expected = xr.Dataset({'first': (['lat', 'lon'], [[1, 2, 3],
                                                          [2, 3, 4]])})
        actual = harmonize(dataset)
        self.assertDatasetEqual(actual, expected)

        dataset = xr.Dataset({'first': (['lat', 'long'], [[1, 2, 3],
                                                          [2, 3, 4]])})
        expected = xr.Dataset({'first': (['lat', 'lon'], [[1, 2, 3],
                                                          [2, 3, 4]])})
        actual = harmonize(dataset)
        self.assertDatasetEqual(actual, expected)

        dataset = xr.Dataset({'first': (['latitude',
                                         'spacetime'], [[1, 2, 3],
                                                        [2, 3, 4]])})
        expected = xr.Dataset({'first': (['lat', 'spacetime'], [[1, 2, 3],
                                                                [2, 3, 4]])})
        actual = harmonize(dataset)
        self.assertDatasetEqual(actual, expected)

        dataset = xr.Dataset({'first': (['zef', 'spacetime'], [[1, 2, 3],
                                                               [2, 3, 4]])})
        expected = xr.Dataset({'first': (['zef', 'spacetime'], [[1, 2, 3],
                                                                [2, 3, 4]])})
        actual = harmonize(dataset)
        self.assertDatasetEqual(actual, expected)

    def assertDatasetEqual(self, expected, actual):
        # this method is functionally equivalent to
        # `assert expected == actual`, but it
        # checks each aspect of equality separately for easier debugging
        assert expected.equals(actual), (expected, actual)
