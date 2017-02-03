from unittest import TestCase

import xarray as xr

from cate.ops.select import select_var


class TestSelect(TestCase):
    def test_select(self):
        dataset = xr.Dataset({'abc': ('x', [1, 2, 3]),
                              'bde': ('x', [4, 5, 6])})

        # Test if nothing gets dropped if nothing has to be dropped
        actual = select_var(dataset)
        self.assertDatasetEqual(dataset, actual)

        actual = select_var(dataset, var='')
        self.assertDatasetEqual(dataset, actual)

        # Test that everything is dropped if the desired name does not exist in
        # the dataset
        expected = xr.Dataset({'abc': ('x', [1, 2, 3])})
        expected = expected.drop('abc')
        actual = select_var(dataset, var='xyz')
        self.assertDatasetEqual(expected, actual)

        # Test that a single variable selection works
        actual = select_var(dataset, var='abc')
        expected = xr.Dataset({'abc': ('x', [1, 2, 3])})
        self.assertDatasetEqual(expected, actual)

        # Test that simple multiple variable selection works
        actual = select_var(dataset, var='abc,bde')
        self.assertDatasetEqual(dataset, actual)

        # Test that wildcard selection works
        actual = select_var(dataset, var='*b*')
        self.assertDatasetEqual(dataset, actual)

    def assertDatasetEqual(self, expected, actual):
        # this method is functionally equivalent to `assert expected == actual`, but it
        # checks each aspect of equality separately for easier debugging
        assert expected.equals(actual), (expected, actual)
