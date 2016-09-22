from unittest import TestCase
import xarray as xr

from ect.ops.select import select_variables


class TestSelect(TestCase):
    def test_select(self):
        dataset = xr.Dataset({'abc': ('x', [1, 2, 3]),
                               'bde': ('x', [4, 5, 6])})

        # Test if nothing gets dropped if nothing has to be dropped
        actual = select_variables(dataset)
        self.assertDatasetEqual(dataset, actual)

        actual = select_variables(dataset, variable_names='')
        self.assertDatasetEqual(dataset, actual)

        # Test that everything is dropped if the desired name does not exist in
        # the dataset
        expected = xr.Dataset({'abc': ('x', [1, 2, 3])})
        expected = expected.drop('abc')
        actual = select_variables(dataset, variable_names='xyz')
        self.assertDatasetEqual(expected, actual)

        # Test that a single variable selection works
        actual = select_variables(dataset, variable_names='abc')
        expected = xr.Dataset({'abc': ('x', [1, 2, 3])})
        self.assertDatasetEqual(expected, actual)

        # Test that regex selection works
        actual = select_variables(dataset, variable_names='.*b.*')
        self.assertDatasetEqual(dataset, actual)

        actual = select_variables(dataset, variable_names='.*c.*')
        expected = xr.Dataset({'abc': ('x', [1, 2, 3])})
        self.assertDatasetEqual(expected, actual)

        # Test that simple multiple variable selection works
        actual = select_variables(dataset, variable_names='abc|bde')
        self.assertDatasetEqual(dataset, actual)

    def assertDatasetEqual(self, expected, actual):
        # this method is functionally equivalent to `assert expected == actual`, but it
        # checks each aspect of equality separately for easier debugging
        assert expected.equals(actual), (expected, actual)
