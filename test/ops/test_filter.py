from unittest import TestCase
import xarray as xr

from ect.ops.filter import filter_dataset


class TestFilter(TestCase):
    def test_filter(self):
        dataset = xr.Dataset({'abc': ('x', [1, 2, 3]),
                               'bde': ('x', [4, 5, 6])})

        actual = filter_dataset(dataset)
        self.assertDatasetEqual(dataset, actual)

        expected = xr.Dataset({'abc': ('x', [1, 2, 3])})
        expected = expected.drop('abc')
        actual = filter_dataset(dataset, variable_names=['xyz'])
        self.assertDatasetEqual(expected, actual)

        actual = filter_dataset(dataset, variable_names=['abc'])
        expected = xr.Dataset({'abc': ('x', [1, 2, 3])})
        self.assertDatasetEqual(expected, actual)

        actual = filter_dataset(dataset, variable_names=['.*b.*'], regex=True)
        self.assertDatasetEqual(dataset, actual)

        actual = filter_dataset(dataset, variable_names=['.*c.*'], regex=True)
        expected = xr.Dataset({'abc': ('x', [1, 2, 3])})
        self.assertDatasetEqual(expected, actual)

    def assertDatasetEqual(self, expected, actual):
        # this method is functionally equivalent to `assert expected == actual`, but it
        # checks each aspect of equality separately for easier debugging
        assert expected.equals(actual), (expected, actual)
