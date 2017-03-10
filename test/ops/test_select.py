from unittest import TestCase

import xarray as xr

from cate.ops.select import select_var
from cate.core.op import OP_REGISTRY
from cate.util.misc import object_to_qualified_name


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

    def test_select_registry(self):
        """
        Test the select operation invocation from the operation registry, as
        done by the GUI and the CLI. This tests the @op* decorators in
        conjunction with this op.
        """
        dataset = xr.Dataset({'abc': ('x', [1, 2, 3]),
                              'bde': ('x', [4, 5, 6])})
        op_reg = OP_REGISTRY.get_op(object_to_qualified_name(select_var))

        actual = op_reg(ds=dataset, var='aa')

    def assertDatasetEqual(self, expected, actual):
        # this method is functionally equivalent to `assert expected == actual`, but it
        # checks each aspect of equality separately for easier debugging
        assert expected.equals(actual), (expected, actual)
