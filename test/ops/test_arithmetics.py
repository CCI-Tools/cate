"""
Tests for arithmetic operations
"""

from unittest import TestCase

import numpy as np
import xarray as xr
from datetime import datetime

from cate.ops import arithmetics
from cate.core.op import OP_REGISTRY
from cate.util.misc import object_to_qualified_name


def assert_dataset_equal(expected, actual):
    # this method is functionally equivalent to
    # `assert expected == actual`, but it checks each aspect
    # of equality separately for easier debugging
    assert expected.equals(actual), (expected, actual)


class TestDsArithmetics(TestCase):
    """
    Test dataset arithmetic operations
    """

    def test_nominal(self):
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        actual = arithmetics.ds_arithmetics(dataset, '+2, -2, *3, /3, *4')
        assert_dataset_equal(expected * 4, actual)

        actual = arithmetics.ds_arithmetics(dataset,
                                            'exp, log, *10, log10, *2, log2, +1.5')
        assert_dataset_equal(expected * 2.5, actual)

        actual = arithmetics.ds_arithmetics(dataset, 'exp, -1, log1p, +3')
        assert_dataset_equal(expected * 4, actual)

        with self.assertRaises(ValueError) as err:
            arithmetics.ds_arithmetics(dataset, 'not')
        self.assertTrue('not implemented' in str(err.exception))

    def test_registered(self):
        """
        Test the operation when invoked through the OP_REGISTRY
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(arithmetics.ds_arithmetics))
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        actual = reg_op(ds=dataset, op='+2, -2, *3, /3, *4')
        assert_dataset_equal(expected * 4, actual)


class TestDiff(TestCase):
    """
    Test taking the difference between two datasets
    """

    def test_diff(self):
        # Test nominal
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})
        actual = arithmetics.diff(dataset, dataset * 2)
        assert_dataset_equal(expected * -1, actual)

        # Test variable mismatch
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'third': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        ds1 = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        actual = arithmetics.diff(ds, ds1)
        assert_dataset_equal(expected, actual)
        actual = arithmetics.diff(ds1, ds)
        assert_dataset_equal(expected, actual)

        # Test date range mismatch
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)]})

        ds1 = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2003, x, 1) for x in range(1, 13)]})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        actual = arithmetics.diff(ds, ds1)
        assert_dataset_equal(actual, expected)

        actual = arithmetics.diff(ds, ds1.drop('time'))
        expected['time'] = [datetime(2000, x, 1) for x in range(1, 13)]
        assert_dataset_equal(actual, expected)

        # Test broadcasting
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        ds1 = xr.Dataset({
            'first': (['lat', 'lon'], np.ones([45, 90])),
            'second': (['lat', 'lon'], np.ones([45, 90])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})
        actual = arithmetics.diff(ds, ds1)
        assert_dataset_equal(expected, actual)

        ds['time'] = [datetime(2000, x, 1) for x in range(1, 4)]
        expected['time'] = [datetime(2000, x, 1) for x in range(1, 4)]
        actual = arithmetics.diff(ds, ds1)
        assert_dataset_equal(expected, actual)

        ds1 = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 1])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 1])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2001, 1, 1)]})
        actual = arithmetics.diff(ds, ds1)
        assert_dataset_equal(expected, actual)

        ds1 = ds1.squeeze('time')
        ds1['time'] = 1
        actual = arithmetics.diff(ds, ds1)
        assert_dataset_equal(expected, actual)

    def test_registered(self):
        """
        Test the operation when invoked from the OP_REGISTRY
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(arithmetics.diff))
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 3])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})
        actual = reg_op(ds=dataset, ds2=dataset * 2)
        assert_dataset_equal(expected * -1, actual)


# noinspection PyMethodMayBeStatic
class ComputeTest(TestCase):

    def test_plain_compute(self):
        first = np.ones([45, 90, 3])
        second = np.ones([45, 90, 3])
        lon = np.linspace(-178, 178, 90)
        lat = np.linspace(-88, 88, 45)
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], first),
            'second': (['lat', 'lon', 'time'], second),
            'lat': lat,
            'lon': lon
        })
        actual = arithmetics.compute_dataset(ds=dataset,
                                             script="third = 6 * first - 3 * second")
        expected = xr.Dataset({
            'third': (['lat', 'lon', 'time'], 6 * first - 3 * second),
            'lat': lat,
            'lon': lon
        })
        assert_dataset_equal(expected, actual)

        actual = arithmetics.compute_dataset(ds=dataset,
                                             script="third = 6 * first - 3 * second",
                                             copy=True)
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], first),
            'second': (['lat', 'lon', 'time'], second),
            'third': (['lat', 'lon', 'time'], 6 * first - 3 * second),
            'lat': lat,
            'lon': lon
        })
        assert_dataset_equal(expected, actual)

    def test_registered_compute(self):
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(arithmetics.compute_dataset))
        first = np.ones([45, 90, 3])
        second = np.ones([45, 90, 3])
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], first),
            'second': (['lat', 'lon', 'time'], second),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})
        actual = reg_op(ds=dataset,
                        script="third = 6 * first - 3 * second")
        expected = xr.Dataset({
            'third': (['lat', 'lon', 'time'], 6 * first - 3 * second),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})
        assert_dataset_equal(expected, actual)

        actual = reg_op(ds=dataset,
                        script="third = 6 * first - 3 * second",
                        copy=True)
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], first),
            'second': (['lat', 'lon', 'time'], second),
            'third': (['lat', 'lon', 'time'], 6 * first - 3 * second),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})
        assert_dataset_equal(expected, actual)

    def test_plain_compute_with_context(self):
        first = np.ones([45, 90, 3])
        second = np.ones([45, 90, 3])
        lon = np.linspace(-178, 178, 90)
        lat = np.linspace(-88, 88, 45)

        res_1 = xr.Dataset({
            'first': (['lat', 'lon', 'time'], first),
            'lat': lat,
            'lon': lon
        })
        res_2 = xr.Dataset({
            'second': (['lat', 'lon', 'time'], second),
            'lat': lat,
            'lon': lon
        })

        # Note, if executed from a workflow, _ctx will be set by the framework
        _ctx = dict(value_cache=dict(res_1=res_1, res_2=res_2))
        actual = arithmetics.compute_dataset(ds=None,
                                             script="third = 6 * res_1.first - 3 * res_2.second",
                                             _ctx=_ctx)
        expected = xr.Dataset({
            'third': (['lat', 'lon', 'time'], 6 * first - 3 * second),
            'lat': lat,
            'lon': lon})
        assert_dataset_equal(expected, actual)

    def test_registered_compute_with_context(self):
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(arithmetics.compute_dataset))
        first = np.ones([45, 90, 3])
        second = np.ones([45, 90, 3])
        lon = np.linspace(-178, 178, 90)
        lat = np.linspace(-88, 88, 45)

        res_1 = xr.Dataset({
            'first': (['lat', 'lon', 'time'], first),
            'lat': lat,
            'lon': lon
        })
        res_2 = xr.Dataset({
            'second': (['lat', 'lon', 'time'], second),
            'lat': lat,
            'lon': lon
        })

        # Note, if executed from a workflow, _ctx will be set by the framework
        _ctx = dict(value_cache=dict(res_1=res_1, res_2=res_2))
        actual = reg_op(ds=None,
                        script="third = 6 * res_1.first - 3 * res_2.second",
                        _ctx=_ctx)
        expected = xr.Dataset({
            'third': (['lat', 'lon', 'time'], 6 * first - 3 * second),
            'lat': lat,
            'lon': lon})
        assert_dataset_equal(expected, actual)
