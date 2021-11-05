"""
Tests for arithmetic operations
"""

from datetime import datetime
from unittest import TestCase

import numpy as np
import pandas as pd
import xarray as xr

from cate.core.op import OP_REGISTRY
from cate.ops import arithmetics
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

        actual = arithmetics.diff(ds, ds1.drop_vars(['time', ]))
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


class ComputeDatasetTest(TestCase):

    def test_plain_compute(self):
        da1 = np.ones([45, 90, 3])
        da2 = np.ones([45, 90, 3])
        lon = np.linspace(-178, 178, 90)
        lat = np.linspace(-88, 88, 45)

        ds1 = xr.Dataset({
            'da1': (['lat', 'lon', 'time'], da1),
            'da2': (['lat', 'lon', 'time'], da2),
            'lat': lat,
            'lon': lon
        })

        ds2 = arithmetics.compute_dataset(ds=ds1,
                                          script="_x = 0.5 * da2\n"
                                                 "x1 = 2 * da1 - 3 * _x\n"
                                                 "x2 = 3 * da1 + 4 * _x\n")
        self.assertIsInstance(ds2, xr.Dataset)
        self.assertIn('lon', ds2)
        self.assertIn('lat', ds2)
        self.assertIn('x1', ds2)
        self.assertIn('x2', ds2)
        self.assertNotIn('da1', ds2)
        self.assertNotIn('da2', ds2)
        _x = 0.5 * da2
        expected_x1 = 2 * da1 - 3 * _x
        expected_x2 = 3 * da1 + 4 * _x
        np.testing.assert_array_almost_equal(expected_x1, ds2['x1'].values)
        np.testing.assert_array_almost_equal(expected_x2, ds2['x2'].values)

        ds2 = arithmetics.compute_dataset(ds=ds1,
                                          script="_x = 0.6 * da2\n"
                                                 "x1 = 4 * da1 - 4 * _x\n"
                                                 "x2 = 5 * da1 + 3 * _x\n",
                                          copy=True)
        self.assertIsInstance(ds2, xr.Dataset)
        self.assertIn('lon', ds2)
        self.assertIn('lat', ds2)
        self.assertIn('x1', ds2)
        self.assertIn('x2', ds2)
        self.assertIn('da1', ds2)
        self.assertIn('da2', ds2)
        _x = 0.6 * da2
        expected_x1 = 4 * da1 - 4 * _x
        expected_x2 = 5 * da1 + 3 * _x
        np.testing.assert_array_almost_equal(expected_x1, ds2['x1'].values)
        np.testing.assert_array_almost_equal(expected_x2, ds2['x2'].values)

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
        self.assertIsInstance(actual, xr.Dataset)
        expected = xr.Dataset({
            'third': (['lat', 'lon', 'time'], 6 * first - 3 * second),
            'lat': lat,
            'lon': lon})
        assert_dataset_equal(expected, actual)


class ComputeDataFrameTest(TestCase):

    def test_compute_simple(self):
        s1 = 10. * np.linspace(0, 1, 11)
        s2 = -2 * np.linspace(0, 1, 11)
        s3 = +2 * np.linspace(0, 1, 11)

        df1 = pd.DataFrame(dict(s1=s1, s2=s2, s3=s3))

        df2 = arithmetics.compute_data_frame(df=df1,
                                             script="_a = 3 * s2 - 4 * s3\n"
                                                    "a1 = 1 + 2 * s1 + _a\n"
                                                    "a2 = 2 + 3 * s1 + _a\n")

        self.assertIsInstance(df2, pd.DataFrame)
        self.assertEqual(11, len(df2))
        self.assertIn('a1', df2)
        self.assertIn('a2', df2)
        self.assertNotIn('_a', df2)
        self.assertNotIn('s1', df2)
        self.assertNotIn('s2', df2)
        self.assertNotIn('s3', df2)
        expected_a = 3 * s2 - 4 * s3
        expected_a1 = 1 + 2 * s1 + expected_a
        expected_a2 = 2 + 3 * s1 + expected_a
        np.testing.assert_array_almost_equal(expected_a1, df2['a1'].values)
        np.testing.assert_array_almost_equal(expected_a2, df2['a2'].values)

        df2 = arithmetics.compute_data_frame(df=df1,
                                             script="_a = 3 * s2 - 4 * s3\n"
                                                    "a1 = 1 + 2 * s1 + _a\n"
                                                    "a2 = 2 + 3 * s1 + _a\n",
                                             copy=True)

        self.assertIsInstance(df2, pd.DataFrame)
        self.assertEqual(11, len(df2))
        self.assertIn('a1', df2)
        self.assertIn('a2', df2)
        self.assertNotIn('_a', df2)
        self.assertIn('s1', df2)
        self.assertIn('s2', df2)
        self.assertIn('s3', df2)
        expected_a = 3 * s2 - 4 * s3
        expected_a1 = 1 + 2 * s1 + expected_a
        expected_a2 = 2 + 3 * s1 + expected_a
        np.testing.assert_array_almost_equal(expected_a1, df2['a1'].values)
        np.testing.assert_array_almost_equal(expected_a2, df2['a2'].values)

    def test_compute_aggregations(self):
        s1 = 10. * np.linspace(0, 1, 11)
        s2 = -2 * np.linspace(0, 1, 11)
        s3 = +2 * np.linspace(0, 1, 11)

        df1 = pd.DataFrame(dict(s1=s1, s2=s2, s3=s3))

        df2 = arithmetics.compute_data_frame(df=df1,
                                             script="s1_mean = s1.mean()\n"
                                                    "s2_sum = s2.sum()\n"
                                                    "s3_median = s3.median()\n")

        self.assertIsInstance(df2, pd.DataFrame)
        self.assertEqual(1, len(df2))
        self.assertIn('s1_mean', df2)
        self.assertIn('s2_sum', df2)
        self.assertIn('s3_median', df2)
        self.assertNotIn('s1', df2)
        self.assertNotIn('s2', df2)
        self.assertNotIn('s3', df2)
        np.testing.assert_almost_equal(np.mean(s1), df2['s1_mean'].values)
        np.testing.assert_almost_equal(np.sum(s2), df2['s2_sum'].values)
        np.testing.assert_almost_equal(np.median(s3), df2['s3_median'].values)

        df2 = arithmetics.compute_data_frame(df=df1,
                                             script="s1_mean = s1.mean()\n"
                                                    "s2_sum = s2.sum()\n"
                                                    "s3_median = s3.median()\n",
                                             copy=True)

        self.assertIsInstance(df2, pd.DataFrame)
        self.assertEqual(11, len(df2))
        self.assertIn('s1_mean', df2)
        self.assertIn('s2_sum', df2)
        self.assertIn('s3_median', df2)
        self.assertIn('s1', df2)
        self.assertIn('s2', df2)
        self.assertIn('s3', df2)

        np.testing.assert_almost_equal(np.mean(s1), df2['s1_mean'].values)
        np.testing.assert_almost_equal(np.sum(s2), df2['s2_sum'].values)
        np.testing.assert_almost_equal(np.median(s3), df2['s3_median'].values)
