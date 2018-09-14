"""
Test the IO operations
"""

from unittest import TestCase

import numpy as np
import pandas as pd
import xarray as xr

from cate.core.op import OP_REGISTRY
from cate.core.types import ValidationError
from cate.ops.utility import merge, sel, from_dataframe, identity, literal, pandas_fillna
from cate.util.misc import object_to_qualified_name


class MergeTest(TestCase):
    def test_nominal(self):
        """
        Test nominal execution
        """
        periods = 5
        time = pd.date_range('2000-01-01', periods=periods)

        ds_1 = xr.Dataset({'A': (['time'], np.random.randn(periods)),
                           'B': (['time'], np.random.randn(periods)),
                           'time': time})
        ds_2 = xr.Dataset({'C': (['time'], np.random.randn(periods)),
                           'D': (['time'], np.random.randn(periods)),
                           'time': time})
        new_ds = merge(ds_1=ds_1, ds_2=ds_2, ds_3=None, ds_4=None)
        self.assertTrue('A' in new_ds)
        self.assertTrue('B' in new_ds)
        self.assertTrue('C' in new_ds)
        self.assertTrue('D' in new_ds)

        new_ds = merge(ds_1=ds_1, ds_2=ds_1, ds_3=ds_1, ds_4=ds_2)
        self.assertTrue('A' in new_ds)
        self.assertTrue('B' in new_ds)
        self.assertTrue('C' in new_ds)
        self.assertTrue('D' in new_ds)

        new_ds = merge(ds_1=ds_1, ds_2=ds_1, ds_3=ds_1, ds_4=ds_1)
        self.assertIs(new_ds, ds_1)

        new_ds = merge(ds_1=ds_2, ds_2=ds_2, ds_3=ds_2, ds_4=ds_2)
        self.assertIs(new_ds, ds_2)

        ds_3 = xr.Dataset({'E': (['time'], np.random.randn(periods)),
                           'time': time})
        new_ds = merge(ds_1=ds_1, ds_2=ds_2, ds_3=ds_3, ds_4=None)
        self.assertTrue('A' in new_ds)
        self.assertTrue('B' in new_ds)
        self.assertTrue('C' in new_ds)
        self.assertTrue('D' in new_ds)
        self.assertTrue('E' in new_ds)

        ds_4 = xr.Dataset({'F': (['time'], np.random.randn(periods)),
                           'time': time})
        new_ds = merge(ds_1=ds_1, ds_2=ds_2, ds_3=ds_3, ds_4=ds_4)
        self.assertTrue('A' in new_ds)
        self.assertTrue('B' in new_ds)
        self.assertTrue('C' in new_ds)
        self.assertTrue('D' in new_ds)
        self.assertTrue('E' in new_ds)

    def test_failures(self):
        with self.assertRaises(ValidationError):
            merge(ds_1=None, ds_2=None, ds_3=None, ds_4=None)


class SelTest(TestCase):
    def test_nominal(self):
        ds = new_ds()

        sel_ds = sel(ds=ds, time='2014-09-06')
        self.assertEqual(set(sel_ds.coords.keys()), {'lon', 'lat', 'time', 'reference_time'})
        self.assertEqual(sel_ds.dims['lon'], 4)
        self.assertEqual(sel_ds.dims['lat'], 2)
        self.assertNotIn('time', sel_ds.dims)

        sel_ds = sel(ds=ds, point=(34.51, 10.25))
        self.assertEqual(set(sel_ds.coords.keys()), {'lon', 'lat', 'time', 'reference_time'})
        self.assertNotIn('lon', sel_ds.dims)
        self.assertNotIn('lat', sel_ds.dims)
        self.assertEqual(sel_ds.dims['time'], 10)

    def test_registered(self):
        """
        Test execution as a registered operation
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(sel))

        ds = new_ds()

        sel_ds = reg_op(ds=ds, time='2014-09-06')
        self.assertEqual(set(sel_ds.coords.keys()), {'lon', 'lat', 'time', 'reference_time'})
        self.assertEqual(sel_ds.dims['lon'], 4)
        self.assertEqual(sel_ds.dims['lat'], 2)
        self.assertNotIn('time', sel_ds.dims)

        sel_ds = reg_op(ds=ds, point=(34.51, 10.25))
        self.assertEqual(set(sel_ds.coords.keys()), {'lon', 'lat', 'time', 'reference_time'})
        self.assertNotIn('lon', sel_ds.dims)
        self.assertNotIn('lat', sel_ds.dims)
        self.assertEqual(sel_ds.dims['time'], 10)


class TestFromDataframe(TestCase):
    def test_nominal(self):
        """
        Test nominal execution
        """
        time = pd.date_range('2000-01-01', periods=10)
        data = {'A': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                'B': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                'time': time}
        df = pd.DataFrame(data)
        df = df.set_index('time')

        expected = xr.Dataset({
            'A': (['time'], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            'B': (['time'], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            'time': time})

        actual = from_dataframe(df)
        assert_dataset_equal(expected, actual)

    def test_registered(self):
        """
        Test nominal execution as a registered operation
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(from_dataframe))

        time = pd.date_range('2000-01-01', periods=10)
        data = {'A': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                'B': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                'time': time}
        df = pd.DataFrame(data)
        df = df.set_index('time')

        expected = xr.Dataset({
            'A': (['time'], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            'B': (['time'], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            'time': time})

        actual = reg_op(df=df)
        assert_dataset_equal(expected, actual)


class TestIdentity(TestCase):
    def test_nominal(self):
        """
        Test nominal execution
        """
        self.assertEqual(identity(True), True)
        self.assertEqual(identity(42), 42)
        self.assertEqual(identity(3.14), 3.14)
        self.assertEqual(identity('ha'), 'ha')
        self.assertEqual(identity([3, 4, 5]), [3, 4, 5])

    def test_registered(self):
        """
        Test nominal execution as a registered operation
        """
        op = OP_REGISTRY.get_op(object_to_qualified_name(identity))
        self.assertEqual(op(value=True), True)
        self.assertEqual(op(value=42), 42)
        self.assertEqual(op(value=3.14), 3.14)
        self.assertEqual(op(value='ha'), 'ha')
        self.assertEqual(op(value=[3, 4, 5]), [3, 4, 5])


class TestLiteral(TestCase):
    def test_nominal(self):
        """
        Test nominal execution
        """
        self.assertEqual(literal('True'), True)
        self.assertEqual(literal('42'), 42)
        self.assertEqual(literal('3.14'), 3.14)
        self.assertEqual(literal('"ha"'), 'ha')
        self.assertEqual(literal('[3,4,5]'), [3, 4, 5])

    def test_registered(self):
        """
        Test nominal execution as a registered operation
        """
        op = OP_REGISTRY.get_op(object_to_qualified_name(literal))
        self.assertEqual(op(value='True'), True)
        self.assertEqual(op(value='42'), 42)
        self.assertEqual(op(value='3.14'), 3.14)
        self.assertEqual(op(value='"ha"'), 'ha')
        self.assertEqual(op(value='[3,4,5]'), [3, 4, 5])


class TestFillna(TestCase):
    """
    Test fillna operation
    """

    def test_nominal(self):
        """
        Test nominal operation
        """
        # Test na filling using a given method
        data = {'A': [1, 2, 3, np.nan, 4, 9, np.nan, np.nan, 1, 0, 4, 6],
                'B': [5, 6, 8, 7, 5, np.nan, np.nan, np.nan, 1, 2, 7, 6]}
        expected = {'A': [1, 2, 3, 3, 4, 9, 9, 9, 1, 0, 4, 6],
                    'B': [5, 6, 8, 7, 5, 5, 5, 5, 1, 2, 7, 6]}
        time = pd.date_range('2000-01-01', freq='MS', periods=12)

        expected = pd.DataFrame(data=expected, index=time, dtype=float)
        df = pd.DataFrame(data=data, index=time, dtype=float)

        actual = pandas_fillna(df, method='ffill')
        self.assertTrue(actual.equals(expected))

        # Test na filling using a given value
        actual = pandas_fillna(df, value=3.14)
        expected = {'A': [1, 2, 3, 3.14, 4, 9, 3.14, 3.14, 1, 0, 4, 6],
                    'B': [5, 6, 8, 7, 5, 3.14, 3.14, 3.14, 1, 2, 7, 6]}
        expected = pd.DataFrame(data=expected, index=time, dtype=float)
        self.assertTrue(actual.equals(expected))

    def test_registered(self):
        """
        Test operation when run as a registered operation
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(pandas_fillna))
        # Test na filling using a given method
        data = {'A': [1, 2, 3, np.nan, 4, 9, np.nan, np.nan, 1, 0, 4, 6],
                'B': [5, 6, 8, 7, 5, np.nan, np.nan, np.nan, 1, 2, 7, 6]}
        expected = {'A': [1, 2, 3, 3, 4, 9, 9, 9, 1, 0, 4, 6],
                    'B': [5, 6, 8, 7, 5, 5, 5, 5, 1, 2, 7, 6]}
        time = pd.date_range('2000-01-01', freq='MS', periods=12)

        expected = pd.DataFrame(data=expected, index=time, dtype=float)
        df = pd.DataFrame(data=data, index=time, dtype=float)

        actual = reg_op(df=df, method='ffill')
        self.assertTrue(actual.equals(expected))


def new_ds():
    lon = [10.1, 10.2, 10.3, 10.4]
    lat = [34.5, 34.6]
    time = pd.date_range('2014-09-06', periods=10)
    reference_time = pd.Timestamp('2014-09-05')

    time_res = len(time)
    lon_res = len(lon)
    lat_res = len(lat)

    temperature = (15 + 8 * np.random.randn(lon_res, lat_res, time_res)).round(decimals=1)
    precipitation = (10 * np.random.rand(lon_res, lat_res, time_res)).round(decimals=1)

    ds = xr.Dataset({'temperature': (['lon', 'lat', 'time'], temperature),
                     'precipitation': (['lon', 'lat', 'time'], precipitation)
                     },
                    coords={'lon': lon,
                            'lat': lat,
                            'time': time,
                            'reference_time': reference_time
                            })
    return ds


def assert_dataset_equal(expected, actual):
    # this method is functionally equivalent to
    # `assert expected == actual`, but it checks each aspect
    # of equality separately for easier debugging
    assert expected.equals(actual), (expected, actual)
