"""
Test the IO operations
"""

from unittest import TestCase

import numpy as np
import pandas as pd
import xarray as xr

from cate.ops.xarray import sel as sel_op
from cate.ops.xarray import from_dataframe
from cate.core.op import OP_REGISTRY
from cate.util.misc import object_to_qualified_name


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


class TestSel(TestCase):
    def test_nominal(self):
        ds = new_ds()

        sel_ds = sel_op(ds=ds, time='2014-09-06')
        self.assertEqual(set(sel_ds.coords.keys()), {'lon', 'lat', 'time', 'reference_time'})
        self.assertEqual(sel_ds.dims['lon'], 4)
        self.assertEqual(sel_ds.dims['lat'], 2)
        self.assertNotIn('time', sel_ds.dims)

        sel_ds = sel_op(ds=ds, lat=10.25, lon=34.51)
        self.assertEqual(set(sel_ds.coords.keys()), {'lon', 'lat', 'time', 'reference_time'})
        self.assertNotIn('lon', sel_ds.dims)
        self.assertNotIn('lat', sel_ds.dims)
        self.assertEqual(sel_ds.dims['time'], 10)

    def test_registered(self):
        """
        Test execution as a registered operation
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(sel_op))

        ds = new_ds()

        sel_ds = reg_op(ds=ds, time='2014-09-06')
        self.assertEqual(set(sel_ds.coords.keys()), {'lon', 'lat', 'time', 'reference_time'})
        self.assertEqual(sel_ds.dims['lon'], 4)
        self.assertEqual(sel_ds.dims['lat'], 2)
        self.assertNotIn('time', sel_ds.dims)

        sel_ds = reg_op(ds=ds, lat=10.25, lon=34.51)
        self.assertEqual(set(sel_ds.coords.keys()), {'lon', 'lat', 'time', 'reference_time'})
        self.assertNotIn('lon', sel_ds.dims)
        self.assertNotIn('lat', sel_ds.dims)
        self.assertEqual(sel_ds.dims['time'], 10)
        pass


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
