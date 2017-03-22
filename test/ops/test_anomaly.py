"""
Tests for anomaly operations
"""

from unittest import TestCase

import os
import numpy as np
import xarray as xr
from datetime import datetime

from cate.ops import anomaly
from cate.core.op import OP_REGISTRY
from cate.util.misc import object_to_qualified_name


def assert_dataset_equal(expected, actual):
    # this method is functionally equivalent to
    # `assert expected == actual`, but it checks each aspect
    # of equality separately for easier debugging
    assert expected.equals(actual), (expected, actual)


class TestExternal(TestCase):
    """
    Test anomaly calculation with external reference
    """
    def setUp(self):
        self._TEMP = 'temp_ref.nc'
        self._TEMP_DS = 'temp_ds.nc'

    def tearDown(self):
        self.cleanup()

    def cleanup(self):
        try:
            os.remove(self._TEMP)
        except:
            pass

        try:
            os.remove(self._TEMP_DS)
        except:
            pass


    def test_nominal(self):
        # Test nominal
        ref = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1,13)]+\
                    [datetime(2001, x, 1) for x in range(1,13)]})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1,13)]+\
                    [datetime(2001, x, 1) for x in range(1,13)]})

        ref.to_netcdf(self._TEMP)
        actual = anomaly.anomaly_external(ds, self._TEMP)
        assert_dataset_equal(actual, expected)
        self.cleanup()

        # Test with reference data with a labeled time coordinate
        ref['time'] = [datetime(1700, x, 1) for x in range(1,13)]
        ref.to_netcdf(self._TEMP)
        actual = anomaly.anomaly_external(ds, self._TEMP)
        assert_dataset_equal(actual, expected)
        self.cleanup()

    def test_transform(self):
        # Test applying a transformation before doing the anomaly
        ref = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1,13)]+\
                    [datetime(2001, x, 1) for x in range(1,13)]})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1,13)]+\
                    [datetime(2001, x, 1) for x in range(1,13)]})

        ds = ds*10
        expected = expected + 3
        ref.to_netcdf(self._TEMP)
        actual = anomaly.anomaly_external(ds,
                                          self._TEMP,
                                          transform='log10, +3')
        assert_dataset_equal(actual, expected)

    def test_dask(self):
        # Test if the operation works with xarray datasets with dask as the
        # backend.
        pass
        ref = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1,13)]+\
                    [datetime(2001, x, 1) for x in range(1,13)]})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1,13)]+\
                    [datetime(2001, x, 1) for x in range(1,13)]})

        # Test that ds is not a dask array
        self.assertTrue(not ds.chunks)
        ref.to_netcdf(self._TEMP)
        ds.to_netcdf(self._TEMP_DS)
        # This makes ds a dask dataset in xarray backend
        ds = xr.open_dataset(self._TEMP_DS, chunks={})

        actual = anomaly.anomaly_external(ds, self._TEMP)
        assert_dataset_equal(actual, expected)
        # Test that actual is also a dask array, based on ds
        self.assertEqual(actual.chunks, ds.chunks)

    def test_registered(self):
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(anomaly.anomaly_external))
        ref = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1,13)]+\
                    [datetime(2001, x, 1) for x in range(1,13)]})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1,13)]+\
                    [datetime(2001, x, 1) for x in range(1,13)]})

        ref.to_netcdf(self._TEMP)
        actual = reg_op(ds=ds, file=self._TEMP)
        assert_dataset_equal(actual, expected)
        self.cleanup()


class TestInternal(TestCase):
    """
    Test anomaly calculation with internal reference
    """
    def test_nominal(self):
        pass

    def test_registered(self):
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(anomaly.anomaly_internal))
        pass
