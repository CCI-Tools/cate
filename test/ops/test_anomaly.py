"""
Tests for anomaly operations
"""

from unittest import TestCase

import os
import numpy as np
import xarray as xr
from datetime import datetime

from cate.ops import anomaly
from cate.ops import subset_spatial
from cate.core.op import OP_REGISTRY
from cate.util.misc import object_to_qualified_name
from cate.util.monitor import ConsoleMonitor


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
        """
        Nominal execution test
        """
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

    def test_partial(self):
        """
        Test situations where the given dataset does not correspond perfectly
        to the reference dataset.
        """
        # Test mismatching variable names
        ref = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1,13)]+\
                    [datetime(2001, x, 1) for x in range(1,13)]})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1,13)]+\
                    [datetime(2001, x, 1) for x in range(1,13)]})

        ref.to_netcdf(self._TEMP)
        actual = anomaly.anomaly_external(ds, self._TEMP)
        assert_dataset_equal(actual, expected)

        # Test differing spatial extents
        ds = subset_spatial(ds, '-50, -50, 50, 50')
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([25, 26, 24])),
            'lat': np.linspace(-48, 48, 25),
            'lon': np.linspace(-50, 50, 26),
            'time': [datetime(2000, x, 1) for x in range(1,13)]+\
                    [datetime(2001, x, 1) for x in range(1,13)]})
        actual = anomaly.anomaly_external(ds, self._TEMP)
        assert_dataset_equal(actual, expected)
        self.cleanup()

    def test_monitor(self):
        """
        Test monitor integration
        """
        ref = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1,13)]+\
                    [datetime(2001, x, 1) for x in range(1,13)]})

        ref.to_netcdf(self._TEMP)
        m = ConsoleMonitor()
        anomaly.anomaly_external(ds, self._TEMP, monitor=m)
        self.cleanup()
        self.assertEqual(m._percentage, 100)

    def test_transform(self):
        """
        Test the application of an arithmetic transormation to the dataset, as
        part of the anomaly calculation.
        """
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
        self.cleanup()

    def test_dask(self):
        """
        Test if the operation works with xarray datasets with dask as the
        backend.
        """
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
        # Test that it is indeed the case
        self.assertFalse(not ds.chunks)

        actual = anomaly.anomaly_external(ds, self._TEMP)
        assert_dataset_equal(actual, expected)
        # Test that actual is also a dask array, based on ds
        self.assertEqual(actual.chunks, ds.chunks)
        self.cleanup()

    def test_registered(self):
        """
        Test the operation when it is invoked through the operation registry
        """
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

    def test_validation(self):
        """
        Test input validation
        """
        # Test wrong dtype
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
            'time': [x for x in range(0, 24)]})

        ref.to_netcdf(self._TEMP)
        with self.assertRaises(ValueError) as err:
            anomaly.anomaly_external(ds, self._TEMP)
        self.assertIn('dtype datetime', str(err.exception))

        # Test missing time coordinate
        ds = xr.Dataset({
            'first': (['lat', 'lon'], np.ones([45, 90])),
            'second': (['lat', 'lon'], np.ones([45, 90])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})
        with self.assertRaises(ValueError) as err:
            anomaly.anomaly_external(ds, self._TEMP)
        self.assertIn('time coordinate.', str(err.exception))
        self.cleanup()


class TestInternal(TestCase):
    """
    Test anomaly calculation with internal reference
    """
    def test_nominal(self):
        """
        Test nominal execution
        """
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)]})
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)]})
        actual = anomaly.anomaly_internal(ds,
                                          '2000-01-01, 2000-04-01',
                                          '-50, -50, 50, 50')
        assert_dataset_equal(expected, actual)

        actual = anomaly.anomaly_internal(ds)
        assert_dataset_equal(expected, actual)

    def test_registered(self):
        """
        Test nominal execution through the operations registry.
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(anomaly.anomaly_internal))
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)]})
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)]})
        actual = reg_op(ds=ds,
                        time_range='2000-01-01, 2000-04-01',
                        region='-50, -50, 50, 50')
        assert_dataset_equal(expected, actual)
