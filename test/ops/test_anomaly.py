"""
Tests for anomaly operations
"""

from unittest import TestCase

import os
import sys
import numpy as np
import xarray as xr
from datetime import datetime
import tempfile
import shutil
from contextlib import contextmanager
import itertools

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


_counter = itertools.count()
ON_WIN = sys.platform == 'win32'


@contextmanager
def create_tmp_file():
    tmp_dir = tempfile.mkdtemp()
    path = os.path.join(tmp_dir, 'tmp_file_{}.nc'.format(next(_counter)))
    try:
        yield path
    finally:
        try:
            shutil.rmtree(tmp_dir)
        except OSError:
            if not ON_WIN:
                raise


class TestExternal(TestCase):
    """
    Test anomaly calculation with external reference
    """

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
            'time': [datetime(2000, x, 1) for x in range(1, 13)]
                    + [datetime(2001, x, 1) for x in range(1, 13)]})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)]
                    + [datetime(2001, x, 1) for x in range(1, 13)]})

        with create_tmp_file() as tmp_file:
            ref.to_netcdf(tmp_file, 'w')
            actual = anomaly.anomaly_external(ds, tmp_file)
            assert_dataset_equal(actual, expected)

        # Test with reference data with a labeled time coordinate
        ref['time'] = [datetime(1700, x, 1) for x in range(1, 13)]
        with create_tmp_file() as tmp_file:
            ref.to_netcdf(tmp_file, 'w')
            actual = anomaly.anomaly_external(ds, tmp_file)
            assert_dataset_equal(actual, expected)

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
            'time': [datetime(2000, x, 1) for x in range(1, 13)]
                    + [datetime(2001, x, 1) for x in range(1, 13)]})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)]
                    + [datetime(2001, x, 1) for x in range(1, 13)]})

        with create_tmp_file() as tmp_file:
            ref.to_netcdf(tmp_file, 'w')
            actual = anomaly.anomaly_external(ds, tmp_file)
            assert_dataset_equal(actual, expected)

            # Test differing spatial extents
            ds = subset_spatial(ds, '-50, -50, 50, 50')
            expected = xr.Dataset({
                'first': (['lat', 'lon', 'time'], np.zeros([27, 26, 24])),
                'lat': np.linspace(-52, 52, 27),
                'lon': np.linspace(-50, 50, 26),
                'time': [datetime(2000, x, 1) for x in range(1, 13)]
                        + [datetime(2001, x, 1) for x in range(1, 13)]})
            actual = anomaly.anomaly_external(ds, tmp_file)
            assert_dataset_equal(actual, expected)

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
            'time': [datetime(2000, x, 1) for x in range(1, 13)] + [datetime(2001, x, 1) for x in range(1, 13)]})

        with create_tmp_file() as tmp_file:
            ref.to_netcdf(tmp_file, 'w')
            m = ConsoleMonitor()
            anomaly.anomaly_external(ds, tmp_file, monitor=m)
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
            'time': [datetime(2000, x, 1) for x in range(1, 13)] + [datetime(2001, x, 1) for x in range(1, 13)]})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)] + [datetime(2001, x, 1) for x in range(1, 13)]})

        ds = ds * 10
        expected = expected + 3
        with create_tmp_file() as tmp_file:
            ref.to_netcdf(tmp_file, 'w')
            actual = anomaly.anomaly_external(ds,
                                              tmp_file,
                                              transform='log10, +3')
            assert_dataset_equal(actual, expected)

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
            'time': [datetime(2000, x, 1) for x in range(1, 13)] + [datetime(2001, x, 1) for x in range(1, 13)]})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)] + [datetime(2001, x, 1) for x in range(1, 13)]})

        # Test that ds is not a dask array
        self.assertTrue(not ds.chunks)
        with create_tmp_file() as tmp1:
            ref.to_netcdf(tmp1, 'w')
            with create_tmp_file() as tmp2:
                ds.to_netcdf(tmp2, 'w')
                # This makes ds a dask dataset in xarray backend
                ds = xr.open_dataset(tmp2, chunks={})
                # Test that it is indeed the case
                self.assertFalse(not ds.chunks)

                actual = anomaly.anomaly_external(ds, tmp1)
                assert_dataset_equal(actual, expected)
                # Test that actual is also a dask array
                self.assertFalse(not actual.chunks)

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
            'time': [datetime(2000, x, 1) for x in range(1, 13)] + [datetime(2001, x, 1) for x in range(1, 13)]})

        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.zeros([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.zeros([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [datetime(2000, x, 1) for x in range(1, 13)] + [datetime(2001, x, 1) for x in range(1, 13)]
        })

        with create_tmp_file() as tmp_file:
            ref.to_netcdf(tmp_file, 'w')
            actual = reg_op(ds=ds, file=tmp_file)
            assert_dataset_equal(actual, expected)

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

        with create_tmp_file() as tmp_file:
            ref.to_netcdf(tmp_file, 'w')
            with self.assertRaises(ValueError) as err:
                anomaly.anomaly_external(ds, tmp_file)
            self.assertIn('dtype datetime', str(err.exception))

            # Test missing time coordinate
            ds = xr.Dataset({
                'first': (['lat', 'lon'], np.ones([45, 90])),
                'second': (['lat', 'lon'], np.ones([45, 90])),
                'lat': np.linspace(-88, 88, 45),
                'lon': np.linspace(-178, 178, 90)})
            with self.assertRaises(ValueError) as err:
                anomaly.anomaly_external(ds, tmp_file)
            self.assertIn('time coordinate.', str(err.exception))


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
