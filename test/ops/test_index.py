"""
Tests for index operations
"""

from unittest import TestCase

import os
import sys
from datetime import datetime
import tempfile
import shutil
from contextlib import contextmanager
import itertools

import xarray as xr
import pandas as pd
import numpy as np

from cate.ops import index
from cate.ops import subset
from cate.core.op import OP_REGISTRY
from cate.util.misc import object_to_qualified_name


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


class TestEnsoNino34(TestCase):
    def test_nominal(self):
        """
        Test ENSO index calculation using Nino34 region
        """

        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': ([datetime(2001, x, 1) for x in range(1, 13)] +
                     [datetime(2002, x, 1) for x in range(1, 13)])})
        lta = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [x for x in range(1,13)]})
        lta = 2*lta
        expected_time = ([datetime(2001, x, 1) for x in range(3, 13)] +
                         [datetime(2002, x, 1) for x in range(1, 11)])
        expected = pd.DataFrame(data=(np.ones([20])*-1),
                                columns=['ENSO N3.4 Index'],
                                index=expected_time)
        with create_tmp_file() as tmp_file:
            lta.to_netcdf(tmp_file)
            actual = index.enso_nino34(dataset, 'first', tmp_file)
            self.assertTrue(expected.equals(actual))

    def test_threshold(self):
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': ([datetime(2001, x, 1) for x in range(1, 13)] +
                     [datetime(2002, x, 1) for x in range(1, 13)])})
        lta = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [x for x in range(1,13)]})
        lta = 2*lta
        expected_time = ([datetime(2001, x, 1) for x in range(3, 13)] +
                         [datetime(2002, x, 1) for x in range(1, 11)])
        expected = pd.DataFrame(data=(np.ones([20])*-1),
                                columns=['ENSO N3.4 Index'],
                                index=expected_time)
        expected['El Nino'] = pd.Series(np.zeros([20], dtype=bool),
                                        index=expected.index)
        expected['La Nina'] = pd.Series(np.ones([20], dtype=bool),
                                        index=expected.index)
        with create_tmp_file() as tmp_file:
            lta.to_netcdf(tmp_file)
            actual = index.enso_nino34(dataset, 'first', tmp_file,
                                       threshold=0.0)
            self.assertTrue(expected.equals(actual))

    def test_registered(self):
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(index.enso_nino34))
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': ([datetime(2001, x, 1) for x in range(1, 13)] +
                     [datetime(2002, x, 1) for x in range(1, 13)])})
        lta = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [x for x in range(1,13)]})
        lta = 2*lta
        expected_time = ([datetime(2001, x, 1) for x in range(3, 13)] +
                         [datetime(2002, x, 1) for x in range(1, 11)])
        expected = pd.DataFrame(data=(np.ones([20])*-1),
                                columns=['ENSO N3.4 Index'],
                                index=expected_time)
        with create_tmp_file() as tmp_file:
            lta.to_netcdf(tmp_file)
            actual = reg_op(ds=dataset, var='first', file=tmp_file)
            self.assertTrue(expected.equals(actual))


class TestEnso(TestCase):
    def test_nominal(self):
        """
        Test nominal execution of the generic ENSO Index calculation operation
        """

        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': ([datetime(2001, x, 1) for x in range(1, 13)] +
                     [datetime(2002, x, 1) for x in range(1, 13)])})
        lta = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [x for x in range(1,13)]})
        lta = 2*lta
        expected_time = ([datetime(2001, x, 1) for x in range(3, 13)] +
                         [datetime(2002, x, 1) for x in range(1, 11)])
        expected = pd.DataFrame(data=(np.ones([20])*-1),
                                columns=['ENSO N3 Index'],
                                index=expected_time)
        with create_tmp_file() as tmp_file:
            lta.to_netcdf(tmp_file)
            actual = index.enso(dataset, 'first', tmp_file, region='N3')
            self.assertTrue(expected.equals(actual))

    def test_antimeridian(self):
        """
        Test execution with N4 region that crosses the antimeridian
        """

        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': ([datetime(2001, x, 1) for x in range(1, 13)] +
                     [datetime(2002, x, 1) for x in range(1, 13)])})
        lta = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [x for x in range(1,13)]})
        lta = 2*lta
        expected_time = ([datetime(2001, x, 1) for x in range(3, 13)] +
                         [datetime(2002, x, 1) for x in range(1, 11)])
        expected = pd.DataFrame(data=(np.ones([20])*-1),
                                columns=['ENSO N4 Index'],
                                index=expected_time)
        with create_tmp_file() as tmp_file:
            lta.to_netcdf(tmp_file)
            actual = index.enso(dataset, 'first', tmp_file, region='N4')
            self.assertTrue(expected.equals(actual))

    def test_custom_region(self):
        """
        Test execution with a generic WKT poygon
        """

        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': ([datetime(2001, x, 1) for x in range(1, 13)] +
                     [datetime(2002, x, 1) for x in range(1, 13)])})
        lta = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [x for x in range(1,13)]})
        lta = 2*lta
        expected_time = ([datetime(2001, x, 1) for x in range(3, 13)] +
                         [datetime(2002, x, 1) for x in range(1, 11)])
        expected = pd.DataFrame(data=(np.ones([20])*-1),
                                columns=['ENSO Index over POLYGON '
            '((-141.15234375 3.513421045640057, -129.0234375 6.839169626342807,'
            ' -102.65625 6.489983332670652, -90.703125 -3.688855143147035, -110'
            '.21484375 -13.06877673435769, -141.6796875 -6.31529853833002, -141'
            '.15234375 3.513421045640057))'],
                                index=expected_time)
        region = str('POLYGON((-141.15234375 3.513421045640057,-129.0234375'
                     ' 6.839169626342807,-102.65625 6.4899833326706515,-90.703125 '
                     '-3.6888551431470353,-110.21484375 -13.068776734357693,'
                     '-141.6796875 -6.31529853833002,-141.15234375 '
                     '3.513421045640057))')
        with create_tmp_file() as tmp_file:
            lta.to_netcdf(tmp_file)
            actual = index.enso(dataset, 'first', tmp_file, region='custom',
                                custom_region=region)
            self.assertTrue(expected.equals(actual))

        # Test a situation where the user forgets to provide the custom region
        with self.assertRaises(ValueError) as err:
            ret = index.enso(dataset, 'first', 'dummy/file.nc', region='custom')
        self.assertIn('No region', str(err.exception))

    def test_registered(self):
        """
        Test execution as a registered operation.
        """

        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(index.enso))
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': ([datetime(2001, x, 1) for x in range(1, 13)] +
                     [datetime(2002, x, 1) for x in range(1, 13)])})
        lta = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [x for x in range(1,13)]})
        lta = 2*lta
        expected_time = ([datetime(2001, x, 1) for x in range(3, 13)] +
                         [datetime(2002, x, 1) for x in range(1, 11)])
        expected = pd.DataFrame(data=(np.ones([20])*-1),
                                columns=['ENSO N3 Index'],
                                index=expected_time)
        with create_tmp_file() as tmp_file:
            lta.to_netcdf(tmp_file)
            actual = reg_op(ds=dataset, var='first', file=tmp_file, region='N3')
            self.assertTrue(expected.equals(actual))


class TestOni(TestCase):
    def test_nominal(self):
        """
        Test nominal ONI Index calculation execution
        """

        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': ([datetime(2001, x, 1) for x in range(1, 13)] +
                     [datetime(2002, x, 1) for x in range(1, 13)])})
        lta = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [x for x in range(1,13)]})
        lta = 2*lta
        expected_time = ([datetime(2001, x, 1) for x in range(2, 13)] +
                         [datetime(2002, x, 1) for x in range(1, 12)])
        expected = pd.DataFrame(data=(np.ones([22])*-1),
                                columns=['ONI Index'],
                                index=expected_time)
        with create_tmp_file() as tmp_file:
            lta.to_netcdf(tmp_file)
            actual = index.oni(dataset, 'first', tmp_file)
            self.assertTrue(expected.equals(actual))

    def test_registered(self):
        """
        Test nominal execution of ONI Index calculation, as a registered
        operation.
        """

        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(index.oni))
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': ([datetime(2001, x, 1) for x in range(1, 13)] +
                     [datetime(2002, x, 1) for x in range(1, 13)])})
        lta = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': [x for x in range(1,13)]})
        lta = 2*lta
        expected_time = ([datetime(2001, x, 1) for x in range(2, 13)] +
                         [datetime(2002, x, 1) for x in range(1, 12)])
        expected = pd.DataFrame(data=(np.ones([22])*-1),
                                columns=['ONI Index'],
                                index=expected_time)
        with create_tmp_file() as tmp_file:
            lta.to_netcdf(tmp_file)
            actual = reg_op(ds=dataset, var='first', file=tmp_file)
            self.assertTrue(expected.equals(actual))
