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
        pass

    def test_antimeridian(self):
        pass

    def test_custom_region(self):
        pass

    def test_registered(self):
        pass


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
        actual = subset.subset_spatial(dataset, "-20, -10, 20, 10")
        expected = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([5, 10, 24])),
            'second': (['lat', 'lon', 'time'], np.ones([5, 10, 24])),
            'lat': np.linspace(-8, 8, 5),
            'lon': np.linspace(-18, 18, 10),
            'time': ([datetime(2001, x, 1) for x in range(1, 13)] +
                     [datetime(2002, x, 1) for x in range(1, 13)])})
        assert_dataset_equal(expected, actual)
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

    def test_registered(self):
        """
        Test nominal execution of ONI Index calculation, as a registered
        operation.
        """
        pass
