"""
Tests for index operations
"""

from unittest import TestCase

import os
import sys
import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime
import tempfile
import shutil
from contextlib import contextmanager
import itertools

from cate.ops import index
from cate.ops import subset


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


class TestIndices(TestCase):
    def test_n34(self):
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
        with create_tmp_file() as tmp_file:
            lta.to_netcdf(tmp_file)
            ret = index.enso_nino34(dataset, 'first', tmp_file)
            print(ret)

    def test_preset_region(self):
        """
        Test ENSO index calculation using a pre-defined region
        """
        pass

    def test_custom(self):
        """
        Test ENSO index calculation using a user-supplied region
        """
        pass

    def test_oni(self):
        """
        Test ONI index calculation.
        """
        pass
