"""
Tests for index operations
"""

from unittest import TestCase

import os
import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime

from cate.ops import index
from cate.ops import subset


def assert_dataset_equal(expected, actual):
    # this method is functionally equivalent to
    # `assert expected == actual`, but it checks each aspect
    # of equality separately for easier debugging
    assert expected.equals(actual), (expected, actual)


class TestIndices(TestCase):
    def test_n34(self):
        """
        Test ENSO index calculation using Nino34 region
        """
        tmp_path = 'temp_lta.nc'

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
        lta.to_netcdf(tmp_path)
        ret = index.enso_nino34(dataset, 'first', tmp_path)
        print(ret)

        try:
            os.remove(tmp_path)
        except OSError:
            # Doesn't exist
            pass

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
