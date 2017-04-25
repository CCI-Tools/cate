"""
Tests for aggregation operations
"""

from unittest import TestCase

import xarray as xr
import pandas as pd
import numpy as np

from cate.core.op import OP_REGISTRY
from cate.util.misc import object_to_qualified_name
from cate.util.monitor import ConsoleMonitor

from cate.ops import long_term_average, temporal_aggregation


def assert_dataset_equal(expected, actual):
    # this method is functionally equivalent to
    # `assert expected == actual`, but it checks each aspect
    # of equality separately for easier debugging
    assert expected.equals(actual), (expected, actual)


class TestLTA(TestCase):
    """
    Test long term averaging
    """
    def test_nominal(self):
        """
        Test nominal execution
        """
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', freq='MS', periods=24)})

        m = ConsoleMonitor()
        actual = long_term_average(ds, monitor=m)
        print(actual)

    def test_registered(self):
        """
        Test registered operation execution
        """
        pass

    def test_validation(self):
        """
        Test input validation
        """
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        with self.assertRaises(ValueError) as err:
            long_term_average(ds)
        self.assertIn('harmonization', str(err.exception))

        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', periods=24)})

        with self.assertRaises(ValueError) as err:
            long_term_average(ds)
        self.assertIn('temporal aggregation', str(err.exception))


class TestTemporalAggregation(TestCase):
    """
    Test temporal aggregation
    """
    def test_nominal(self):
        """
        Test nominal exeuction
        """
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', periods=24)})

        actual = temporal_aggregation(ds)
        print(actual)

    def test_registered(self):
        """
        Test registered operation execution
        """
        pass
