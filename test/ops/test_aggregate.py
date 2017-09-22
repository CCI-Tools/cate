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
from cate.ops import adjust_temporal_attrs


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

        ds = adjust_temporal_attrs(ds)

        # Test monitor
        m = ConsoleMonitor()
        actual = long_term_average(ds, monitor=m)
        self.assertEqual(m._percentage, 100)

        # Test CF attributes
        self.assertEqual(actual['first'].attrs['cell_methods'],
                         'time: mean over years')
        self.assertEqual(actual.dims, {'time': 12,
                                       'nv': 2,
                                       'lat': 45,
                                       'lon': 90})
        self.assertEqual(actual.time.attrs['climatology'],
                         'climatology_bounds')

        # Test variable selection
        actual = long_term_average(ds, var='first')
        with self.assertRaises(KeyError):
            actual['second']

    def test_registered(self):
        """
        Test registered operation execution
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(long_term_average))
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', freq='MS', periods=24)})

        ds = adjust_temporal_attrs(ds)

        reg_op(ds=ds)

    def test_validation(self):
        """
        Test input validation
        """
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})

        ds = adjust_temporal_attrs(ds)

        with self.assertRaises(ValueError) as err:
            long_term_average(ds)
        self.assertIn('normalize', str(err.exception))

        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', periods=24)})

        ds = adjust_temporal_attrs(ds)

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
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31')})
        ds = adjust_temporal_attrs(ds)

        ex = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', freq='MS', periods=12)})
        ex.first.attrs['cell_methods'] = 'time: mean within years'
        ex.second.attrs['cell_methods'] = 'time: mean within years'

        actual = temporal_aggregation(ds)
        self.assertTrue(actual.broadcast_equals(ex))

    def test_registered(self):
        """
        Test registered operation execution
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(temporal_aggregation))
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31')})
        ds = adjust_temporal_attrs(ds)

        ex = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', freq='MS', periods=12)})
        ex.first.attrs['cell_methods'] = 'time: mean within years'
        ex.second.attrs['cell_methods'] = 'time: mean within years'

        actual = reg_op(ds=ds)
        self.assertTrue(actual.broadcast_equals(ex))

    def test_validation(self):
        """
        Test input validation
        """
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90)})
        ds = adjust_temporal_attrs(ds)

        with self.assertRaises(ValueError) as err:
            temporal_aggregation(ds)
        self.assertIn('normalize', str(err.exception))

        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 24])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', freq='MS', periods=24)})
        ds = adjust_temporal_attrs(ds)

        with self.assertRaises(ValueError) as err:
            temporal_aggregation(ds)
        self.assertIn('daily dataset', str(err.exception))
