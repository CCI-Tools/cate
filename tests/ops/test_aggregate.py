"""
Tests for aggregation operations
"""
from unittest import TestCase

import numpy as np
import pandas as pd
import xarray as xr

from cate.core.op import OP_REGISTRY
from cate.ops import adjust_temporal_attrs
from cate.ops import long_term_average, temporal_aggregation, reduce
from cate.util.misc import object_to_qualified_name
from cate.util.monitor import ConsoleMonitor


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

    # @unittest.skip("Daily aggregation does do weird things. Skipping for the moment")
    def test_daily(self):
        """
        Test creating a daily LTA dataset
        """
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 730])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 730])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2001-01-01', '2002-12-31')})
        ds = adjust_temporal_attrs(ds)
        actual = long_term_average(ds)

        self.assertEqual(actual.dims, {'dayofyear': 365,
                                       'lat': 45,
                                       'lon': 90})

    def test_general(self):
        """
        Test creating a 'general' LTA dataset
        """
        # Test seasonal
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 5])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 5])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('1999-12-01', freq='QS-DEC', periods=5)})
        ds = adjust_temporal_attrs(ds)
        actual = long_term_average(ds)
        self.assertEqual(actual['first'].attrs['cell_methods'],
                         'time: mean over years')
        self.assertEqual(actual.dims, {'time': 4,
                                       'nv': 2,
                                       'lat': 45,
                                       'lon': 90})
        self.assertEqual(actual.time.attrs['climatology'],
                         'climatology_bounds')

        # Test irregular seasons
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 6])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 6])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', freq='5M', periods=6)})
        ds = adjust_temporal_attrs(ds)
        with self.assertRaises(ValueError) as err:
            long_term_average(ds)
        self.assertIn('inconsistent seasons', str(err.exception))

        # Test 8D
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 137])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 137])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2002-12-31', freq='8D')})
        ds = adjust_temporal_attrs(ds)
        with self.assertRaises(ValueError) as err:
            long_term_average(ds)
        self.assertIn('inconsistent seasons', str(err.exception))

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

        m = ConsoleMonitor()
        actual = temporal_aggregation(ds, monitor=m)

        self.assertTrue(actual.broadcast_equals(ex))

    def test_seasonal(self):
        """
        Test aggregation to a seasonal dataset
        """
        # daily -> seasonal
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31')})
        ds = adjust_temporal_attrs(ds)

        ex = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 5])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 5])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('1999-12-01', freq='QS-DEC', periods=5)})
        m = ConsoleMonitor()
        actual = temporal_aggregation(ds, period='QS-DEC', monitor=m)
        self.assertTrue(actual.broadcast_equals(ex))

        # TODO: uncomment once we have a fix for
        #   cate.core.types.ValidationError: Units 'M', 'Y', and 'y' are no longer supported,
        #   as they do not represent unambiguous timedelta values durations.

        # # monthly -> seasonal
        # ds = xr.Dataset({
        #     'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
        #     'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
        #     'lat': np.linspace(-88, 88, 45),
        #     'lon': np.linspace(-178, 178, 90),
        #     'time': pd.date_range('2000-01-01', freq='MS', periods=12)})
        # ds = adjust_temporal_attrs(ds)
        # actual = temporal_aggregation(ds, output_resolution='season', monitor=m)
        # self.assertTrue(actual.broadcast_equals(ex))

    def test_custom(self):
        """
        Test aggergating to custom temporal resolutions
        """
        # daily -> 8 days
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31')})
        ds = adjust_temporal_attrs(ds)

        ex = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 46])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 46])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31', freq='8D')})
        ex.first.attrs['cell_methods'] = 'time: mean within years'
        ex.second.attrs['cell_methods'] = 'time: mean within years'
        m = ConsoleMonitor()
        actual = temporal_aggregation(ds, period='8D', monitor=m)
        self.assertTrue(actual.broadcast_equals(ex))

        # daily -> weekly
        ex = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 53])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 53])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31', freq='W')})
        ex.first.attrs['cell_methods'] = 'time: mean within years'
        ex.second.attrs['cell_methods'] = 'time: mean within years'
        actual = temporal_aggregation(ds, period='W', monitor=m)
        self.assertTrue(actual.broadcast_equals(ex))

        # TODO: uncomment once we have a fix for
        #   cate.core.types.ValidationError: Units 'M', 'Y', and 'y' are no longer supported,
        #   as they do not represent unambiguous timedelta values durations.

        # # monthly -> 4 month seasons
        # ds = xr.Dataset({
        #     'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
        #     'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
        #     'lat': np.linspace(-88, 88, 45),
        #     'lon': np.linspace(-178, 178, 90),
        #     'time': pd.date_range('2000-01-01', freq='MS', periods=12)})
        # ds = adjust_temporal_attrs(ds)
        # ex = xr.Dataset({
        #     'first': (['lat', 'lon', 'time'], np.ones([45, 90, 4])),
        #     'second': (['lat', 'lon', 'time'], np.ones([45, 90, 4])),
        #     'lat': np.linspace(-88, 88, 45),
        #     'lon': np.linspace(-178, 178, 90),
        #     'time': pd.date_range('2000-01-01', freq='4M', periods=4)})
        # ex.first.attrs['cell_methods'] = 'time: mean within years'
        # ex.second.attrs['cell_methods'] = 'time: mean within years'
        # actual = temporal_aggregation(ds, custom_resolution='4M', monitor=m)
        # self.assertTrue(actual.broadcast_equals(ex))

    def test_8days(self):
        """
        Test nominal execution with a 8 Days frequency input dataset
        """
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 46])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 46])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31', freq='8D')})
        ds = adjust_temporal_attrs(ds)

        ex = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', freq='MS', periods=12)})
        ex.first.attrs['cell_methods'] = 'time: mean within years'
        ex.second.attrs['cell_methods'] = 'time: mean within years'

        m = ConsoleMonitor()
        actual = temporal_aggregation(ds, monitor=m)

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

        ex = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 12])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', freq='MS', periods=12)})

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

        # invalid custom resolution
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31')})
        ds = adjust_temporal_attrs(ds)

        with self.assertRaises(ValueError) as err:
            temporal_aggregation(ds, period='SDFG')
        self.assertEqual('Invalid freq value "SDFG". '
                         'Please check operation documentation.', str(err.exception))


class TestReduce(TestCase):
    """
    Test reduce() operation
    """

    def test_nominal(self):
        """
        Test nominal execution
        """
        ds = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'second': (['lat', 'lon', 'time'], np.ones([45, 90, 366])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31')})
        ds = adjust_temporal_attrs(ds)

        ex = xr.Dataset({
            'first': (['lat', 'lon'], np.ones([45, 90])),
            'second': (['lat', 'lon'], np.ones([45, 90])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31')})

        m = ConsoleMonitor()
        actual = reduce(ds, monitor=m)

        self.assertTrue(actual.broadcast_equals(ex))

        actual = reduce(ds, dim=['lat', 'time', 'there_is_no_spoon'], monitor=m)
        ex = xr.Dataset({
            'first': (['lon'], np.ones([90])),
            'second': (['lon'], np.ones([90])),
            'lat': np.linspace(-88, 88, 45),
            'lon': np.linspace(-178, 178, 90),
            'time': pd.date_range('2000-01-01', '2000-12-31')})

        self.assertTrue(actual.broadcast_equals(ex))
