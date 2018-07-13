"""
Tests for correlation operations
"""

from unittest import TestCase

import numpy as np
import xarray as xr
import pandas as pd
from scipy.stats import pearsonr

from cate.ops.correlation import pearson_correlation_scalar, pearson_correlation
from ..util.test_monitor import RecordingMonitor


class TestPearsonScalar(TestCase):
    def test_nominal(self):
        """
        Test nominal run
        """
        dataset = xr.Dataset({
            'first': ('time', np.linspace(0, 5, 6))})
        dataset2 = xr.Dataset({
            'first': ('time', np.linspace(0, 5, 6))})
        dataset2['first'][0] = 3

        correlation = pearson_correlation_scalar(dataset, dataset2, 'first', 'first')

        test_value = correlation['p_value']
        self.assertTrue(np.isclose(test_value, 0.082086))
        corr_coef = correlation['corr_coef']
        self.assertTrue(np.isclose(corr_coef, 0.755928))

    def test_polymorphism(self):
        """
        Nominal run with an xr.Dataset and a pd.DataFrame
        """
        ds = xr.Dataset({'first': ('time', np.linspace(0, 5, 6))})
        df = pd.DataFrame({'first': np.linspace(0, 5, 6),
                           'time': np.linspace(0, 5, 6)})
        df.index = df['time']
        df['first'][0] = 3

        correlation = pearson_correlation_scalar(ds, df, 'first', 'first')

        test_value = correlation['p_value']
        self.assertTrue(np.isclose(test_value, 0.082086))
        corr_coef = correlation['corr_coef']
        self.assertTrue(np.isclose(corr_coef, 0.755928))

    def test_error(self):
        """
        Test error conditions
        """
        # Test incompatible time dimension
        ds1 = xr.Dataset({'first': ('time', np.linspace(0, 5, 6))})
        ds2 = xr.Dataset({'first': ('time', np.linspace(0, 1, 2))})

        with self.assertRaises(ValueError) as err:
            pearson_correlation_scalar(ds1, ds2, 'first', 'first')
        self.assertIn('dimension differs', str(err.exception))

        # Test incompatible time dimension
        ds1 = xr.Dataset({'first': ('time', np.linspace(0, 1, 2))})
        ds2 = xr.Dataset({'first': ('time', np.linspace(0, 1, 2))})

        with self.assertRaises(ValueError) as err:
            pearson_correlation_scalar(ds1, ds2, 'first', 'first')
        self.assertIn('dimension should not be less', str(err.exception))


class TestPearson(TestCase):
    def test_nominal(self):
        """
        Test nominal run
        """
        # Test general functionality 3D dataset variables
        ds1 = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.ones([4, 8]),
                                                        np.ones([4, 8]) * 2,
                                                        np.ones([4, 8]) * 3])),
            'second': (['time', 'lat', 'lon'], np.array([np.ones([4, 8]) * 2,
                                                         np.ones([4, 8]) * 3,
                                                         np.ones([4, 8])])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2, 3])}).chunk(chunks={'lat': 2, 'lon': 4})

        ds2 = xr.Dataset({
            'second': (['time', 'lat', 'lon'], np.array([np.ones([4, 8]),
                                                         np.ones([4, 8]) * 2,
                                                         np.ones([4, 8]) * 3])),
            'first': (['time', 'lat', 'lon'], np.array([np.ones([4, 8]) * 2,
                                                        np.ones([4, 8]) * 3,
                                                        np.ones([4, 8])])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2, 3])}).chunk(chunks={'lat': 2, 'lon': 4})

        rm = RecordingMonitor()
        corr = pearson_correlation(ds1, ds2, 'first', 'first', monitor=rm)
        self.assertTrue(len(rm.records) > 0)

        self.assertTrue(corr['corr_coef'].max() == corr['corr_coef'].min())
        self.assertTrue(corr['corr_coef'].max() == -0.5)

        self.assertTrue(corr['p_value'].max() == corr['p_value'].min())
        self.assertTrue(np.isclose(corr['p_value'].min(), 0.6666666))

    def test_validate_against_scipy(self):
        """
        Validate the result against scipy implementation
        """
        x = np.linspace(0, 5, 6)
        y = np.linspace(0, 5, 6)
        y[0] = 3

        cc_sp, pv_sp = pearsonr(x, y)

        x_3d = np.empty((4, 8, 6))
        y_3d = np.empty((4, 8, 6))
        x_3d[:] = x
        y_3d[:] = y

        ds1 = xr.Dataset({
            'first': (['lat', 'lon', 'time'], x_3d),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.linspace(0, 5, 6)})

        ds2 = xr.Dataset({
            'first': (['lat', 'lon', 'time'], y_3d),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.linspace(0, 5, 6)})

        correlation = pearson_correlation(ds1, ds2, 'first', 'first')
        self.assertTrue(np.all(np.isclose(correlation['corr_coef'].values,
                                          cc_sp)))
        self.assertTrue(np.all(np.isclose(correlation['p_value'].values,
                                          pv_sp)))

        # Test non-overlapping times
        ds1 = xr.Dataset({
            'first': (['lat', 'lon', 'time'], x_3d),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.linspace(0, 5, 6)})

        ds2 = xr.Dataset({
            'first': (['lat', 'lon', 'time'], y_3d),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.linspace(6, 11, 6)})

        correlation = pearson_correlation(ds1, ds2, 'first', 'first')
        self.assertTrue(np.all(np.isclose(correlation['corr_coef'].values,
                                          cc_sp)))
        self.assertTrue(np.all(np.isclose(correlation['p_value'].values,
                                          pv_sp)))

    def test_broadcasting(self):
        """
        Test a (3d, 1d) input pair
        """
        x = np.linspace(0, 5, 6)
        y = np.linspace(0, 5, 6)
        y[0] = 3

        cc_sp, pv_sp = pearsonr(x, y)

        x_3d = np.empty((4, 8, 6))
        x_3d[:] = x

        ds1 = xr.Dataset({
            'first': (['lat', 'lon', 'time'], x_3d),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.linspace(0, 5, 6)})

        ds2 = xr.Dataset({
            'first': (['time'], y),
            'time': np.linspace(0, 5, 6)})

        correlation = pearson_correlation(ds1, ds2, 'first', 'first')
        self.assertTrue(np.all(np.isclose(correlation['corr_coef'].values,
                                          cc_sp)))
        self.assertTrue(np.all(np.isclose(correlation['p_value'].values,
                                          pv_sp)))

    def test_polymorphism(self):
        """
        Broadcasting run where the 1d input is a pandas dataframe
        """
        x = np.linspace(0, 5, 6)
        y = np.linspace(0, 5, 6)
        y[0] = 3

        cc_sp, pv_sp = pearsonr(x, y)

        x_3d = np.empty((4, 8, 6))
        x_3d[:] = x

        ds1 = xr.Dataset({
            'first': (['lat', 'lon', 'time'], x_3d),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.linspace(0, 5, 6)})

        df = pd.DataFrame({'first': y, 'time': np.linspace(0, 5, 6)})
        df.index = df['time']

        correlation = pearson_correlation(ds1, df, 'first', 'first')
        self.assertTrue(np.all(np.isclose(correlation['corr_coef'].values,
                                          cc_sp)))
        self.assertTrue(np.all(np.isclose(correlation['p_value'].values,
                                          pv_sp)))

    def test_error(self):
        """
        Test error conditions
        """
        # Test incompatible dimensions
        ds1 = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.ones([3, 4, 8])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2, 3])})

        ds2 = xr.Dataset({
            'second': (['time', 'lat', 'lon', 'f'], np.ones([3, 4, 8, 2])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2, 3]),
            'f': np.array([1, 2])})

        with self.assertRaises(ValueError) as err:
            pearson_correlation(ds1, ds2, 'first', 'second')
        self.assertIn('dimensionality', str(err.exception))

        with self.assertRaises(ValueError) as err:
            pearson_correlation(ds2, ds2, 'second', 'second')
        self.assertIn('dimensionality', str(err.exception))

        # Test incompatible shape
        ds1 = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.ones([3, 4, 8])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2, 3])})

        ds2 = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.ones([4, 4, 8])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2, 3, 4])})

        with self.assertRaises(ValueError) as err:
            pearson_correlation(ds1, ds2, 'first', 'first')
        self.assertIn('shape', str(err.exception))

        # Test incompatible lon/lat
        ds1 = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.ones([3, 4, 8])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2, 3])})

        ds2 = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.ones([3, 4, 8])),
            'lat': np.linspace(0, 3, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2, 3])})

        with self.assertRaises(ValueError) as err:
            pearson_correlation(ds1, ds2, 'first', 'first')
        self.assertIn('lat/lon definition', str(err.exception))

        # Test incompatible time dimension
        ds1 = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.ones([3, 4, 8])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2, 3])})

        ds2 = xr.Dataset({
            'first': (['time'], np.ones([4])),
            'time': np.array([1, 2, 3, 4])})

        with self.assertRaises(ValueError) as err:
            pearson_correlation(ds1, ds2, 'first', 'first')
        self.assertIn('dimension differs', str(err.exception))

        # Test incompatible time dimension
        ds1 = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.ones([2, 4, 8])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2])})

        ds2 = xr.Dataset({
            'first': (['time'], np.ones([2])),
            'time': np.array([1, 2])})

        with self.assertRaises(ValueError) as err:
            pearson_correlation(ds1, ds2, 'first', 'first')
        self.assertIn('dimension should not be less', str(err.exception))
