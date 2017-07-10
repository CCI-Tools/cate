"""
Tests for correlation operations
"""

from unittest import TestCase

import numpy as np
import xarray as xr
from scipy.stats import pearsonr

from cate.ops.correlation import pearson_correlation_simple, pearson_correlation_map


class TestCorrelation(TestCase):
    def test_pearson(self):
        """
        General functionality test
        """
        # Test general functionality 1D dataset variables, test for correctness
        # of correlation
        dataset = xr.Dataset({
            'first': ('time', np.linspace(0, 5, 6))})
        dataset2 = xr.Dataset({
            'first': ('time', np.linspace(0, 5, 6))})
        dataset2['first'][0] = 3

        correlation = pearson_correlation_simple(dataset, dataset2, 'first', 'first')

        test_value = correlation['p_value']
        self.assertTrue(np.isclose(test_value, 0.082086))
        corr_coef = correlation['corr_coef']
        self.assertTrue(np.isclose(corr_coef, 0.755928))

        # Test general functionality 3D dataset variables
        ds1 = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(4, 8),
                                                        np.eye(4, 8),
                                                        np.eye(4, 8)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(4, 8),
                                                         np.eye(4, 8),
                                                         np.eye(4, 8)])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2, 3])})

        ds2 = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(4, 8),
                                                        np.eye(4, 8),
                                                        np.eye(4, 8)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(4, 8),
                                                         np.eye(4, 8),
                                                         np.eye(4, 8)])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2, 3])})

        correlation = pearson_correlation_map(ds1, ds2, 'first', 'first')

    def test_validate_against_scipy(self):
        """
        Test if results in a 3D array correspond to what would be expected
        if scipy.stats.pearsonr was used
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

        correlation = pearson_correlation_map(ds1, ds2, 'first', 'first')
        self.assertTrue(np.all(np.isclose(correlation['corr_coef'].values,
                                          cc_sp)))
        self.assertTrue(np.all(np.isclose(correlation['p_value'].values,
                                          pv_sp)))
