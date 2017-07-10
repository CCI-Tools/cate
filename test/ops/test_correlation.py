"""
Tests for correlation operations
"""

from unittest import TestCase

import numpy as np
import xarray as xr
from scipy.stats import pearsonr

from cate.ops.correlation import pearson_correlation_simple, pearson_correlation_map


class TestPearsonSimple(TestCase):
    def test_nominal(self):
        """
        Test nominal run
        """
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

    def test_registered(self):
        """
        Nominal run using the operation through OP_REGISTRY
        """
        pass

    def test_polymorphism(self):
        """
        Nominal run with an xr.Dataset and a pd.DataFrame
        """
        pass

    def test_2d(self):
        """
        Nominal run with a 2d object
        """
        pass

    def test_3d(self):
        """
        Nominal run with a 3d object
        """
        pass

    def test_error(self):
        """
        Test error conditions
        """
        pass


class TestPearsonMap(TestCase):
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
            'time': np.array([1, 2, 3])})

        ds2 = xr.Dataset({
            'second': (['time', 'lat', 'lon'], np.array([np.ones([4, 8]),
                                                         np.ones([4, 8]) * 2,
                                                         np.ones([4, 8]) * 3])),
            'first': (['time', 'lat', 'lon'], np.array([np.ones([4, 8]) * 2,
                                                        np.ones([4, 8]) * 3,
                                                        np.ones([4, 8])])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2, 3])})

        corr = pearson_correlation_map(ds1, ds2, 'first', 'first')

        self.assertTrue(corr['corr_coef'].max() == corr['corr_coef'].min())
        self.assertTrue(corr['corr_coef'].max() == -0.5)

        self.assertTrue(corr['p_value'].max() == corr['p_value'].min())
        self.assertTrue(np.isclose(corr['p_value'].min(), 0.6666666))

    def test_registered(self):
        """
        Nominal run using the operation through OP_REGISTRY
        """
        pass

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

        correlation = pearson_correlation_map(ds1, ds2, 'first', 'first')
        self.assertTrue(np.all(np.isclose(correlation['corr_coef'].values,
                                          cc_sp)))
        self.assertTrue(np.all(np.isclose(correlation['p_value'].values,
                                          pv_sp)))

    def test_broadcasting(self):
        """
        Test a (3d, 1d) input pair
        """
        pass

    def test_polymorphism(self):
        """
        Broadcasting run where the 1d input is a pandas dataframe
        """
        pass

    def test_error(self):
        """
        Test error conditions
        """
        pass
