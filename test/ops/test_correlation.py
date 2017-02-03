"""
Tests for correlation operations
"""

import os
from unittest import TestCase

import numpy as np
import xarray as xr

from cate.ops.correlation import pearson_correlation


class TestCorrelation(TestCase):
    def test_pearson(self):
        # Test general functionality 1D dataset variables, test for correctness
        # of correlation
        dataset = xr.Dataset({
            'first': ('time', np.linspace(0, 5, 6))})
        dataset2 = xr.Dataset({
            'first': ('time', np.linspace(5, 0, 6, ))})

        correlation = pearson_correlation(dataset, dataset2, 'first', 'first',
                                          file='remove_me.txt')
        self.assertTrue(os.path.isfile('remove_me.txt'))
        os.remove('remove_me.txt')

        test_value = correlation['p_value']
        self.assertTrue(test_value == 0)
        corr_coef = correlation['corr_coef']
        self.assertTrue(corr_coef == -1)

        # Test general functionality 3D dataset variables
        ds1 = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(4, 8),
                                                        np.eye(4, 8)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(4, 8),
                                                         np.eye(4, 8)])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2])})

        ds2 = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(4, 8),
                                                        np.eye(4, 8)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(4, 8),
                                                         np.eye(4, 8)])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2])})

        correlation = pearson_correlation(ds1, ds2, 'first', 'first',
                                          file='remove_me.txt')
        self.assertTrue(os.path.isfile('remove_me.txt'))
        os.remove('remove_me.txt')
