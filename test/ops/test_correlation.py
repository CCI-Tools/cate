"""
Tests for correlation operations
"""

from unittest import TestCase
import xarray as xr
import numpy as np
import os

from ect.ops.correlation import pearson_correlation


class TestCorrelation(TestCase):
    def test_pearson(self):
        # Test general functionality
        dataset = xr.Dataset({
            'first': ('time', np.linspace(0,5,6))})
        dataset2 = xr.Dataset({
            'first': ('time', np.linspace(5,0,6))})

        correlation = pearson_correlation(dataset, dataset2, 'first', 'first', file='remove_me.txt')
        self.assertTrue(os.path.isfile('remove_me.txt'))
        os.remove('remove_me.txt')

        test_value = round(float(correlation['test_value']), 2)
        self.assertTrue(test_value == 2.47)
        correlation_coefficient = round(float(correlation['correlation_coefficient']), 2)
        self.assertTrue(correlation_coefficient == 0.78)

        # Test too many data vars
        with self.assertRaises(TypeError):
            dataset = xr.Dataset({
                'first': ('time', np.linspace(0,5,6)),
                'second': ('time', np.linspace(0,5,6))})
            dataset2 = xr.Dataset({
                'first': ('time', np.linspace(5,0,6))})

            correlation = pearson_correlation(dataset, dataset2, file='remove_me.txt')

        self.assertFalse(os.path.isfile('remove_me.txt'))
