"""
Test ect/ops/coregistration.py

This does not test for the validiy of results, rather
that the resampling routines from ect/ops/resample.py are invoked
correctly and yield a result when doing coregistration.
"""

from unittest import TestCase
import numpy as np
import xarray as xr

from ect.ops.coregistration import coregister

class TestCoregistration(TestCase): 
    def test_coregister(self):
        ds_fine = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.zeros([2, 4, 8])),
            'second': (['time', 'lat', 'lon'], np.zeros([2, 4, 8])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1,2])})

        ds_coarse = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.zeros([2, 3, 6])),
            'second': (['time', 'lat', 'lon'], np.zeros([2, 3, 6])),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'time': np.array([1,2])})

        # Test that the coarse dataset has been resampled onto the grid
        # of the finer dataset. Values are not expected to change, as we
        # have zeroes in all matrices.
        ds_coarse_resampled = coregister(ds_fine, ds_coarse)
        self.assertDatasetEqual(ds_coarse_resampled, ds_fine)

        # Test that the fine dataset has been resampled (aggregated)
        # onto the grid of the coarse dataset.
        ds_fine_resampled = coregister(ds_coarse, ds_fine)
        self.assertDatasetEqual(ds_fine_resampled, ds_coarse)

        # Test if non pixel-registered is rejected
        ds_coarse_err = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.zeros([2, 5, 10])),
            'second': (['time', 'lat', 'lon'], np.zeros([2, 5, 10])),
            'lat': np.linspace(-90, 90, 5),
            'lon': np.linspace(-162, 162, 10),
            'time': np.array([1,2])})

        with self.assertRaises(ValueError):
            coregister(ds_fine, ds_coarse_err)

        ds_coarse_err = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.zeros([2, 5, 10])),
            'second': (['time', 'lat', 'lon'], np.zeros([2, 5, 10])),
            'lat': np.linspace(-72, 72, 5),
            'lon': np.linspace(-180, 180, 10),
            'time': np.array([1,2])})

        with self.assertRaises(ValueError):
            coregister(ds_fine, ds_coarse_err)


    def assertDatasetEqual(self, expected, actual):
        # this method is functionally equivalent to `assert expected == actual`, but it
        # checks each aspect of equality separately for easier debugging
        assert expected.equals(actual), (expected, actual)

