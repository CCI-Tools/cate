"""
Test cate/ops/coregistration.py

Test coregistration, checks if the values seem as expected
when using default upsampling/downsampling methods.

"""

from unittest import TestCase

import numpy as np
import xarray as xr
from numpy.testing import assert_almost_equal

from cate.ops.coregistration import coregister


class TestCoregistration(TestCase):
    def test_coregister(self):
        ds_fine = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(4, 8), np.eye(4, 8)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(4, 8), np.eye(4, 8)])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2])})

        ds_coarse = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([np.eye(3, 6), np.eye(3, 6)])),
            'second': (['time', 'lat', 'lon'], np.array([np.eye(3, 6), np.eye(3, 6)])),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'time': np.array([1, 2])})

        # Test that the coarse dataset has been resampled onto the grid
        # of the finer dataset. Values are not expected to change, as we
        # have zeroes in all matrices.
        ds_coarse_resampled = coregister(ds_fine, ds_coarse)
        expected = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([[[1., 0.28571429, 0., 0., 0., 0., 0., 0.],
                                                         [0.33333333, 0.57142857, 0.38095238, 0., 0., 0., 0., 0.],
                                                         [0., 0.47619048, 0.52380952, 0.28571429, 0.04761905, 0., 0.,
                                                          0.],
                                                         [0., 0., 0.42857143, 0.85714286, 0.14285714, 0., 0., 0.]],
                                                        [[1., 0.28571429, 0., 0., 0., 0., 0., 0.],
                                                         [0.33333333, 0.57142857, 0.38095238, 0., 0., 0., 0., 0.],
                                                         [0., 0.47619048, 0.52380952, 0.28571429, 0.04761905, 0., 0.,
                                                          0.],
                                                         [0., 0., 0.42857143, 0.85714286, 0.14285714, 0., 0., 0.]]])),
            'second': (['time', 'lat', 'lon'], np.array([[[1., 0.28571429, 0., 0., 0., 0., 0., 0.],
                                                          [0.33333333, 0.57142857, 0.38095238, 0., 0., 0., 0., 0.],
                                                          [0., 0.47619048, 0.52380952, 0.28571429, 0.04761905, 0., 0.,
                                                           0.],
                                                          [0., 0., 0.42857143, 0.85714286, 0.14285714, 0., 0., 0.]],
                                                         [[1., 0.28571429, 0., 0., 0., 0., 0., 0.],
                                                          [0.33333333, 0.57142857, 0.38095238, 0., 0., 0., 0., 0.],
                                                          [0., 0.47619048, 0.52380952, 0.28571429, 0.04761905, 0., 0.,
                                                           0.],
                                                          [0., 0., 0.42857143, 0.85714286, 0.14285714, 0., 0., 0.]]])),
            'lat': np.linspace(-67.5, 67.5, 4),
            'lon': np.linspace(-157.5, 157.5, 8),
            'time': np.array([1, 2])})
        assert_almost_equal(ds_coarse_resampled['first'].values, expected['first'].values)

        # Test that the fine dataset has been resampled (aggregated)
        # onto the grid of the coarse dataset.
        ds_fine_resampled = coregister(ds_coarse, ds_fine)
        expected = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.array([[[0.625, 0.125, 0., 0., 0., 0.],
                                                         [0.125, 0.5, 0.125, 0., 0., 0.],
                                                         [0., 0.125, 0.625, 0., 0., 0.]],

                                                        [[0.625, 0.125, 0., 0., 0., 0.],
                                                         [0.125, 0.5, 0.125, 0., 0., 0.],
                                                         [0., 0.125, 0.625, 0., 0., 0.]]])),
            'second': (['time', 'lat', 'lon'], np.array([[[0.625, 0.125, 0., 0., 0., 0.],
                                                          [0.125, 0.5, 0.125, 0., 0., 0.],
                                                          [0., 0.125, 0.625, 0., 0., 0.]],

                                                         [[0.625, 0.125, 0., 0., 0., 0.],
                                                          [0.125, 0.5, 0.125, 0., 0., 0.],
                                                          [0., 0.125, 0.625, 0., 0., 0.]]])),
            'lat': np.linspace(-60, 60, 3),
            'lon': np.linspace(-150, 150, 6),
            'time': np.array([1, 2])})

        assert_almost_equal(ds_fine_resampled['first'].values, expected['first'].values)

        # Test if non pixel-registered is rejected
        ds_coarse_err = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.zeros([2, 5, 10])),
            'second': (['time', 'lat', 'lon'], np.zeros([2, 5, 10])),
            'lat': np.linspace(-90, 90, 5),
            'lon': np.linspace(-162, 162, 10),
            'time': np.array([1, 2])})

        with self.assertRaises(ValueError):
            coregister(ds_fine, ds_coarse_err)

        ds_coarse_err = xr.Dataset({
            'first': (['time', 'lat', 'lon'], np.zeros([2, 5, 10])),
            'second': (['time', 'lat', 'lon'], np.zeros([2, 5, 10])),
            'lat': np.linspace(-72, 72, 5),
            'lon': np.linspace(-180, 180, 10),
            'time': np.array([1, 2])})

        with self.assertRaises(ValueError):
            coregister(ds_fine, ds_coarse_err)
