"""
Tests for plotting operations
"""

import os
import unittest
from unittest import TestCase

import numpy as np
import xarray as xr

from cate.ops import plot


@unittest.skipIf(condition=os.environ.get('CATE_DISABLE_PLOT_TESTS', None),
                 reason="skipped if CATE_DISABLE_PLOT_TESTS=1")
class TestPlot(TestCase):
    def test_plot_map(self):
        # Test the nominal functionality. This doesn't check that the plot is what is expected,
        # rather, it simply tests if it seems to have been created
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.random.rand(5, 10, 2)),
            'second': (['lat', 'lon', 'time'], np.random.rand(5, 10, 2)),
            'lat': np.linspace(-89.5, 89.5, 5),
            'lon': np.linspace(-179.5, 179.5, 10)})

        plot.plot_map(dataset, file='remove_me.png')
        self.assertTrue(os.path.isfile('remove_me.png'))
        os.remove('remove_me.png')

        # Test if an error is raised when an unsupported format is passed
        with self.assertRaises(ValueError):
            plot.plot_map(dataset, file='remove_me.pgm')
        self.assertFalse(os.path.isfile('remove_me.pgm'))

        # Test if extents can be used
        plot.plot_map(dataset,
                      var='second',
                      lat_min=-20.0,
                      lat_max=60.0,
                      lon_min=-40.0,
                      lon_max=50.0,
                      file='remove_me.pdf')
        self.assertTrue(os.path.isfile('remove_me.pdf'))
        os.remove('remove_me.pdf')

    def test_plot_map_exceptions(self):
        # Test if the corner cases are detected without creating a plot for it.

        # Test value error is raised when passing an unexpected dataset type
        with self.assertRaises(NotImplementedError):
            plot.plot_map([1, 2, 4], file='remove_me.jpeg')
        self.assertFalse(os.path.isfile('remove_me.jpg'))

        # Test the extensions bound checking
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.random.rand(2, 4, 1)),
            'lat': np.linspace(-89.5, 89.5, 2),
            'lon': np.linspace(-179.5, 179.5, 4)})

        with self.assertRaises(ValueError):
            plot.plot_map(dataset, lat_min=-95.0)

        with self.assertRaises(ValueError):
            plot.plot_map(dataset, lat_max=95.0)

        with self.assertRaises(ValueError):
            plot.plot_map(dataset, lon_min=-181.0)

        with self.assertRaises(ValueError):
            plot.plot_map(dataset, lon_max=181.0)

        with self.assertRaises(ValueError):
            plot.plot_map(dataset, lat_min=-20.0, lat_max=-25.0)

        with self.assertRaises(ValueError):
            plot.plot_map(dataset, lon_min=-20.0, lon_max=-25.0)

    def test_plot_1D(self):
        # Test plot_1D
        dataset = xr.Dataset({
            'first': np.random.rand(10)})

        plot.plot_1D(dataset, 'first', file='remove_me.jpg')
        self.assertTrue(os.path.isfile('remove_me.jpg'))
        os.remove('remove_me.jpg')
