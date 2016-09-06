"""
Tests for plotting operations
"""

from unittest import TestCase
import xarray as xr
import numpy as np
import os

from ect.ops import plot

class TestPlot(TestCase):
    def test_plot_map(self):
        # Test the nominal functionality. This doesn't check that the plot is what is expected,
        # rather, it simply tests if it seems to have been created
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.random.rand(180,360,6)),
            'second': (['lat', 'lon', 'time'], np.random.rand(180,360,6)),
            'lat': np.linspace(-89.5, 89.5, 180),
            'lon': np.linspace(-179.5, 179.5, 360)})

        plot.plot_map(dataset, path='remove_me.png')
        self.assertTrue(os.path.isfile('remove_me.png'))
        os.remove('remove_me.png')

        # Test if an error is raised when an unsupported format is passed
        with self.assertRaises(ValueError):
            plot.plot_map(dataset, path='remove_me.pgm')
        self.assertFalse(os.path.isfile('remove_me.pgm'))

        # Test if extents can be used
        plot.plot_map(dataset, variable='second', extents=[-20., 60., -40., 50.], path='remove_me.pdf')
        self.assertTrue(os.path.isfile('remove_me.pdf'))
        os.remove('remove_me.pdf')

        # Test value error is raised when passing an unexpected dataset type
        with self.assertRaises(NotImplementedError):
            plot.plot_map([1,2,4], extents=[1,2,3,4], path='remove_me.jpeg')
        self.assertFalse(os.path.isfile('remove_me.jpg'))
