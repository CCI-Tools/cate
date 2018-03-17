"""
Tests for animation operations
"""
import itertools
import os
import sys
import shutil
import tempfile
from contextlib import contextmanager
from unittest import TestCase
import unittest

import xarray as xr
import numpy as np
import pandas as pd

from cate.ops.animate import animate_map

_counter = itertools.count()
ON_WIN = sys.platform == 'win32'


@contextmanager
def create_tmp_file(name, ext):
    """
    Create a temporary filename for testing
    """
    tmp_dir = tempfile.mkdtemp()
    path = os.path.join(tmp_dir, '{}{}.{}'.format(name,
                                                  next(_counter),
                                                  ext))
    try:
        yield path
    finally:
        try:
            shutil.rmtree(tmp_dir)
        except OSError:
            if not ON_WIN:
                raise


@unittest.skipIf(condition=os.environ.get('CATE_DISABLE_PLOT_TESTS', None),
                 reason="skipped if CATE_DISABLE_PLOT_TESTS=1")
class TestAnimateMap(TestCase):
    """
    Test animate_map() operation
    """

    def test_animate_map(self):
        # Test the nominal functionality. This doesn't check that the animation is what is expected,
        # rather, it simply tests if it seems to have been created
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.random.rand(5, 10, 2)),
            'second': (['lat', 'lon', 'time'], np.random.rand(5, 10, 2)),
            'lat': np.linspace(-89.5, 89.5, 5),
            'lon': np.linspace(-179.5, 179.5, 10),
            'time': pd.date_range('2000-01-01', periods=2)})

        with create_tmp_file('remove_me', 'html') as tmp_file:
            animate_map(dataset, file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

        # Test if extents can be used
        with create_tmp_file('remove_me', 'html') as tmp_file:
            animate_map(dataset,
                        var='second',
                        region='-40.0, -20.0, 50.0, 60.0',
                        file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

        # Test setting a Title
        with create_tmp_file('remove_me', 'html') as tmp_file:
            animate_map(dataset,
                        var='second',
                        title='Title',
                        file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

        # Test using a global color map
        with create_tmp_file('remove_me', 'html') as tmp_file:
            animate_map(dataset,
                        var='second',
                        true_range=True,
                        file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

    def test_plot_map_exceptions(self):
        # Test if the corner cases are detected without creating a plot for it.

        # Test value error is raised when passing an unexpected dataset type
        with create_tmp_file('remove_me', 'html') as tmp_file:
            with self.assertRaises(ValueError) as cm:
                animate_map([1, 2, 4], file=tmp_file)
            self.assertEqual(str(cm.exception),
                             "input 'ds' for operation 'cate.ops.animate.animate_map' "
                             "must be of type 'Dataset', but got type 'list'")
            self.assertFalse(os.path.isfile(tmp_file))

        # Test the extensions bound checking
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.random.rand(2, 4, 1)),
            'lat': np.linspace(-89.5, 89.5, 2),
            'lon': np.linspace(-179.5, 179.5, 4)})

        with self.assertRaises(ValueError):
            region = '-40.0, -95.0, 50.0, 60.0',
            animate_map(dataset, region=region)

        with self.assertRaises(ValueError):
            region = '-40.0, -20.0, 50.0, 95.0',
            animate_map(dataset, region=region)

        with self.assertRaises(ValueError):
            region = '-181.0, -20.0, 50.0, 60.0',
            animate_map(dataset, region=region)

        with self.assertRaises(ValueError):
            region = '-40.0, -20.0, 181.0, 60.0',
            animate_map(dataset, region=region)

        with self.assertRaises(ValueError):
            region = '-40.0, -20.0, 50.0, -25.0',
            animate_map(dataset, region=region)

        with self.assertRaises(ValueError):
            region = '-20.0, -20.0, -25.0, 60.0',
            animate_map(dataset, region=region)

        # Test all-nan dataset
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.full([5, 10, 2], np.nan)),
            'second': (['lat', 'lon', 'time'], np.full([5, 10, 2], np.nan)),
            'lat': np.linspace(-89.5, 89.5, 5),
            'lon': np.linspace(-179.5, 179.5, 10),
            'time': pd.date_range('2000-01-01', periods=2)})

        with create_tmp_file('remove_me', 'html') as tmp_file:
            with self.assertRaises(ValueError):
                animate_map(dataset,
                            var='second',
                            true_range=True,
                            file=tmp_file)
