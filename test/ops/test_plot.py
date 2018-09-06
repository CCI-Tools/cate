"""
Tests for plotting operations
"""

import itertools
import os
import shutil
import sys
import tempfile
import unittest
from contextlib import contextmanager
from unittest import TestCase

import numpy as np
import pandas as pd
import xarray as xr

from cate.core.op import OP_REGISTRY
from cate.ops.plot import plot, plot_line, plot_map, plot_data_frame, plot_hovmoeller
from cate.util.misc import object_to_qualified_name

_counter = itertools.count()
ON_WIN = sys.platform == 'win32'


@contextmanager
def create_tmp_file(name, ext):
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
class TestPlotMap(TestCase):
    """
    Test plot_map() function
    """

    def test_plot_map(self):
        # Test the nominal functionality. This doesn't check that the plot is what is expected,
        # rather, it simply tests if it seems to have been created
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.random.rand(5, 10, 2)),
            'second': (['lat', 'lon', 'time'], np.random.rand(5, 10, 2)),
            'lat': np.linspace(-89.5, 89.5, 5),
            'lon': np.linspace(-179.5, 179.5, 10),
            'time': pd.date_range('2000-01-01', periods=2)})

        with create_tmp_file('remove_me', 'png') as tmp_file:
            plot_map(dataset, file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

        # Test if an error is raised when an unsupported format is passed
        with create_tmp_file('remove_me', 'pgm') as tmp_file:
            with self.assertRaises(ValueError):
                plot_map(dataset, file=tmp_file)
            self.assertFalse(os.path.isfile(tmp_file))

        # Test if extents can be used
        with create_tmp_file('remove_me', 'pdf') as tmp_file:
            plot_map(dataset,
                     var='second',
                     region='-40.0, -20.0, 50.0, 60.0',
                     file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

        # Test time slice selection
        with create_tmp_file('remove_me', 'png') as tmp_file:
            plot_map(dataset, time='2000-01-01', file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

    def test_plot_map_exceptions(self):
        # Test if the corner cases are detected without creating a plot for it.

        # Test value error is raised when passing an unexpected dataset type
        with create_tmp_file('remove_me', 'jpeg') as tmp_file:
            with self.assertRaises(ValueError) as cm:
                plot_map([1, 2, 4], file=tmp_file)
            self.assertEqual(str(cm.exception),
                             "Input 'ds' for operation 'cate.ops.plot.plot_map' "
                             "must be of type 'Dataset', but got type 'list'.")
            self.assertFalse(os.path.isfile(tmp_file))

        # Test the extensions bound checking
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.random.rand(2, 4, 1)),
            'lat': np.linspace(-89.5, 89.5, 2),
            'lon': np.linspace(-179.5, 179.5, 4)})

        with self.assertRaises(ValueError):
            region = '-40.0, -95.0, 50.0, 60.0',
            plot_map(dataset, region=region)

        with self.assertRaises(ValueError):
            region = '-40.0, -20.0, 50.0, 95.0',
            plot_map(dataset, region=region)

        with self.assertRaises(ValueError):
            region = '-181.0, -20.0, 50.0, 60.0',
            plot_map(dataset, region=region)

        with self.assertRaises(ValueError):
            region = '-40.0, -20.0, 181.0, 60.0',
            plot_map(dataset, region=region)

        with self.assertRaises(ValueError):
            region = '-40.0, -20.0, 50.0, -25.0',
            plot_map(dataset, region=region)

        with self.assertRaises(ValueError):
            region = '-20.0, -20.0, -25.0, 60.0',
            plot_map(dataset, region=region)

        # Test temporal slice validation
        with self.assertRaises(ValueError):
            plot_map(dataset, time=0)

    def test_registered(self):
        """
        Test nominal execution of the function as a registered operation.
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(plot_map))
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.random.rand(5, 10, 2)),
            'second': (['lat', 'lon', 'time'], np.random.rand(5, 10, 2)),
            'lat': np.linspace(-89.5, 89.5, 5),
            'lon': np.linspace(-179.5, 179.5, 10),
            'time': pd.date_range('2000-01-01', periods=2)})

        with create_tmp_file('remove_me', 'png') as tmp_file:
            reg_op(ds=dataset, file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))


@unittest.skipIf(condition=os.environ.get('CATE_DISABLE_PLOT_TESTS', None),
                 reason="skipped if CATE_DISABLE_PLOT_TESTS=1")
class TestPlot(TestCase):
    """
    Test plot() function
    """

    def test_plot(self):
        # Test plot
        dataset = xr.Dataset({
            'first': np.random.rand(10)})

        with create_tmp_file('remove_me', 'jpg') as tmp_file:
            plot(dataset, 'first', file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

    def test_registered(self):
        """
        Test nominal execution of the function as a registered operation.
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(plot))
        # Test plot
        dataset = xr.Dataset({
            'first': np.random.rand(10)})

        with create_tmp_file('remove_me', 'jpg') as tmp_file:
            reg_op(ds=dataset, var='first', file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))


@unittest.skipIf(condition=os.environ.get('CATE_DISABLE_PLOT_TESTS', None),
                 reason="skipped if CATE_DISABLE_PLOT_TESTS=1")
class TestPlotLine(TestCase):
    """
    Test plot_line() function
    """

    def test_plot(self):
        # Test plot_line
        single_dim_ds = xr.Dataset({
            'first': (['time'], np.random.rand(10)),
            'second': (['time'], np.random.rand(10)),
            'time': pd.date_range('2000-01-01', periods=10)})

        # Test with only 1 variable selected
        with create_tmp_file('remove_me', 'jpg') as tmp_file:
            plot_line(single_dim_ds, ['first'], file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

        # Test with 2 variables selected
        with create_tmp_file('remove_me', 'jpg') as tmp_file:
            plot_line(single_dim_ds, ['first', 'second'], file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

        # Test with specified formats for each variable
        with create_tmp_file('remove_me', 'jpg') as tmp_file:
            plot_line(single_dim_ds, ['first', 'second'], fmt='r^--;b-',  file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

        # Test with partially specified formats
        with create_tmp_file('remove_me', 'jpg') as tmp_file:
            plot_line(single_dim_ds, ['first', 'second'], fmt='r^-', file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

        multi_dim_ds = xr.Dataset({
            'first': (['lat', 'lon', 'layers', 'time'], np.random.rand(5, 10, 12, 2)),
            'second': (['lat', 'lon', 'layers', 'time'], np.random.rand(5, 10, 12, 2)),
            'lat': np.linspace(-89.5, 89.5, 5),
            'lon': np.linspace(-179.5, 179.5, 10),
            'layers': np.linspace(0, 12, 12),
            'time': pd.date_range('2000-01-01', periods=2)})

        # Test value error is raised when there are too many dimensions to plot
        with create_tmp_file('remove_me', 'jpeg') as tmp_file:
            with self.assertRaises(ValueError) as cm:
                plot_line(multi_dim_ds, ['first', 'second'], file=tmp_file)
            self.assertEqual("One dimension is expected, but multiple dimensions are present. Please use indexers to "
                             "specify values from 3 of the following dimensions ['lat', 'lon', 'layers', 'time']",
                             str(cm.exception))
            self.assertFalse(os.path.isfile(tmp_file))

        # Now with indexers but there are still some unspecified dimension
        with create_tmp_file('remove_me', 'jpeg') as tmp_file:
            with self.assertRaises(ValueError) as cm:
                plot_line(multi_dim_ds, ['first', 'second'], indexers="lat=89.5, lon=-179.5", file=tmp_file)
            self.assertEqual("One dimension is expected, but multiple dimensions are present. Please use indexers to "
                             "specify values from 1 of the following dimensions ['layers', 'time']",
                             str(cm.exception))
            self.assertFalse(os.path.isfile(tmp_file))

        # Now with indexers with all dimensions (except 1) specified
        with create_tmp_file('remove_me', 'jpeg') as tmp_file:
            plot_line(multi_dim_ds, ['first', 'second'], indexers="lat=89.5, lon=-179.5, layers=0", file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

        # Now with indexers with all dimensions (except 1) and label specified
        with create_tmp_file('remove_me', 'jpeg') as tmp_file:
            plot_line(multi_dim_ds, ['first', 'second'], label='time', indexers="lat=89.5, lon=-179.5, layers=0",
                      file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

        # Now with indexers and label specified with some overlapping
        with create_tmp_file('remove_me', 'jpeg') as tmp_file:
            with self.assertRaises(ValueError) as cm:
                plot_line(multi_dim_ds, ['first', 'second'], label='layers', indexers="lat=89.5, lon=-179.5, layers=0",
                          file=tmp_file)
            self.assertEqual(
                "The specified label 'layers' does not match with any dimension names of the dataset. It is possible "
                "that it has been accidentally specified as one of the indexers.",
                str(cm.exception))
            self.assertFalse(os.path.isfile(tmp_file))

    def test_registered(self):
        """
        Test nominal execution of the function as a registered operation.
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(plot_line))
        # Test plot
        dataset = xr.Dataset({
            'first': (['time'], np.random.rand(10)),
            'second': (['time'], np.random.rand(10)),
            'time': pd.date_range('2000-01-01', periods=10)})

        with create_tmp_file('remove_me', 'jpg') as tmp_file:
            reg_op(ds=dataset, var_names=['first', 'second'], file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))


@unittest.skipIf(condition=os.environ.get('CATE_DISABLE_PLOT_TESTS', None),
                 reason="skipped if CATE_DISABLE_PLOT_TESTS=1")
class TestPlotDataFrame(TestCase):
    """
    Test plot_data_frame() function.
    """

    def test_nominal(self):
        """
        Test nominal execution
        """
        data = {'A': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                'B': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]}
        df = pd.DataFrame(data=data, index=pd.date_range('2000-01-01',
                                                         periods=10))

        with create_tmp_file('remove_me', 'png') as tmp_file:
            plot_data_frame(df, file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

    def test_registered(self):
        """
        Test the method when run as a registered operation
        """
        reg_op = OP_REGISTRY.get_op(object_to_qualified_name(plot_data_frame))

        data = {'A': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                'B': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]}
        df = pd.DataFrame(data=data, index=pd.date_range('2000-01-01',
                                                         periods=10))

        with create_tmp_file('remove_me', 'png') as tmp_file:
            reg_op(df=df, file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))


@unittest.skipIf(condition=os.environ.get('CATE_DISABLE_PLOT_TESTS', None),
                 reason="skipped if CATE_DISABLE_PLOT_TESTS=1")
class TestPlotHovmoeller(TestCase):
    """
    Test plot_hovmoeller() function
    """

    def test_nominal(self):
        """
        Test nominal execution
        """
        # Test nominal
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.random.rand(5, 10, 2)),
            'second': (['lat', 'lon', 'time'], np.random.rand(5, 10, 2)),
            'lat': np.linspace(-89.5, 89.5, 5),
            'lon': np.linspace(-179.5, 179.5, 10),
            'time': pd.date_range('2000-01-01', periods=2)})

        with create_tmp_file('remove_me', 'png') as tmp_file:
            plot_hovmoeller(dataset, file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

        # Test colormesh and title
        with create_tmp_file('remove_me', 'png') as tmp_file:
            plot_hovmoeller(dataset, contour=False, title='FooBar', file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

        # More than three dims
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time', 'depth'], np.random.rand(5, 10, 2, 2)),
            'second': (['lat', 'lon', 'time'], np.random.rand(5, 10, 2)),
            'lat': np.linspace(-89.5, 89.5, 5),
            'lon': np.linspace(-179.5, 179.5, 10),
            'depth': [1, 2],
            'time': pd.date_range('2000-01-01', periods=2)})

        with create_tmp_file('remove_me', 'png') as tmp_file:
            plot_hovmoeller(dataset, var='first', x_axis='time', y_axis='depth', file=tmp_file)
            self.assertTrue(os.path.isfile(tmp_file))

    def test_exceptions(self):
        """
        Test error conditions
        """
        dataset = xr.Dataset({
            'first': (['lat'], np.random.rand(5)),
            'second': (['lat', 'lon'], np.random.rand(5, 10)),
            'lat': np.linspace(-89.5, 89.5, 5),
            'lon': np.linspace(-179.5, 179.5, 10),
            'time': pd.date_range('2000-01-01', periods=2)})

        # test 1D dataset
        with self.assertRaises(ValueError):
            plot_hovmoeller(dataset, var='first', x_axis='lat', y_axis='lat')

        # test illegal dimensions
        with self.assertRaises(ValueError):
            plot_hovmoeller(dataset, var='second', x_axis='foo', y_axis='bar')
