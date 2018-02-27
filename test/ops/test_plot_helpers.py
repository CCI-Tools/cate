"""
Tests for plot and animation helper functions
"""

from unittest import TestCase

import xarray as xr
import numpy as np
import pandas as pd

from cate.ops.plot_helpers import check_bounding_box, in_notebook, get_var_data, determine_cmap_params


class TestCheckBoundingBox(TestCase):
    """
    Test check_bounding_box()
    """
    def test_nominal(self):
        """
        Nominal test
        """
        self.assertTrue(check_bounding_box(10, 20, 10, 20))
        self.assertFalse(check_bounding_box(20, 10, 10, 20))
        self.assertFalse(check_bounding_box(10, 20, 20, 10))
        self.assertFalse(check_bounding_box(-91, 10, 10, 20))
        self.assertFalse(check_bounding_box(20, 91, 10, 20))
        self.assertFalse(check_bounding_box(10, 20, -181, 20))
        self.assertFalse(check_bounding_box(10, 20, 10, 181))


class TestInNotebook(TestCase):
    """
    Test in_notebook()
    """
    def test_nominal(self):
        """
        Nominal test
        """
        self.assertFalse(in_notebook())


class TestGetVarData(TestCase):
    """
    Test get_var_data()
    """
    def test_nominal(self):
        """
        Nominal test
        """
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'time'], np.random.rand(5, 10, 2)),
            'second': (['lat', 'lon', 'time'], np.random.rand(5, 10, 2)),
            'lat': np.linspace(-89.5, 89.5, 5),
            'lon': np.linspace(-179.5, 179.5, 10),
            'time': pd.date_range('2000-01-01', periods=2)})

        indexers = {'time': '2000-01-01', 'lat': -88}
        out = get_var_data(dataset.second, indexers, remaining_dims=['lon', ])
        self.assertEqual(out.time, np.datetime64('2000-01-01'))
        self.assertEqual(out.lat, -89.5)
        self.assertEqual(len(out.lon.values), 10)

        indexers = {'lat': -88, }
        out = get_var_data(dataset.second,
                           indexers,
                           remaining_dims=['lon', ],
                           time=np.datetime64('2000-01-01'))
        self.assertEqual(out.time, np.datetime64('2000-01-01'))
        self.assertEqual(out.lat, -89.5)
        self.assertEqual(len(out.lon.values), 10)


class TestDetermineCmapParams(TestCase):
    """
    Test determine_cmap_params()
    """
    def test_nominal(self):
        """
        Nominal test
        """
        # Test nominal
        cparams = determine_cmap_params(0, 100)
        self.assertEqual(cparams['cmap'], 'viridis')

        # Test divergent
        cparams = determine_cmap_params(-100, 100)
        self.assertEqual(cparams['cmap'], 'RdBu_r')

        # Test extents
        cparams = determine_cmap_params(0, 100, vmin=10, vmax=90)
        self.assertEqual(cparams['extend'], 'both')

        # Test levels
        cparams = determine_cmap_params(0, 100, levels=3, vmin=10)
        self.assertEqual(len(cparams['levels']), 3)
        self.assertIsNotNone(cparams['norm'])
