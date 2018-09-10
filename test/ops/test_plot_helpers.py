"""
Tests for plot and animation helper functions
"""

from unittest import TestCase

import xarray as xr
import numpy as np
import pandas as pd

from cate.ops.plot_helpers import check_bounding_box, in_notebook, get_var_data, get_vars_data, determine_cmap_params
from cate.core.types import ValidationError


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

        # test on data with more dims
        dataset2 = xr.Dataset({
            'first': (['lat', 'lon', 'layers', 'time'], np.random.rand(5, 10, 16, 2)),
            'second': (['lat', 'lon', 'layers', 'time'], np.random.rand(5, 10, 16, 2)),
            'lat': np.linspace(-89.5, 89.5, 5),
            'lon': np.linspace(-179.5, 179.5, 10),
            'layers': np.linspace(1, 16, 16),
            'time': pd.date_range('2000-01-01', periods=2)})

        indexers = {'lat': 0, 'lon': -179.5, 'layers': 5}
        out = get_var_data(dataset2.second, indexers, remaining_dims=['time', ])
        self.assertEqual(out.lat, 0)
        self.assertEqual(out.lon, -179.5)
        self.assertEqual(out.layers, 5)
        self.assertEqual(len(out.time.values), 2)

        # test when a dimension is neither in indexers nor remaining_dims specified,
        # it will select the first element of that dimension
        indexers = {'lat': 0, 'lon': -179.5}
        out = get_var_data(dataset2.second, indexers, remaining_dims=['time', ])
        print(f'out {out}')
        self.assertEqual(out.lat, 0)
        self.assertEqual(out.lon, -179.5)
        self.assertEqual(out.layers, 1)
        self.assertEqual(len(out.time.values), 2)

        # should raise a ValidationError when the specified dimension is not part of variable dimension names
        indexers = {'lat': 0, 'lon': -179.5, 'time': '2000-01-01'}
        with self.assertRaises(ValidationError) as cm:
            get_var_data(dataset2.second, indexers, remaining_dims=['dummy', ])
        self.assertEqual(str(cm.exception), "The specified dataset does not have a dimension called \'dummy\'.")

        # should raise a ValidationError when a dimension is specified as both indexers and remaining_dims
        indexers = {'lat': 0, 'lon': -179.5, 'time': '2000-01-01'}
        with self.assertRaises(ValidationError) as cm:
            get_var_data(dataset2.second, indexers, remaining_dims=['time', ])
        self.assertEqual(str(cm.exception), "Dimension 'time' is also specified as indexers. Please ensure that a "
                                            "dimension is used exclusively either as indexers or as the selected "
                                            "dimension.")

        # should raise a ValidationError when a dimension is specified as indexers does not exist in the given variable
        indexers = {'lat': 0, 'lon': -179.5, 'dummy': 1}
        with self.assertRaises(ValidationError) as cm:
            get_var_data(dataset2.second, indexers, remaining_dims=['time', ])
        self.assertEqual(str(cm.exception), "The specified dataset does not have a dimension called 'dummy'.")

    def test_get_vars_data(self):
        dataset = xr.Dataset({
            'first': (['lat', 'lon', 'layers1', 'time'], np.random.rand(5, 10, 16, 2)),
            'second': (['lat', 'lon', 'layers2', 'time'], np.random.rand(5, 10, 4, 2)),
            'third': (['lat', 'lon', 'layers1', 'layers2', 'time'], np.random.rand(5, 10, 16, 4, 2)),
            'fourth': (['lat', 'lon', 'time'], np.random.rand(5, 10, 2)),
            'lat': np.linspace(-89.5, 89.5, 5),
            'lon': np.linspace(-179.5, 179.5, 10),
            'layers1': np.linspace(1, 16, 16),
            'layers2': np.linspace(1, 4, 4),
            'time': pd.date_range('2000-01-01', periods=2)})

        indexers = {'lat': -89.5, 'lon': -179.5, 'layers1': 5, 'layers2': 4}
        vars_data = get_vars_data(dataset, indexers, remaining_dims=['time', ])
        self.assertEqual(9, len(vars_data.variables))
        self.assertEqual(5, len(vars_data.dims))
        self.assertEqual(1, len(vars_data.first.dims))
        self.assertEqual(1, len(vars_data.second.dims))
        self.assertEqual(1, len(vars_data.third.dims))
        self.assertEqual(1, len(vars_data.fourth.dims))
        # TODO (hp): is it possible to check if the right data has been selected? When merged back to ds, the
        # dimensions selected by the indexers are not visible anymore.
        self.assertEqual(2, len(vars_data.time))

        indexers = {'lat': -89.5, 'lon': 179.5}
        vars_data = get_vars_data(dataset, indexers, remaining_dims=['time', ])
        self.assertEqual(9, len(vars_data.variables))
        self.assertEqual(5, len(vars_data.dims))
        self.assertEqual(1, len(vars_data.first.dims))
        self.assertEqual(1, len(vars_data.second.dims))
        self.assertEqual(1, len(vars_data.third.dims))
        self.assertEqual(1, len(vars_data.fourth.dims))
        self.assertEqual(2, len(vars_data.time))

        indexers = {'lat': -89.5, 'lon': 179.5}
        vars_data = get_vars_data(dataset, indexers)
        self.assertEqual(9, len(vars_data.variables))
        self.assertEqual(5, len(vars_data.dims))
        self.assertEqual(2, len(vars_data.first.dims))
        self.assertEqual(2, len(vars_data.second.dims))
        self.assertEqual(3, len(vars_data.third.dims))
        self.assertEqual(1, len(vars_data.fourth.dims))
        self.assertEqual(2, len(vars_data.time))

        # should raise a ValidationError when the specified dimension is not part of variable dimension names
        indexers = {'lat': 0, 'lon': -179.5, 'time': '2000-01-01'}
        with self.assertRaises(ValidationError) as cm:
            get_vars_data(dataset, indexers, remaining_dims=['dummy', ])
        self.assertEqual("The specified dataset does not have a dimension called 'dummy'.", str(cm.exception))

        # should raise a ValidationError when a dimension is specified as both indexers and remaining_dims
        indexers = {'lat': 0, 'lon': -179.5, 'time': '2000-01-01'}
        with self.assertRaises(ValidationError) as cm:
            get_vars_data(dataset, indexers, remaining_dims=['time', ])
        self.assertEqual("Dimension 'time' is also specified as indexers. Please ensure that a "
                         "dimension is used exclusively either as indexers or as the selected "
                         "dimension.", str(cm.exception))

        # should raise a ValidationError when a dimension is specified as indexers does not exist in the given variable
        indexers = {'lat': 0, 'lon': -179.5, 'dummy': 1}
        with self.assertRaises(ValidationError) as cm:
            get_vars_data(dataset, indexers, remaining_dims=['time', ])
        self.assertEqual("There are dimensions specified in indexers but do not match dimensions in "
                         "any variables: ['dummy']", str(cm.exception))


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
