import json
import os
import unittest
from collections import OrderedDict

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from shapely.geometry import Point

from cate.core.types import ValidationError
from cate.core.workflow import Workflow, OpStep
from cate.core.workspace import Workspace, mk_op_arg, mk_op_args, mk_op_kwargs
from cate.util.opmetainf import OpMetaInfo
from cate.util.undefined import UNDEFINED

NETCDF_TEST_FILE_1 = os.path.join(os.path.dirname(__file__), '..', 'data', 'precip_and_temp.nc')
NETCDF_TEST_FILE_2 = os.path.join(os.path.dirname(__file__), '..', 'data', 'precip_and_temp_2.nc')


class WorkspaceTest(unittest.TestCase):
    def test_utilities(self):
        self.assertEqual(mk_op_arg(1), {'value': 1})
        self.assertEqual(mk_op_arg('2'), {'value': '2'})
        self.assertEqual(mk_op_arg('a'), {'value': 'a'})
        self.assertEqual(mk_op_arg('@b'), {'source': 'b'})

        self.assertEqual(mk_op_args(), [])
        self.assertEqual(mk_op_args(1, '2', 'a', '@b'), [{'value': 1}, {'value': '2'}, {'value': 'a'}, {'source': 'b'}])

        self.assertEqual(mk_op_kwargs(a=1), OrderedDict([('a', {'value': 1})]))
        self.assertEqual(mk_op_kwargs(a=1, b='@c'), OrderedDict([('a', {'value': 1}), ('b', {'source': 'c'})]))

    def test_workspace_is_part_of_context(self):

        def some_op(ctx: dict) -> dict:
            return dict(ctx)

        from cate.core.op import OP_REGISTRY

        try:
            op_reg = OP_REGISTRY.add_op(some_op)
            op_reg.op_meta_info.inputs['ctx']['context'] = True

            ws = Workspace('/path', Workflow(OpMetaInfo('workspace_workflow', header=dict(description='Test!'))))
            ws.set_resource(op_reg.op_meta_info.qualified_name, {}, res_name='new_ctx')
            ws.execute_workflow('new_ctx')

            self.assertTrue('new_ctx' in ws.resource_cache)
            self.assertTrue('workspace' in ws.resource_cache['new_ctx'])
            self.assertIs(ws.resource_cache['new_ctx']['workspace'], ws)

        finally:
            OP_REGISTRY.remove_op(some_op)

    def test_workspace_can_create_new_res_names(self):
        ws = Workspace('/path', Workflow(OpMetaInfo('workspace_workflow', header=dict(description='Test!'))))
        res_name_1 = ws.set_resource('cate.ops.utility.identity', mk_op_kwargs(value='A'))
        res_name_2 = ws.set_resource('cate.ops.utility.identity', mk_op_kwargs(value='B'))
        res_name_3 = ws.set_resource('cate.ops.utility.identity', mk_op_kwargs(value='C'))
        self.assertEqual(res_name_1, 'res_1')
        self.assertEqual(res_name_2, 'res_2')
        self.assertEqual(res_name_3, 'res_3')
        self.assertIsNotNone(ws.workflow.find_node(res_name_1))
        self.assertIsNotNone(ws.workflow.find_node(res_name_2))
        self.assertIsNotNone(ws.workflow.find_node(res_name_3))

    def test_to_json_dict(self):

        def dataset_op() -> xr.Dataset:
            periods = 5
            temperature_data = (15 + 8 * np.random.randn(periods, 2, 2)).round(decimals=1)
            temperature_attrs = {'a': np.array([1, 2, 3]), 'comment': 'hot', '_FillValue': np.nan}
            precipitation_data = (10 * np.random.rand(periods, 2, 2)).round(decimals=1)
            precipitation_attrs = {'x': True, 'comment': 'wet', '_FillValue': -1.0}
            ds = xr.Dataset(
                data_vars={
                    'temperature': (('time', 'lat', 'lon'), temperature_data, temperature_attrs),
                    'precipitation': (('time', 'lat', 'lon'), precipitation_data, precipitation_attrs)
                },
                coords={
                    'lon': np.array([12, 13]),
                    'lat': np.array([50, 51]),
                    'time': pd.date_range('2014-09-06', periods=periods)
                },
                attrs={
                    'history': 'a b c'
                })
            return ds

        def scalar_dataset_op() -> xr.Dataset:
            ds = xr.Dataset(
                data_vars={
                    'temperature': (('time', 'lat', 'lon'), [[[15.2]]]),
                    'precipitation': (('time', 'lat', 'lon'), [[[10.1]]])
                },
                coords={
                    'lon': [12.],
                    'lat': [50.],
                    'time': [pd.to_datetime('2014-09-06')],
                },
                attrs={
                    'history': 'a b c'
                })
            return ds

        def empty_dataset_op() -> xr.Dataset:
            ds = xr.Dataset(
                data_vars={
                    'temperature': (('time', 'lat', 'lon'), np.ndarray(shape=(0, 0, 0), dtype=np.float32)),
                    'precipitation': (('time', 'lat', 'lon'), np.ndarray(shape=(0, 0, 0), dtype=np.float32))
                },
                coords={
                    'lon': np.ndarray(shape=(0,), dtype=np.float32),
                    'lat': np.ndarray(shape=(0,), dtype=np.float32),
                    'time': np.ndarray(shape=(0,), dtype=np.datetime64),
                },
                attrs={
                    'history': 'a b c'
                })
            return ds

        def data_frame_op() -> pd.DataFrame:
            data = {'A': [1, 2, 3, np.nan, 4, 9, np.nan, np.nan, 1, 0, 4, 6],
                    'B': [5, 6, 8, 7, 5, 5, 5, 9, 1, 2, 7, 6]}
            time = pd.date_range('2000-01-01', freq='MS', periods=12)
            return pd.DataFrame(data=data, index=time, dtype=float, columns=['A', 'B'])

        def scalar_data_frame_op() -> pd.DataFrame:
            data = {'A': [1.3],
                    'B': [5.9]}
            return pd.DataFrame(data=data, dtype=float, columns=['A', 'B'])

        def empty_data_frame_op() -> pd.DataFrame:
            data = {'A': [],
                    'B': []}
            return pd.DataFrame(data=data, dtype=float, columns=['A', 'B'])

        def geo_data_frame_op() -> gpd.GeoDataFrame:
            data = {'name': ['A', 'B', 'C'],
                    'lat': [45, 46, 47.5],
                    'lon': [-120, -121.2, -122.9]}
            df = pd.DataFrame(data, columns=['name', 'lat', 'lon'])
            geometry = [Point(xy) for xy in zip(df['lon'], df['lat'])]
            return gpd.GeoDataFrame(df, geometry=geometry)

        def scalar_geo_data_frame_op() -> gpd.GeoDataFrame:
            data = {'name': [2000 * 'A'],
                    'lat': [45],
                    'lon': [-120]}
            df = pd.DataFrame(data, columns=['name', 'lat', 'lon'])
            geometry = [Point(xy) for xy in zip(df['lon'], df['lat'])]
            return gpd.GeoDataFrame(df, geometry=geometry)

        def int_op() -> int:
            return 394852

        def str_op() -> str:
            return 'Hi!'

        from cate.core.op import OP_REGISTRY

        try:
            OP_REGISTRY.add_op(dataset_op)
            OP_REGISTRY.add_op(data_frame_op)
            OP_REGISTRY.add_op(geo_data_frame_op)
            OP_REGISTRY.add_op(scalar_dataset_op)
            OP_REGISTRY.add_op(scalar_data_frame_op)
            OP_REGISTRY.add_op(scalar_geo_data_frame_op)
            OP_REGISTRY.add_op(empty_dataset_op)
            OP_REGISTRY.add_op(empty_data_frame_op)
            OP_REGISTRY.add_op(int_op)
            OP_REGISTRY.add_op(str_op)
            workflow = Workflow(OpMetaInfo('workspace_workflow', header=dict(description='Test!')))
            workflow.add_step(OpStep(dataset_op, node_id='ds'))
            workflow.add_step(OpStep(data_frame_op, node_id='df'))
            workflow.add_step(OpStep(geo_data_frame_op, node_id='gdf'))
            workflow.add_step(OpStep(scalar_dataset_op, node_id='scalar_ds'))
            workflow.add_step(OpStep(scalar_data_frame_op, node_id='scalar_df'))
            workflow.add_step(OpStep(scalar_geo_data_frame_op, node_id='scalar_gdf'))
            workflow.add_step(OpStep(empty_dataset_op, node_id='empty_ds'))
            workflow.add_step(OpStep(empty_data_frame_op, node_id='empty_df'))
            workflow.add_step(OpStep(int_op, node_id='i'))
            workflow.add_step(OpStep(str_op, node_id='s'))
            ws = Workspace('/path', workflow)
            ws.execute_workflow()

            d_ws = ws.to_json_dict()
            # import pprint
            # pprint.pprint(d_ws)

            d_wf = d_ws.get('workflow')
            self.assertIsNotNone(d_wf)

            l_res = d_ws.get('resources')
            self.assertIsNotNone(l_res)
            self.assertEqual(len(l_res), 10)

            res_ds = l_res[0]
            self.assertEqual(res_ds.get('name'), 'ds')
            self.assertEqual(res_ds.get('dataType'), 'xarray.core.dataset.Dataset')
            self.assertEqual(res_ds.get('dimSizes'), dict(lat=2, lon=2, time=5))
            self.assertEqual(res_ds.get('attributes'), {'history': 'a b c'})
            res_ds_vars = res_ds.get('variables')
            self.assertIsNotNone(res_ds_vars)
            self.assertEqual(len(res_ds_vars), 2)
            res_ds_var_1 = res_ds_vars[0]
            self.assertEqual(res_ds_var_1.get('name'), 'precipitation')
            self.assertEqual(res_ds_var_1.get('dataType'), 'float64')
            self.assertEqual(res_ds_var_1.get('numDims'), 3)
            self.assertEqual(res_ds_var_1.get('shape'), (5, 2, 2))
            self.assertEqual(res_ds_var_1.get('chunkSizes'), None)
            self.assertEqual(res_ds_var_1.get('isYFlipped'), True)
            self.assertEqual(res_ds_var_1.get('isFeatureAttribute'), None)
            self.assertEqual(res_ds_var_1.get('attributes'), dict(x=True, comment='wet', _FillValue=-1.))
            res_ds_var_2 = res_ds_vars[1]
            self.assertEqual(res_ds_var_2.get('name'), 'temperature')
            self.assertEqual(res_ds_var_2.get('dataType'), 'float64')
            self.assertEqual(res_ds_var_2.get('numDims'), 3)
            self.assertEqual(res_ds_var_2.get('shape'), (5, 2, 2))
            self.assertEqual(res_ds_var_2.get('chunkSizes'), None)
            self.assertEqual(res_ds_var_2.get('isYFlipped'), True)
            self.assertEqual(res_ds_var_2.get('isFeatureAttribute'), None)
            self.assertEqual(res_ds_var_2.get('attributes'), dict(a=[1, 2, 3], comment='hot', _FillValue=np.nan))

            res_df = l_res[1]
            self.assertEqual(res_df.get('name'), 'df')
            self.assertEqual(res_df.get('dataType'), 'pandas.core.frame.DataFrame')
            self.assertIsNone(res_df.get('attributes'))
            res_df_vars = res_df.get('variables')
            self.assertIsNotNone(res_df_vars)
            self.assertEqual(len(res_df_vars), 2)
            res_df_var_1 = res_df_vars[0]
            self.assertEqual(res_df_var_1.get('name'), 'A')
            self.assertEqual(res_df_var_1.get('dataType'), 'float64')
            self.assertEqual(res_df_var_1.get('numDims'), 1)
            self.assertEqual(res_df_var_1.get('shape'), (12,))
            self.assertEqual(res_df_var_1.get('isYFlipped'), None)
            self.assertEqual(res_df_var_1.get('isFeatureAttribute'), True)
            self.assertIsNone(res_df_var_1.get('attributes'))
            res_df_var_2 = res_df_vars[1]
            self.assertEqual(res_df_var_2.get('name'), 'B')
            self.assertEqual(res_df_var_2.get('dataType'), 'float64')
            self.assertEqual(res_df_var_2.get('numDims'), 1)
            self.assertEqual(res_df_var_2.get('shape'), (12,))
            self.assertEqual(res_df_var_2.get('isYFlipped'), None)
            self.assertEqual(res_df_var_2.get('isFeatureAttribute'), True)
            self.assertIsNone(res_df_var_2.get('attributes'))

            res_gdf = l_res[2]
            self.assertEqual(res_gdf.get('name'), 'gdf')
            self.assertEqual(res_gdf.get('dataType'), 'geopandas.geodataframe.GeoDataFrame')
            self.assertIsNone(res_gdf.get('attributes'))
            res_gdf_vars = res_gdf.get('variables')
            self.assertIsNotNone(res_gdf_vars)
            self.assertEqual(len(res_gdf_vars), 4)
            res_gdf_var_1 = res_gdf_vars[0]
            self.assertEqual(res_gdf_var_1.get('name'), 'name')
            self.assertEqual(res_gdf_var_1.get('dataType'), 'object')
            self.assertEqual(res_gdf_var_1.get('numDims'), 1)
            self.assertEqual(res_gdf_var_1.get('shape'), (3,))
            self.assertEqual(res_gdf_var_1.get('isYFlipped'), None)
            self.assertEqual(res_gdf_var_1.get('isFeatureAttribute'), True)
            self.assertIsNone(res_gdf_var_1.get('attributes'))
            res_gdf_var_2 = res_gdf_vars[1]
            self.assertEqual(res_gdf_var_2.get('name'), 'lat')
            self.assertEqual(res_gdf_var_2.get('dataType'), 'float64')
            self.assertEqual(res_gdf_var_2.get('numDims'), 1)
            self.assertEqual(res_gdf_var_2.get('shape'), (3,))
            self.assertEqual(res_gdf_var_2.get('isYFlipped'), None)
            self.assertEqual(res_gdf_var_2.get('isFeatureAttribute'), True)
            self.assertIsNone(res_gdf_var_2.get('attributes'))
            res_gdf_var_3 = res_gdf_vars[2]
            self.assertEqual(res_gdf_var_3.get('name'), 'lon')
            self.assertEqual(res_gdf_var_3.get('dataType'), 'float64')
            self.assertEqual(res_gdf_var_3.get('numDims'), 1)
            self.assertEqual(res_gdf_var_3.get('shape'), (3,))
            self.assertEqual(res_gdf_var_3.get('isYFlipped'), None)
            self.assertEqual(res_gdf_var_3.get('isFeatureAttribute'), True)
            self.assertIsNone(res_gdf_var_3.get('attributes'))
            res_gdf_var_4 = res_gdf_vars[3]
            self.assertEqual(res_gdf_var_4.get('name'), 'geometry')
            self.assertEqual(res_gdf_var_4.get('dataType'), 'object')
            self.assertEqual(res_gdf_var_4.get('numDims'), 1)
            self.assertEqual(res_gdf_var_4.get('shape'), (3,))
            self.assertEqual(res_gdf_var_4.get('isYFlipped'), None)
            self.assertEqual(res_gdf_var_4.get('isFeatureAttribute'), True)
            self.assertIsNone(res_gdf_var_4.get('attributes'))

            res_scalar_ds = l_res[3]
            res_scalar_ds_vars = res_scalar_ds.get('variables')
            self.assertIsNotNone(res_scalar_ds_vars)
            self.assertEqual(len(res_scalar_ds_vars), 2)
            scalar_values = {res_scalar_ds_vars[0].get('name'): res_scalar_ds_vars[0].get('value'),
                             res_scalar_ds_vars[1].get('name'): res_scalar_ds_vars[1].get('value')}
            self.assertEqual(scalar_values, {'temperature': 15.2, 'precipitation': 10.1})

            res_scalar_df = l_res[4]
            res_scalar_df_vars = res_scalar_df.get('variables')
            self.assertIsNotNone(res_scalar_df_vars)
            self.assertEqual(len(res_scalar_df_vars), 2)
            scalar_values = {res_scalar_df_vars[0].get('name'): res_scalar_df_vars[0].get('value'),
                             res_scalar_df_vars[1].get('name'): res_scalar_df_vars[1].get('value')}
            self.assertEqual(scalar_values, {'A': 1.3, 'B': 5.9})

            res_scalar_gdf = l_res[5]
            res_scalar_gdf_vars = res_scalar_gdf.get('variables')
            self.assertIsNotNone(res_scalar_gdf_vars)
            self.assertEqual(len(res_scalar_gdf_vars), 4)
            scalar_values = {res_scalar_gdf_vars[0].get('name'): res_scalar_gdf_vars[0].get('value'),
                             res_scalar_gdf_vars[1].get('name'): res_scalar_gdf_vars[1].get('value'),
                             res_scalar_gdf_vars[2].get('name'): res_scalar_gdf_vars[2].get('value'),
                             res_scalar_gdf_vars[3].get('name'): res_scalar_gdf_vars[3].get('value')}
            self.assertEqual(scalar_values, {'name': (1000 * 'A') + '...',
                                             'lat': 45,
                                             'lon': -120,
                                             'geometry': 'POINT (-120 45)'})

            res_empty_ds = l_res[6]
            res_empty_ds_vars = res_empty_ds.get('variables')
            self.assertIsNotNone(res_empty_ds_vars)
            self.assertEqual(len(res_empty_ds_vars), 2)
            scalar_values = {res_empty_ds_vars[0].get('name'): res_empty_ds_vars[0].get('value'),
                             res_empty_ds_vars[1].get('name'): res_empty_ds_vars[1].get('value')}
            self.assertEqual(scalar_values, {'temperature': None, 'precipitation': None})

            res_empty_df = l_res[7]
            res_empty_df_vars = res_empty_df.get('variables')
            self.assertIsNotNone(res_empty_df_vars)
            self.assertEqual(len(res_empty_df_vars), 2)
            scalar_values = {res_empty_df_vars[0].get('name'): res_empty_df_vars[0].get('value'),
                             res_empty_df_vars[1].get('name'): res_empty_df_vars[1].get('value')}
            self.assertEqual(scalar_values, {'A': None, 'B': None})

            res_int = l_res[8]
            self.assertEqual(res_int.get('name'), 'i')
            self.assertEqual(res_int.get('dataType'), 'int')
            self.assertIsNone(res_int.get('attributes'))
            self.assertIsNone(res_int.get('variables'))

            res_str = l_res[9]
            self.assertEqual(res_str.get('name'), 's')
            self.assertEqual(res_str.get('dataType'), 'str')
            self.assertIsNone(res_str.get('attributes'))
            self.assertIsNone(res_str.get('variables'))

        finally:
            OP_REGISTRY.remove_op(dataset_op)
            OP_REGISTRY.remove_op(data_frame_op)
            OP_REGISTRY.remove_op(geo_data_frame_op)
            OP_REGISTRY.remove_op(scalar_dataset_op)
            OP_REGISTRY.remove_op(scalar_data_frame_op)
            OP_REGISTRY.remove_op(scalar_geo_data_frame_op)
            OP_REGISTRY.remove_op(empty_dataset_op)
            OP_REGISTRY.remove_op(empty_data_frame_op)
            OP_REGISTRY.remove_op(int_op)
            OP_REGISTRY.remove_op(str_op)

    def test_execute_empty_workflow(self):
        ws = Workspace('/path', Workflow(OpMetaInfo('workspace_workflow', header=dict(description='Test!'))))
        ws.execute_workflow()

    def test_set_and_execute_step(self):
        ws = Workspace('/path', Workflow(OpMetaInfo('workspace_workflow', header=dict(description='Test!'))))

        with self.assertRaises(ValidationError) as we:
            ws.set_resource("not_existing_op", {})
        self.assertEqual('Unknown operation "not_existing_op"', str(we.exception))

        with self.assertRaises(ValidationError) as we:
            ws.set_resource('cate.ops.io.read_netcdf', mk_op_kwargs(location=NETCDF_TEST_FILE_1), res_name='X')
        self.assertEqual('"location" is not an input of operation "cate.ops.io.read_netcdf"', str(we.exception))

        with self.assertRaises(ValidationError) as we:
            ws.set_resource('cate.ops.io.read_netcdf', {'file': {'foo': 'bar'}}, res_name='X')
        self.assertEqual('Illegal argument for input "file" of operation "cate.ops.io.read_netcdf', str(we.exception))

        ws.set_resource('cate.ops.io.read_netcdf', mk_op_kwargs(file=NETCDF_TEST_FILE_1), res_name='X')
        ws.set_resource('cate.ops.timeseries.tseries_mean', mk_op_kwargs(ds="@X", var="precipitation"), res_name='Y')
        self.assertEqual(ws.resource_cache, {})

        ws.execute_workflow('Y')
        self.assertIn('X', ws.resource_cache)
        self.assertIn('Y', ws.resource_cache)

        ws.set_resource('cate.ops.timeseries.tseries_mean', mk_op_kwargs(ds="@X", var="temperature"), res_name='Y',
                        overwrite=True)
        self.assertIn('X', ws.resource_cache)
        self.assertIn('Y', ws.resource_cache)
        self.assertIs(ws.resource_cache['Y'], UNDEFINED)

        ws.execute_workflow('Y')
        self.assertIn('X', ws.resource_cache)
        self.assertIn('Y', ws.resource_cache)

        ws.set_resource('cate.ops.io.read_netcdf', mk_op_kwargs(file=NETCDF_TEST_FILE_2), res_name='X', overwrite=True)
        self.assertIn('X', ws.resource_cache)
        self.assertIs(ws.resource_cache['X'], UNDEFINED)
        self.assertIn('Y', ws.resource_cache)
        self.assertIs(ws.resource_cache['Y'], UNDEFINED)

        ws.execute_workflow('Y')
        self.assertIn('X', ws.resource_cache)
        self.assertIn('Y', ws.resource_cache)

    def test_set_and_rename_and_execute_step(self):
        ws = Workspace('/path', Workflow(OpMetaInfo('workspace_workflow', header=dict(description='Test!'))))
        self.assertEqual(ws.user_data, {})

        ws.set_resource('cate.ops.utility.identity', mk_op_kwargs(value=1), res_name='X')
        ws.set_resource('cate.ops.utility.identity', mk_op_kwargs(value="@X"), res_name='Y')
        ws.set_resource('cate.ops.utility.identity', mk_op_kwargs(value="@X"), res_name='Z')
        self.assertEqual(len(ws.workflow.steps), 3)
        self.assertEqual(ws.resource_cache, {})

        value = ws.execute_workflow('Y')
        self.assertEqual(value, 1)
        self.assertEqual(ws.resource_cache.get('X'), 1)
        self.assertEqual(ws.resource_cache.get('Y'), 1)
        self.assertEqual(ws.resource_cache.get('Z'), None)

        value = ws.execute_workflow('Z')
        self.assertEqual(value, 1)
        self.assertEqual(ws.resource_cache.get('X'), 1)
        self.assertEqual(ws.resource_cache.get('Y'), 1)
        self.assertEqual(ws.resource_cache.get('Z'), 1)

        ws.set_resource('cate.ops.utility.identity', mk_op_kwargs(value=9), res_name='X', overwrite=True)
        self.assertEqual(len(ws.workflow.steps), 3)
        self.assertEqual(ws.resource_cache.get('X'), UNDEFINED)
        self.assertEqual(ws.resource_cache.get('Y'), UNDEFINED)
        self.assertEqual(ws.resource_cache.get('Z'), UNDEFINED)

        ws.execute_workflow()
        self.assertEqual(ws.resource_cache.get('X'), 9)
        self.assertEqual(ws.resource_cache.get('Y'), 9)
        self.assertEqual(ws.resource_cache.get('Z'), 9)

        ws.rename_resource('X', 'A')
        self.assertIsNone(ws.workflow.find_node('X'))
        self.assertIsNotNone(ws.workflow.find_node('A'))
        self.assertEqual(ws.resource_cache.get('X', '--'), '--')
        self.assertEqual(ws.resource_cache.get('A'), 9)
        self.assertEqual(ws.resource_cache.get('Y'), 9)
        self.assertEqual(ws.resource_cache.get('Z'), 9)

        ws.set_resource('cate.ops.utility.identity', mk_op_kwargs(value=5), res_name='A', overwrite=True)
        self.assertEqual(ws.resource_cache.get('X', '--'), '--')
        self.assertEqual(ws.resource_cache.get('A'), UNDEFINED)
        self.assertEqual(ws.resource_cache.get('Y'), UNDEFINED)
        self.assertEqual(ws.resource_cache.get('Z'), UNDEFINED)

        ws.execute_workflow()
        self.assertEqual(ws.resource_cache.get('X', '--'), '--')
        self.assertEqual(ws.resource_cache.get('A'), 5)
        self.assertEqual(ws.resource_cache.get('Y'), 5)
        self.assertEqual(ws.resource_cache.get('Z'), 5)

    @unittest.skip("_extract_point is not an operator anymore")
    def test_set_step_and_run_op(self):
        ws = Workspace('/path', Workflow(OpMetaInfo('workspace_workflow', header=dict(description='Test!'))))

        ws.set_resource('cate.ops.io.read_netcdf', mk_op_kwargs(file=NETCDF_TEST_FILE_1), res_name='X')
        ws.execute_workflow('X')
        self.assertIsNotNone(ws.workflow)
        self.assertEqual(len(ws.workflow.steps), 1)
        self.assertIn('X', ws.resource_cache)

        op_name = '_extract_point'
        op_args = mk_op_kwargs(ds='@X', point='10.22, 34.52', indexers=dict(time='2014-09-11'), should_return=True)
        op_result = ws.run_op(op_name, op_args)
        self.assertEqual(len(op_result), 4)
        self.assertAlmostEqual(op_result['lat'], 34.5)
        self.assertAlmostEqual(op_result['lon'], 10.2)
        self.assertAlmostEqual(op_result['precipitation'], 5.5)
        self.assertAlmostEqual(op_result['temperature'], 32.9)

        # without asking for return data
        op_args = mk_op_kwargs(ds='@X', point='10.22, 34.52', indexers=dict(time='2014-09-11'))
        op_result = ws.run_op(op_name, op_args)
        self.assertIsNone(op_result)

        # with a non existing operator name
        with self.assertRaises(ValidationError) as we:
            ws.run_op("not_existing_op", {})
        self.assertEqual('Unknown operation "not_existing_op"', str(we.exception))

    # TODO (forman): #391
    def test_set_resource_is_reentrant(self):
        from concurrent.futures import ThreadPoolExecutor

        ws = Workspace('/path', Workflow(OpMetaInfo('workspace_workflow', header=dict(description='Test!'))))

        def set_resource_and_execute():
            res_name = ws.set_resource('cate.ops.utility.no_op',
                                       op_kwargs=dict(num_steps=dict(value=10),
                                                      step_duration=dict(value=0.05)))
            ws.execute_workflow(res_name=res_name)
            return res_name

        num_res = 5
        res_names = []
        with ThreadPoolExecutor(max_workers=2 * num_res) as executor:
            for i in range(num_res):
                res_names.append(executor.submit(set_resource_and_execute))

        actual_res_names = {f.result() for f in res_names}
        expected_res_names = {'res_%s' % (i + 1) for i in range(num_res)}
        self.assertEqual(actual_res_names, expected_res_names)

    def test_validate_res_name(self):
        Workspace._validate_res_name("a")
        Workspace._validate_res_name("A")
        Workspace._validate_res_name("abc_42")
        Workspace._validate_res_name("abc42")
        Workspace._validate_res_name("_abc42")
        # with self.assertRaises(ValidationError):
        #     Workspace._validate_res_name("0")
        with self.assertRaises(ValidationError):
            Workspace._validate_res_name("a-b")
        with self.assertRaises(ValidationError):
            Workspace._validate_res_name("a+b")
        with self.assertRaises(ValidationError):
            Workspace._validate_res_name("a.b")
        with self.assertRaises(ValidationError):
            Workspace._validate_res_name("file://path")

    def test_example(self):
        expected_json_text = """{
            "schema_version": 1,
            "qualified_name": "workspace_workflow",
            "header": {
                "description": "Test!"
            },
            "inputs": {},
            "outputs": {},
            "steps": [
                {
                    "id": "p",
                    "op": "cate.ops.io.read_netcdf",
                    "inputs": {
                        "file": {
                            "value": "%s"
                        }
                    }
                },
                {
                    "id": "ts",
                    "op": "cate.ops.timeseries.tseries_mean",
                    "inputs": {
                        "ds": "p",
                        "var": {
                          "value": "precipitation"
                        }
                    }
                }
            ]
        }
        """ % NETCDF_TEST_FILE_1.replace('\\', '\\\\')

        expected_json_dict = json.loads(expected_json_text)

        ws = Workspace('/path', Workflow(OpMetaInfo('workspace_workflow', header=dict(description='Test!'))))
        # print("wf_1: " + json.dumps(ws.workflow.to_json_dict(), indent='  '))
        ws.set_resource('cate.ops.io.read_netcdf', mk_op_kwargs(file=NETCDF_TEST_FILE_1), res_name='p')
        # print("wf_2: " + json.dumps(ws.workflow.to_json_dict(), indent='  '))
        ws.set_resource('cate.ops.timeseries.tseries_mean', mk_op_kwargs(ds="@p", var="precipitation"), res_name='ts')
        # print("wf_3: " + json.dumps(ws.workflow.to_json_dict(), indent='  '))

        self.maxDiff = None
        self.assertEqual(ws.workflow.to_json_dict(), expected_json_dict)

        with self.assertRaises(ValueError) as e:
            ws.set_resource('cate.ops.timeseries.tseries_point',
                            mk_op_kwargs(ds="@p", point="iih!", var="precipitation"), res_name='ts2',
                            validate_args=True)
        self.assertEqual(str(e.exception),
                         "Input 'point' for operation 'cate.ops.timeseries.tseries_point': "
                         "Value cannot be converted into a 'PointLike': "
                         "Invalid geometry WKT format.")

        ws2 = Workspace.from_json_dict(ws.to_json_dict())
        self.assertEqual(ws2.base_dir, ws.base_dir)
        self.assertEqual(ws2.workflow.op_meta_info.qualified_name, ws.workflow.op_meta_info.qualified_name)
        self.assertEqual(len(ws2.workflow.steps), len(ws.workflow.steps))
