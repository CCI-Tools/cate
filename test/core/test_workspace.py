import json
import os
import unittest
from collections import OrderedDict

import numpy as np
import pandas as pd
import xarray as xr

from cate.core.workflow import Workflow, OpStep
from cate.core.workspace import Workspace, mk_op_arg, mk_op_args, mk_op_kwargs
from cate.util import UNDEFINED
from cate.util.opmetainf import OpMetaInfo

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

        def data_frame_op() -> pd.DataFrame:
            data = {'A': [1, 2, 3, np.nan, 4, 9, np.nan, np.nan, 1, 0, 4, 6],
                    'B': [5, 6, 8, 7, 5, 5, 5, 9, 1, 2, 7, 6]}
            time = pd.date_range('2000-01-01', freq='MS', periods=12)
            return pd.DataFrame(data=data, index=time, dtype=float)

        def int_op() -> int:
            return 394852

        def str_op() -> str:
            return 'Hi!'

        from cate.core.op import OP_REGISTRY

        try:
            OP_REGISTRY.add_op(dataset_op)
            OP_REGISTRY.add_op(data_frame_op)
            OP_REGISTRY.add_op(int_op)
            OP_REGISTRY.add_op(str_op)
            workflow = Workflow(OpMetaInfo('workspace_workflow', header=dict(description='Test!')))
            workflow.add_step(OpStep(dataset_op, node_id='ds'))
            workflow.add_step(OpStep(data_frame_op, node_id='df'))
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
            self.assertEqual(len(l_res), 4)

            res_1 = l_res[0]
            self.assertEqual(res_1.get('name'), 'ds')
            self.assertEqual(res_1.get('dataType'), 'xarray.core.dataset.Dataset')
            self.assertEqual(res_1.get('dimSizes'), dict(lat=2, lon=2, time=5))
            self.assertEqual(res_1.get('attributes'), {'history': 'a b c'})
            res_1_vars = res_1.get('variables')
            self.assertIsNotNone(res_1_vars)
            self.assertEqual(len(res_1_vars), 2)
            var_1 = res_1_vars[0]
            self.assertEqual(var_1.get('name'), 'precipitation')
            self.assertEqual(var_1.get('dataType'), 'float64')
            self.assertEqual(var_1.get('numDims'), 3)
            self.assertEqual(var_1.get('shape'), (5, 2, 2))
            self.assertEqual(var_1.get('chunkSizes'), None)
            self.assertEqual(var_1.get('isYFlipped'), True)
            self.assertEqual(var_1.get('isFeatureAttribute'), None)
            self.assertEqual(var_1.get('attributes'), dict(x=True, comment='wet', _FillValue=-1.))
            var_2 = res_1_vars[1]
            self.assertEqual(var_2.get('name'), 'temperature')
            self.assertEqual(var_2.get('dataType'), 'float64')
            self.assertEqual(var_2.get('numDims'), 3)
            self.assertEqual(var_2.get('shape'), (5, 2, 2))
            self.assertEqual(var_2.get('chunkSizes'), None)
            self.assertEqual(var_2.get('isYFlipped'), True)
            self.assertEqual(var_2.get('isFeatureAttribute'), None)
            self.assertEqual(var_2.get('attributes'), dict(a=[1, 2, 3], comment='hot', _FillValue=np.nan))

            res_2 = l_res[1]
            self.assertEqual(res_2.get('name'), 'df')
            self.assertEqual(res_2.get('dataType'), 'pandas.core.frame.DataFrame')
            self.assertIsNone(res_2.get('attributes'))
            res_2_vars = res_2.get('variables')
            self.assertIsNotNone(res_2_vars)
            self.assertEqual(len(res_2_vars), 2)
            var_1 = res_2_vars[0]
            self.assertEqual(var_1.get('name'), 'A')
            self.assertEqual(var_1.get('dataType'), 'float64')
            self.assertEqual(var_1.get('numDims'), 1)
            self.assertEqual(var_1.get('shape'), (12,))
            self.assertEqual(var_1.get('isYFlipped'), None)
            self.assertEqual(var_1.get('isFeatureAttribute'), None)
            self.assertIsNone(var_1.get('attributes'))
            var_2 = res_2_vars[1]
            self.assertEqual(var_2.get('name'), 'B')
            self.assertEqual(var_2.get('dataType'), 'float64')
            self.assertEqual(var_2.get('numDims'), 1)
            self.assertEqual(var_2.get('shape'), (12,))
            self.assertEqual(var_2.get('isYFlipped'), None)
            self.assertEqual(var_2.get('isFeatureAttribute'), None)
            self.assertIsNone(var_2.get('attributes'))

            res_3 = l_res[2]
            self.assertEqual(res_3.get('name'), 'i')
            self.assertEqual(res_3.get('dataType'), 'int')
            self.assertIsNone(res_3.get('attributes'))
            self.assertIsNone(res_3.get('variables'))

            res_4 = l_res[3]
            self.assertEqual(res_4.get('name'), 's')
            self.assertEqual(res_4.get('dataType'), 'str')
            self.assertIsNone(res_4.get('attrs'))
            self.assertIsNone(res_4.get('variables'))

        finally:
            OP_REGISTRY.remove_op(dataset_op)
            OP_REGISTRY.remove_op(data_frame_op)
            OP_REGISTRY.remove_op(int_op)
            OP_REGISTRY.remove_op(str_op)

    def test_execute_empty_workflow(self):
        ws = Workspace('/path', Workflow(OpMetaInfo('workspace_workflow', header=dict(description='Test!'))))
        ws.execute_workflow()

    def test_set_and_execute_step(self):
        ws = Workspace('/path', Workflow(OpMetaInfo('workspace_workflow', header=dict(description='Test!'))))

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
                            mk_op_kwargs(ds="@p", point="iih!", var="precipitation"), res_name='ts2', validate_args=True)
        self.assertEqual(str(e.exception), "input 'point' for operation 'cate.ops.timeseries.tseries_point': "
                                           "cannot convert value <iih!> to PointLike")

        ws2 = Workspace.from_json_dict(ws.to_json_dict())
        self.assertEqual(ws2.base_dir, ws.base_dir)
        self.assertEqual(ws2.workflow.op_meta_info.qualified_name, ws.workflow.op_meta_info.qualified_name)
        self.assertEqual(len(ws2.workflow.steps), len(ws.workflow.steps))
