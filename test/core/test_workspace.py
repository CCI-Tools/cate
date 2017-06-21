import json
import os
import unittest

from cate.util import UNDEFINED
from cate.util.opmetainf import OpMetaInfo
from cate.core.workflow import Workflow
from cate.core.workspace import Workspace, mk_op_kwargs

NETCDF_TEST_FILE_1 = os.path.join(os.path.dirname(__file__), '..', 'data', 'precip_and_temp.nc')
NETCDF_TEST_FILE_2 = os.path.join(os.path.dirname(__file__), '..', 'data', 'precip_and_temp_2.nc')


class WorkspaceTest(unittest.TestCase):
    def test_workspace_is_part_of_context(self):

        def some_op(ctx: dict) -> dict:
            return dict(ctx)

        from cate.core.op import OP_REGISTRY

        try:
            op_reg = OP_REGISTRY.add_op(some_op)
            op_reg.op_meta_info.input['ctx']['context'] = True

            ws = Workspace('/path', Workflow(OpMetaInfo('workspace_workflow', header_dict=dict(description='Test!'))))
            ws.set_resource('new_ctx', op_reg.op_meta_info.qualified_name, {})
            ws.execute_workflow('new_ctx')

            self.assertTrue('new_ctx' in ws.resource_cache)
            self.assertTrue('workspace' in ws.resource_cache['new_ctx'])
            self.assertIs(ws.resource_cache['new_ctx']['workspace'], ws)

        finally:
            OP_REGISTRY.remove_op(some_op)

    def test_execute_empty_workflow(self):
        ws = Workspace('/path', Workflow(OpMetaInfo('workspace_workflow', header_dict=dict(description='Test!'))))
        ws.execute_workflow()

    def test_set_and_execute_step(self):
        ws = Workspace('/path', Workflow(OpMetaInfo('workspace_workflow', header_dict=dict(description='Test!'))))

        ws.set_resource('X', 'cate.ops.io.read_netcdf', mk_op_kwargs(file=NETCDF_TEST_FILE_1))
        ws.set_resource('Y', 'cate.ops.timeseries.tseries_mean', mk_op_kwargs(ds="@X", var="precipitation"))
        self.assertEqual(ws.resource_cache, {})

        ws.execute_workflow('Y')
        self.assertIn('X', ws.resource_cache)
        self.assertIn('Y', ws.resource_cache)

        ws.set_resource('Y', 'cate.ops.timeseries.tseries_mean', mk_op_kwargs(ds="@X", var="temperature"),
                        overwrite=True)
        self.assertIn('X', ws.resource_cache)
        self.assertIn('Y', ws.resource_cache)
        self.assertIs(ws.resource_cache['Y'], UNDEFINED)

        ws.execute_workflow('Y')
        self.assertIn('X', ws.resource_cache)
        self.assertIn('Y', ws.resource_cache)

        ws.set_resource('X', 'cate.ops.io.read_netcdf', mk_op_kwargs(file=NETCDF_TEST_FILE_2),
                        overwrite=True)
        self.assertIn('X', ws.resource_cache)
        self.assertIs(ws.resource_cache['X'], UNDEFINED)
        self.assertIn('Y', ws.resource_cache)
        self.assertIs(ws.resource_cache['Y'], UNDEFINED)

        ws.execute_workflow('Y')
        self.assertIn('X', ws.resource_cache)
        self.assertIn('Y', ws.resource_cache)

    def test_set_and_rename_and_execute_step(self):
        ws = Workspace('/path', Workflow(OpMetaInfo('workspace_workflow', header_dict=dict(description='Test!'))))

        ws.set_resource('X', 'cate.ops.utility.identity', mk_op_kwargs(value=1))
        ws.set_resource('Y', 'cate.ops.utility.identity', mk_op_kwargs(value="@X"))
        ws.set_resource('Z', 'cate.ops.utility.identity', mk_op_kwargs(value="@X"))
        self.assertEqual(len(ws.workflow.steps), 3)
        self.assertEqual(ws.resource_cache, {})

        print('----------------------------------')
        value = ws.execute_workflow('Y')
        self.assertEqual(value, 1)
        self.assertEqual(ws.resource_cache.get('X'), 1)
        self.assertEqual(ws.resource_cache.get('Y'), 1)
        self.assertEqual(ws.resource_cache.get('Z'), None)

        print('----------------------------------')
        value = ws.execute_workflow('Z')
        self.assertEqual(value, 1)
        self.assertEqual(ws.resource_cache.get('X'), 1)
        self.assertEqual(ws.resource_cache.get('Y'), 1)
        self.assertEqual(ws.resource_cache.get('Z'), 1)

        print('----X------------------------------')
        ws.set_resource('X', 'cate.ops.utility.identity', mk_op_kwargs(value=9), overwrite=True)
        self.assertEqual(len(ws.workflow.steps), 3)
        self.assertEqual(ws.resource_cache.get('X'), UNDEFINED)
        self.assertEqual(ws.resource_cache.get('Y'), UNDEFINED)
        self.assertEqual(ws.resource_cache.get('Z'), UNDEFINED)

        print('----Y------------------------------')
        ws.execute_workflow()
        self.assertEqual(ws.resource_cache.get('X'), 9)
        self.assertEqual(ws.resource_cache.get('Y'), 9)
        self.assertEqual(ws.resource_cache.get('Z'), 9)

        print('----------------------------------')
        ws.rename_resource('X', 'A')
        self.assertIsNone(ws.workflow.find_node('X'))
        self.assertIsNotNone(ws.workflow.find_node('A'))
        self.assertEqual(ws.resource_cache.get('X', '--'), '--')
        self.assertEqual(ws.resource_cache.get('A'), 9)
        self.assertEqual(ws.resource_cache.get('Y'), 9)
        self.assertEqual(ws.resource_cache.get('Z'), 9)

        print('----------------------------------')
        ws.set_resource('A', 'cate.ops.utility.identity', mk_op_kwargs(value=5), overwrite=True)
        self.assertEqual(ws.resource_cache.get('X', '--'), '--')
        self.assertEqual(ws.resource_cache.get('A'), UNDEFINED)
        self.assertEqual(ws.resource_cache.get('Y'), UNDEFINED)
        self.assertEqual(ws.resource_cache.get('Z'), UNDEFINED)

        print('----------------------------------')
        ws.execute_workflow()
        self.assertEqual(ws.resource_cache.get('X', '--'), '--')
        self.assertEqual(ws.resource_cache.get('A'), 5)
        self.assertEqual(ws.resource_cache.get('Y'), 5)
        self.assertEqual(ws.resource_cache.get('Z'), 5)

    def test_example(self):
        expected_json_text = """{
            "qualified_name": "workspace_workflow",
            "header": {
                "description": "Test!"
            },
            "input": {},
            "output": {},
            "steps": [
                {
                    "id": "p",
                    "op": "cate.ops.io.read_netcdf",
                    "input": {
                        "file": {
                            "value": "%s"
                        },
                        "drop_variables": {},
                        "decode_cf": {},
                        "decode_times": {},
                        "engine": {}
                    },
                    "output": {
                        "return": {}
                    }
                },
                {
                    "id": "ts",
                    "op": "cate.ops.timeseries.tseries_mean",
                    "input": {
                        "ds": {
                            "source": "p.return"
                        },
                        "var": {
                          "value": "precipitation"
                        },
                        "std_suffix": {},
                        "calculate_std": {}
                    },
                    "output": {
                        "return": {}
                    }
                }
            ]
        }
        """ % NETCDF_TEST_FILE_1.replace('\\', '\\\\')

        expected_json_dict = json.loads(expected_json_text)

        ws = Workspace('/path', Workflow(OpMetaInfo('workspace_workflow', header_dict=dict(description='Test!'))))
        # print("wf_1: " + json.dumps(ws.workflow.to_json_dict(), indent='  '))
        ws.set_resource('p', 'cate.ops.io.read_netcdf', mk_op_kwargs(file=NETCDF_TEST_FILE_1))
        # print("wf_2: " + json.dumps(ws.workflow.to_json_dict(), indent='  '))
        ws.set_resource('ts', 'cate.ops.timeseries.tseries_mean', mk_op_kwargs(ds="@p", var="precipitation"))
        print("wf_3: " + json.dumps(ws.workflow.to_json_dict(), indent='  '))
        self.assertEqual(ws.workflow.to_json_dict(), expected_json_dict)

        with self.assertRaises(ValueError) as e:
            ws.set_resource('ts2', 'cate.ops.timeseries.tseries_point',
                            mk_op_kwargs(ds="@p", point="iih!", var="precipitation"), validate_args=True)
        self.assertEqual(str(e.exception), "input 'point' for operation 'cate.ops.timeseries.tseries_point': "
                                           "cannot convert value <iih!> to PointLike")
