import json
import os
import unittest

from ect.core.op import OpMetaInfo
from ect.core.workflow import Workflow
from ect.ui.workspace import Workspace

NETCDF_TEST_FILE_1 = os.path.join(os.path.dirname(__file__), 'precip_and_temp.nc')
NETCDF_TEST_FILE_2 = os.path.join(os.path.dirname(__file__), 'precip_and_temp_2.nc')


class WorkspaceTest(unittest.TestCase):
    def test_set_and_execute_step(self):
        ws = Workspace('/path', Workflow(OpMetaInfo('workspace_workflow', header_dict=dict(description='Test!'))))

        ws.set_resource('X', 'ect.ops.io.read_netcdf', ["file=%s" % NETCDF_TEST_FILE_1])
        ws.set_resource('Y', 'ect.ops.timeseries.tseries_mean', ["ds=X", "var=precipitation"])
        self.assertEqual(ws.resource_cache, {})

        value = ws.execute_workflow('Y')
        self.assertIn('X', ws.resource_cache)
        self.assertIn('Y', ws.resource_cache)

        ws.set_resource('Y', 'ect.ops.timeseries.tseries_mean', ["ds=X", "var=temperature"], overwrite=True)
        self.assertIn('X', ws.resource_cache)
        self.assertNotIn('Y', ws.resource_cache)

        value = ws.execute_workflow('Y')
        self.assertIn('X', ws.resource_cache)
        self.assertIn('Y', ws.resource_cache)

        ws.set_resource('X', 'ect.ops.io.read_netcdf', ["file=%s" % NETCDF_TEST_FILE_2], overwrite=True)
        self.assertNotIn('X', ws.resource_cache)
        self.assertNotIn('Y', ws.resource_cache)

        value = ws.execute_workflow('Y')
        self.assertIn('X', ws.resource_cache)
        self.assertIn('Y', ws.resource_cache)

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
                    "op": "ect.ops.io.read_netcdf",
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
                    "op": "ect.ops.timeseries.tseries_mean",
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
        ws.set_resource('p', 'ect.ops.io.read_netcdf', ["file=%s" % NETCDF_TEST_FILE_1])
        # print("wf_2: " + json.dumps(ws.workflow.to_json_dict(), indent='  '))
        ws.set_resource('ts', 'ect.ops.timeseries.tseries_mean', ["ds=p", "var=precipitation"])
        print("wf_3: " + json.dumps(ws.workflow.to_json_dict(), indent='  '))
        self.assertEqual(ws.workflow.to_json_dict(), expected_json_dict)

        with self.assertRaises(ValueError) as e:
            ws.set_resource('ts2', 'ect.ops.timeseries.tseries_point',
                            ["ds=p", "lat=0", "lon=iih!", "var=precipitation"], validate_args=True)
        self.assertEqual(str(e.exception), "input 'lon' for operation 'ect.ops.timeseries.tseries_point' "
                                           "must be of type 'float', but got type 'str'")
