import json
import os
import shutil
from unittest import TestCase

from ect.core.op import OpMetaInfo
from ect.core.workflow import Workflow
from ect.ui.workspace import WebAPIWorkspaceManager
from ect.ui.workspace import Workspace
from tornado.testing import AsyncHTTPTestCase
from ect.ui import webapi


class WebAPIWorkspaceManagerTest(AsyncHTTPTestCase):
    def get_app(self):
        return webapi.get_application()

    def test_init_workspace(self):
        base_dir = 'TESTOMAT'
        if os.path.exists(base_dir):
            shutil.rmtree(base_dir)

        workspace_manager = WebAPIWorkspaceManager(port=self.get_http_port(), http_client=self.http_client)
        workspace_manager.init_workspace(base_dir=base_dir)
        self.assertTrue(os.path.exists(base_dir))

        shutil.rmtree(base_dir)


class WorkspaceTest(TestCase):
    def test_example(self):
        expected_json_text = """{
            "qualified_name": "workspace_workflow",
            "header": {
                "description": "Test!"
            },
            "input": {},
            "output": {
                "p": {
                    "data_type": "xarray.core.dataset.Dataset",
                    "source": "p.return"
                },
                "ts": {
                    "data_type": "xarray.core.dataset.Dataset",
                    "source": "ts.return",
                    "description": "A timeseries dataset."
                }
            },
            "steps": [
                {
                    "id": "p",
                    "op": "ect.ops.io.read_netcdf",
                    "input": {
                        "file": {
                            "value": "2010_precipitation.nc"
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
                    "op": "ect.ops.timeseries.timeseries",
                    "input": {
                        "ds": {
                            "source": "p.return"
                        },
                        "lat": {
                            "value": 53
                        },
                        "lon": {
                            "value": 10
                        },
                        "method": {}
                    },
                    "output": {
                        "return": {}
                    }
                }
            ]
        }
        """

        expected_json_dict = json.loads(expected_json_text)

        ws = Workspace('/path', Workflow(OpMetaInfo('workspace_workflow', header_dict=dict(description='Test!'))))
        # print("wf_1: " + json.dumps(ws.workflow.to_json_dict(), indent='  '))
        ws.add_resource('p', 'ect.ops.io.read_netcdf', ["file=2010_precipitation.nc"])
        # print("wf_2: " + json.dumps(ws.workflow.to_json_dict(), indent='  '))
        ws.add_resource('ts', 'ect.ops.timeseries.timeseries', ["ds=p", "lat=53", "lon=10"])
        print("wf_3: " + json.dumps(ws.workflow.to_json_dict(), indent='  '))
        self.assertEqual(ws.workflow.to_json_dict(), expected_json_dict)
