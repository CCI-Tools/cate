import json
import os
import shutil
import subprocess
import sys
import time
import unittest
import urllib.request

from ect.core.op import OpMetaInfo
from ect.ui.webapi import start_service_subprocess, stop_service_subprocess, find_free_port
from ect.core.workflow import Workflow
from ect.ui.workspace import WorkspaceManager, WebAPIWorkspaceManager, FSWorkspaceManager, Workspace


# noinspection PyUnresolvedReferences
class WorkspaceManagerTestMixin:
    def new_workspace_manager(self) -> WorkspaceManager:
        raise NotImplementedError

    def new_base_dir(self, base_dir):
        if os.path.exists(base_dir):
            shutil.rmtree(base_dir)
        return base_dir

    def del_base_dir(self, base_dir):
        shutil.rmtree(base_dir)

    def test_init_workspace(self):
        base_dir = self.new_base_dir('TESTOMAT1')

        workspace_manager = self.new_workspace_manager()
        workspace = workspace_manager.init_workspace(base_dir=base_dir)
        self.assertTrue(os.path.exists(base_dir))
        self.assertIsNotNone(workspace)

        self.del_base_dir(base_dir)

    def test_get_workspace(self):
        base_dir = self.new_base_dir('TESTOMAT2')

        workspace_manager = self.new_workspace_manager()
        workspace1 = workspace_manager.init_workspace(base_dir=base_dir)
        workspace2 = workspace_manager.get_workspace(base_dir=base_dir)

        self.assertEqual(workspace1.base_dir, workspace2.base_dir)
        self.assertEqual(workspace1.workflow.id, workspace2.workflow.id)

        self.del_base_dir(base_dir)

    def test_add_workspace_resource(self):
        base_dir = self.new_base_dir('TESTOMAT3')

        workspace_manager = self.new_workspace_manager()
        workspace1 = workspace_manager.init_workspace(base_dir=base_dir)
        self.assertTrue(os.path.exists(base_dir))
        workspace_manager.set_workspace_resource(base_dir=base_dir, res_name='SST',
                                                 op_name='ect.ops.io.read_netcdf', op_args=['file=SST.nc'])
        workspace2 = workspace_manager.get_workspace(base_dir=base_dir)

        self.assertEqual(workspace2.base_dir, workspace1.base_dir)
        self.assertEqual(workspace2.workflow.id, workspace1.workflow.id)
        sst_step = workspace2.workflow.find_node('SST')
        self.assertIsNotNone(sst_step)

        self.del_base_dir(base_dir)


class FSWorkspaceManagerTest(WorkspaceManagerTestMixin, unittest.TestCase):
    def new_workspace_manager(self):
        return FSWorkspaceManager()


@unittest.skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', None) == '1', 'ECT_DISABLE_WEB_TESTS = 1')
class WebAPIWorkspaceManagerTest(WorkspaceManagerTestMixin, unittest.TestCase):

    def setUp(self):
        self.port = find_free_port()
        exit_code = start_service_subprocess(port=self.port, caller='pytest')
        if exit_code:
            self.fail("failed to start WebAPI")

    def tearDown(self):
        exit_code = stop_service_subprocess(port=self.port, caller='pytest')
        if exit_code:
            self.webapi.kill()

    def new_workspace_manager(self):
        return WebAPIWorkspaceManager(port=self.port, timeout=2)


class WorkspaceTest(unittest.TestCase):
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
                    "description": "A timeseries dataset"
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
        ws.set_resource('p', 'ect.ops.io.read_netcdf', ["file=2010_precipitation.nc"])
        # print("wf_2: " + json.dumps(ws.workflow.to_json_dict(), indent='  '))
        ws.set_resource('ts', 'ect.ops.timeseries.timeseries', ["ds=p", "lat=53", "lon=10"])
        # print("wf_3: " + json.dumps(ws.workflow.to_json_dict(), indent='  '))
        self.assertEqual(ws.workflow.to_json_dict(), expected_json_dict)

        with self.assertRaises(ValueError) as e:
            ws.set_resource('ts2', 'ect.ops.timeseries.timeseries', ["ds=p", "lat=0", "lon=iih!"], validate_args=True)
        self.assertEqual(str(e.exception), "input 'lon' for operation 'ect.ops.timeseries.timeseries' "
                                           "must be of type 'float', but got type 'str'")
