import json
import os
import shutil
import subprocess
import sys
import unittest
import urllib.request

from ect.core.op import OpMetaInfo
from ect.core.workflow import Workflow
from ect.ui.workspace import WebAPIWorkspaceManager, FSWorkspaceManager, Workspace


# noinspection PyUnresolvedReferences
class WorkspaceManagerTestMixin:
    def new_workspace_manager(self):
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
        self.assertTrue(os.path.exists(base_dir))
        workspace2 = workspace_manager.get_workspace(base_dir=base_dir)
        self.assertTrue(os.path.exists(base_dir))

        self.assertEqual(workspace1.base_dir, workspace2.base_dir)
        self.assertEqual(workspace1.workflow.id, workspace2.workflow.id)

        self.del_base_dir(base_dir)


class FSWorkspaceManagerTest(WorkspaceManagerTestMixin, unittest.TestCase):
    def new_workspace_manager(self):
        return FSWorkspaceManager()


class WebAPIWorkspaceManagerTest(WorkspaceManagerTestMixin, unittest.TestCase):
    PORT = 8782

    def setUp(self):
        self.popen = subprocess.Popen([sys.executable, '-m', 'ect.ui.webapi', 'start', '-p', str(self.PORT)],
                                      shell=True)
        while True:
            try:
                urllib.request.urlopen('http://localhost:%s/' % self.PORT, timeout=1)
                break
            except:
                pass
        # print('WebAPI up')

    def tearDown(self):
        exit_code = subprocess.call([sys.executable, '-m', 'ect.ui.webapi', 'stop', '-p', str(self.PORT)], shell=True)
        if exit_code:
            self.popen.kill()
        # print('WebAPI down')

    def new_workspace_manager(self):
        return WebAPIWorkspaceManager(port=self.PORT, timeout=10)


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
