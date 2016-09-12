import json
import os
import shutil
import subprocess
import sys
import unittest
import urllib.request
import time
from collections import OrderedDict
from ect.core.op import OpMetaInfo
from ect.core.workflow import Workflow
from ect.ui.workspace import WorkspaceManager, WebAPIWorkspaceManager, FSWorkspaceManager, Workspace, encode_path


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

    @classmethod
    def _find_free_port(cls):
        import socket
        s = socket.socket()
        # Bind to a free port provided by the host.
        s.bind(('', 0))
        free_port = s.getsockname()[1]
        s.close()
        # Return the port number assigned.
        return free_port

    def setUp(self):
        self.port = self._find_free_port()
        self.webapi = subprocess.Popen('"%s" -m ect.ui.webapi -p %d start' % (sys.executable, self.port),
                                       shell=True)

        webapi_url = 'http://127.0.0.1:%s/' % self.port
        while True:
            return_code = self.webapi.poll()
            if return_code is not None:
                if return_code:
                    self.fail("failed to start WebAPI")
                else:
                    break
            try:
                time.sleep(0.1)
                urllib.request.urlopen(webapi_url, timeout=2)
                break
            except Exception as e:
                # print(str(e))
                pass

    def tearDown(self):
        exit_code = subprocess.call('"%s" -m ect.ui.webapi -p %d stop' % (sys.executable, self.port), shell=True)
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
        ws.set_resource('p', 'ect.ops.io.read_netcdf', ["file=2010_precipitation.nc"])
        # print("wf_2: " + json.dumps(ws.workflow.to_json_dict(), indent='  '))
        ws.set_resource('ts', 'ect.ops.timeseries.timeseries', ["ds=p", "lat=53", "lon=10"])
        # print("wf_3: " + json.dumps(ws.workflow.to_json_dict(), indent='  '))
        self.assertEqual(ws.workflow.to_json_dict(), expected_json_dict)


class EncodePathTest(unittest.TestCase):
    def test_encode_path(self):
        self.assertEqual(encode_path('/ws/init',
                                     query_args=OrderedDict([('base_path', '/home/norman/workpaces'),
                                                             ('description', 'Hi there!')])),
                         '/ws/init?base_path=%2Fhome%2Fnorman%2Fworkpaces&description=Hi+there%21')
        self.assertEqual(encode_path('/ws/init',
                                     query_args=OrderedDict([('base_path', 'C:\\Users\\Norman\\workpaces'),
                                                             ('description', 'Hi there!')])),
                         '/ws/init?base_path=C%3A%5CUsers%5CNorman%5Cworkpaces&description=Hi+there%21')

        self.assertEqual(encode_path('/ws/get/{base_path}',
                                     path_args=dict(base_path='/home/norman/workpaces')),
                         '/ws/get/%2Fhome%2Fnorman%2Fworkpaces')
        self.assertEqual(encode_path('/ws/get/{base_path}',
                                     path_args=dict(base_path='C:\\Users\\Norman\\workpaces')),
                         '/ws/get/C%3A%5CUsers%5CNorman%5Cworkpaces')
