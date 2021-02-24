import os
import platform
import shutil
import tempfile
import unittest

from cate.core.workspace import mk_op_kwargs
from cate.core.wsmanag import WorkspaceManager, FSWorkspaceManager
from ..util.test_monitor import RecordingMonitor

NETCDF_TEST_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'precip_and_temp.nc')


# noinspection PyUnresolvedReferences
class WorkspaceManagerTestMixin:

    def new_workspace_manager(self) -> WorkspaceManager:
        raise NotImplementedError

    def new_workspace_dir(self, ws_name: str):
        ws_dir = os.path.join(self._root_path, ws_name)
        if os.path.exists(ws_dir):
            shutil.rmtree(ws_dir)
        return ws_dir

    def del_workspace_dir(self, ws_name: str):
        ws_dir = os.path.join(self._root_path, ws_name)
        if os.path.exists(ws_dir) and os.path.isdir(ws_dir):
            shutil.rmtree(ws_dir)

    def test_new_workspace(self):
        ws_dir = self.new_workspace_dir('TESTOMAT')

        workspace_manager = self.new_workspace_manager()
        workspace1 = workspace_manager.new_workspace(ws_dir)
        workspace2 = workspace_manager.get_workspace(ws_dir)

        self.assertEqual(workspace1.base_dir, workspace2.base_dir)
        self.assertEqual(workspace1.workflow.id, workspace2.workflow.id)

    def test_new_workspace_in_existing_dir(self):
        ws_dir = self.new_workspace_dir('TESTOMAT')

        workspace_manager = self.new_workspace_manager()
        workspace1 = workspace_manager.new_workspace(ws_dir)
        self.assertIsNotNone(workspace1)

        workspace2 = workspace_manager.get_workspace(ws_dir)
        workspace_manager.save_workspace(ws_dir)
        self.assertIsNotNone(workspace2)

        workspace_manager.delete_workspace(ws_dir)

        workspace1 = workspace_manager.new_workspace(ws_dir)
        workspace2 = workspace_manager.get_workspace(ws_dir)

        self.assertEqual(workspace1.base_dir, workspace2.base_dir)
        self.assertEqual(workspace1.workflow.id, workspace2.workflow.id)

    def test_list_workspace_names_dir_not_existing(self):
        workspace_manager = self.new_workspace_manager()

        ws_names_list = workspace_manager.list_workspace_names()
        self.assertEqual(0, len(ws_names_list))

    def test_list_workspace_names(self):
        workspace_manager = self.new_workspace_manager()

        workspace = workspace_manager.new_workspace('TESTOMAT-1')
        workspace.save()

        workspace = workspace_manager.new_workspace('TESTOMAT-2')
        workspace.save()

        ws_names_list = workspace_manager.list_workspace_names()
        self.assertEqual(['TESTOMAT-1', 'TESTOMAT-2'], sorted(ws_names_list))

    def test_save_workspace_as(self):
        ws_dir = self.new_workspace_dir('TESTOMAT')
        new_ws_dir = self.new_workspace_dir('TESTOMAT2')

        workspace_manager = self.new_workspace_manager()
        workspace = workspace_manager.new_workspace(ws_dir)
        self.assertIsNotNone(workspace)
        workspace_manager.save_workspace(ws_dir)
        self.assertTrue(os.path.exists(ws_dir))

        workspace_manager.save_workspace_as(ws_dir, new_ws_dir)
        self.assertTrue(os.path.exists(new_ws_dir))

        self.del_workspace_dir(ws_dir)
        self.del_workspace_dir(new_ws_dir)

    def test_new_save_as_delete_new_workspace(self):
        ws_dir = self.new_workspace_dir('TESTOMAT')
        new_ws_dir = self.new_workspace_dir('TESTOMAT2')

        workspace_manager = self.new_workspace_manager()
        workspace = workspace_manager.new_workspace(ws_dir)
        self.assertIsNotNone(workspace)
        workspace_manager.save_workspace(ws_dir)
        self.assertTrue(os.path.exists(ws_dir))

        workspace_manager.save_workspace_as(ws_dir, new_ws_dir)
        self.assertTrue(os.path.exists(new_ws_dir))

        workspace_manager.delete_workspace(new_ws_dir)
        workspace_2 = workspace_manager.new_workspace(new_ws_dir)
        self.assertIsNotNone(workspace_2)
        workspace_manager.save_workspace(new_ws_dir)

        self.del_workspace_dir(ws_dir)
        self.del_workspace_dir(new_ws_dir)

    def test_new_scratch_workspace_and_save(self):
        workspace_manager = self.new_workspace_manager()

        workspace = workspace_manager.new_workspace(None)
        self.assertIsNotNone(workspace)
        self.assertTrue(workspace.is_scratch)
        workspace_manager.save_workspace(workspace.base_dir)
        self.assertTrue(os.path.exists(workspace.workspace_data_dir))

        self.del_workspace_dir(workspace.base_dir)

    def test_new_named_workspace_and_save(self):
        workspace_manager = self.new_workspace_manager()

        workspace = workspace_manager.new_workspace('TESTOMAT')
        self.assertIsNotNone(workspace)
        self.assertFalse(workspace.is_scratch)
        workspace.save()
        self.assertTrue(os.path.exists(workspace.workspace_data_dir))

        self.del_workspace_dir(workspace.base_dir)

    def test_delete_workspace(self):
        ws_dir = self.new_workspace_dir('TESTOMAT')

        workspace_manager = self.new_workspace_manager()

        workspace = workspace_manager.new_workspace(ws_dir)
        workspace.save()
        self.assertTrue(os.path.exists(ws_dir))
        self.assertTrue(os.path.exists(os.path.join(ws_dir, '.cate-workspace')))

        workspace_manager.delete_workspace(ws_dir)
        self.assertTrue(os.path.exists(ws_dir))
        self.assertFalse(os.path.exists(os.path.join(ws_dir, '.cate-workspace')))

        self.del_workspace_dir(ws_dir)

    def test_delete_workspace_completely(self):
        ws_dir = self.new_workspace_dir('TESTOMAT')

        workspace_manager = self.new_workspace_manager()

        workspace = workspace_manager.new_workspace(ws_dir)
        workspace.save()
        self.assertTrue(os.path.exists(ws_dir))
        self.assertTrue(os.path.exists(os.path.join(ws_dir, '.cate-workspace')))
        self.assertIsNotNone(workspace)

        workspace_manager.delete_workspace(ws_dir, True)
        self.assertFalse(os.path.exists(ws_dir))

        self.del_workspace_dir(ws_dir)

    def test_set_workspace_resource(self):
        ws_dir = self.new_workspace_dir('TESTOMAT')

        workspace_manager = self.new_workspace_manager()
        workspace1 = workspace_manager.new_workspace(ws_dir)
        workspace1.save()
        self.assertTrue(os.path.exists(ws_dir))
        workspace_manager.set_workspace_resource(ws_dir,
                                                 'cate.ops.io.read_netcdf',
                                                 mk_op_kwargs(file=NETCDF_TEST_FILE),
                                                 res_name='SST')
        workspace2 = workspace_manager.get_workspace(ws_dir)

        self.assertEqual(workspace2.base_dir, workspace1.base_dir)
        self.assertEqual(workspace2.workflow.id, workspace1.workflow.id)
        sst_step = workspace2.workflow.find_node('SST')
        self.assertIsNotNone(sst_step)

        self.del_workspace_dir(ws_dir)

    def test_clean_workspace(self):
        ws_dir = self.new_workspace_dir('TESTOMAT')

        workspace_manager = self.new_workspace_manager()
        workspace1 = workspace_manager.new_workspace(ws_dir, description='test clean workspace')
        self.assertEqual(workspace1.workflow.op_meta_info.header.get('description', None), 'test clean workspace')

        workspace1.save()
        self.assertTrue(os.path.exists(ws_dir))
        workspace_manager.set_workspace_resource(ws_dir,
                                                 op_name='cate.ops.io.read_netcdf',
                                                 op_args=dict(file=dict(value=NETCDF_TEST_FILE)),
                                                 res_name='SST')
        workspace2 = workspace_manager.get_workspace(ws_dir)

        self.assertEqual(workspace2.base_dir, workspace1.base_dir)
        self.assertEqual(workspace2.workflow.id, workspace1.workflow.id)
        sst_step = workspace2.workflow.find_node('SST')
        self.assertIsNotNone(sst_step)

        workspace_manager.clean_workspace(ws_dir)
        workspace3 = workspace_manager.get_workspace(ws_dir)
        steps = workspace3.workflow.steps
        # Test that all steps & resources are removed
        self.assertEqual(steps, [])
        # Test that header info is kept
        self.assertEqual(workspace3.workflow.op_meta_info.header.get('description', None), 'test clean workspace')

        self.del_workspace_dir(ws_dir)

    def test_resource_progress(self):
        ws_dir = self.new_workspace_dir('TESTOMAT')

        workspace_manager = self.new_workspace_manager()
        workspace1 = workspace_manager.new_workspace(ws_dir)
        workspace1.save()
        self.assertTrue(os.path.exists(ws_dir))
        rm = RecordingMonitor()
        workspace_manager.set_workspace_resource(ws_dir,
                                                 'cate.ops.utility.no_op',
                                                 dict(num_steps=dict(value=10)),
                                                 res_name='noop',
                                                 monitor=rm)
        # the websocket clients send always progress with 'None' arguments after start
        cleaned_records = [r for r in rm.records if len(r) < 2 or r[1] is not None]
        self.assertEqual([
            ('start', 'Computing nothing', 10),
            ('progress', 1.0, 'Step 1 of 10 doing nothing', 10),
            ('progress', 1.0, 'Step 2 of 10 doing nothing', 20),
            ('progress', 1.0, 'Step 3 of 10 doing nothing', 30),
            ('progress', 1.0, 'Step 4 of 10 doing nothing', 40),
            ('progress', 1.0, 'Step 5 of 10 doing nothing', 50),
            ('progress', 1.0, 'Step 6 of 10 doing nothing', 60),
            ('progress', 1.0, 'Step 7 of 10 doing nothing', 70),
            ('progress', 1.0, 'Step 8 of 10 doing nothing', 80),
            ('progress', 1.0, 'Step 9 of 10 doing nothing', 90),
            ('progress', 1.0, 'Step 10 of 10 doing nothing', 100),
            ('done',)
        ], cleaned_records)
        self.del_workspace_dir(ws_dir)

    def test_session(self):
        ws_dir = self.new_workspace_dir('TESTOMAT')

        workspace_manager = self.new_workspace_manager()
        workspace1 = workspace_manager.new_workspace(ws_dir, description='session workspace')
        self.assertIsNotNone(workspace1)
        self.assertEqual(workspace1.base_dir, ws_dir)
        self.assertEqual(workspace1.workflow.op_meta_info.header.get('description', None), 'session workspace')
        workspace_manager.save_workspace(ws_dir)
        self.assertTrue(os.path.exists(ws_dir))

        res_name = 'ds'
        workspace_manager.set_workspace_resource(ws_dir,
                                                 'cate.ops.io.read_netcdf',
                                                 dict(file=dict(value=NETCDF_TEST_FILE)),
                                                 res_name=res_name)
        workspace2 = workspace_manager.get_workspace(ws_dir)

        self.assertEqual(workspace2.base_dir, ws_dir)
        self.assertEqual(workspace2.workflow.id, workspace1.workflow.id)
        self.assertEqual(len(workspace2.workflow.steps), 1)

        sst_step = workspace2.workflow.find_node(res_name)
        self.assertIsNotNone(sst_step)

        file_path = os.path.join(ws_dir, 'precip_and_temp_copy.nc')
        self.assertFalse(os.path.isfile(file_path))
        workspace_manager.write_workspace_resource(ws_dir,
                                                   res_name,
                                                   file_path=file_path)
        self.assertTrue(os.path.isfile(file_path))

        file_path = os.path.join(ws_dir, 'precip_and_temp_copy_copy.nc')
        self.assertFalse(os.path.isfile(file_path))
        workspace_manager.run_op_in_workspace(ws_dir,
                                              'write_netcdf4',
                                              mk_op_kwargs(obj='@ds',
                                                           file=file_path))
        self.assertTrue(os.path.isfile(file_path))

        workspaces = workspace_manager.get_open_workspaces()
        self.assertIsNotNone(workspaces)
        self.assertEqual(len(workspaces), 1)
        self.assertEqual(workspaces[0].base_dir, ws_dir)

        workspace3 = workspace_manager.delete_workspace_resource(ws_dir, res_name)
        self.assertEqual(len(workspace3.workflow.steps), 0)

        self.del_workspace_dir(ws_dir)

    def test_persistence(self):
        ws_dir = self.new_workspace_dir('TESTOMAT')

        workspace_manager = self.new_workspace_manager()
        workspace_manager.new_workspace(ws_dir)
        workspace_manager.save_workspace(ws_dir)
        self.assertTrue(os.path.exists(ws_dir))

        workspace_manager.set_workspace_resource(ws_dir,
                                                 'cate.ops.io.read_netcdf',
                                                 dict(file=dict(value=NETCDF_TEST_FILE)),
                                                 res_name='ds')
        workspace1, _ = workspace_manager.set_workspace_resource(ws_dir,
                                                                 'cate.ops.timeseries.tseries_mean',
                                                                 mk_op_kwargs(ds='@ds', var='temperature'),
                                                                 res_name='ts')
        self.assertEqual(workspace1.base_dir, ws_dir)
        self.assertEqual(len(workspace1.workflow.steps), 2)
        self.assertFalse(workspace1.workflow.find_node('ds').persistent)
        self.assertFalse(workspace1.workflow.find_node('ts').persistent)
        ts_file_path = os.path.abspath(os.path.join(self._root_path, 'TESTOMAT', '.cate-workspace', 'ts.nc'))
        self.assertFalse(os.path.isfile(ts_file_path))

        workspace3 = workspace_manager.set_workspace_resource_persistence(ws_dir, 'ts', True)
        self.assertFalse(workspace3.workflow.find_node('ds').persistent)
        self.assertTrue(workspace3.workflow.find_node('ts').persistent)
        workspace4 = workspace_manager.set_workspace_resource_persistence(ws_dir, 'ts', True)
        self.assertFalse(workspace4.workflow.find_node('ds').persistent)
        self.assertTrue(workspace4.workflow.find_node('ts').persistent)

        workspace_manager.save_workspace(ws_dir)
        self.assertTrue(os.path.isfile(ts_file_path))

        workspace_manager.close_workspace(ws_dir)
        self.assertEqual(len(workspace_manager.get_open_workspaces()), 0)
        self.assertTrue(os.path.isfile(ts_file_path))

        workspace5 = workspace_manager.open_workspace(ws_dir)
        self.assertEqual(workspace4.workflow.to_json_dict(), workspace5.workflow.to_json_dict())
        workspace_manager.set_workspace_resource_persistence(ws_dir, 'ts', False)
        workspace_manager.close_workspace(ws_dir)
        workspace_manager.close_workspace(ws_dir)  # closing a 2nd time should give no error
        self.assertFalse(os.path.isfile(ts_file_path))

        self.del_workspace_dir(ws_dir)


class FSWorkspaceManagerTest(WorkspaceManagerTestMixin, unittest.TestCase):

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self._root_path = tempfile.mkdtemp()

    def __del__(self):
        shutil.rmtree(self._root_path,
                      ignore_errors=True,
                      onerror=lambda e: print(f'FSWorkspaceManagerTest: error: {e}'))

    def new_workspace_manager(self):
        return FSWorkspaceManager(self._root_path)

    def test_resolve_path(self):
        # manager with root_path
        ws_manag = self.new_workspace_manager()
        self.assertEqual(self._root_path, ws_manag.root_path)
        expected_path = os.path.join(self._root_path, 'data')
        self.assertEqual(expected_path,
                         ws_manag.resolve_path('data'))
        self.assertEqual(expected_path,
                         ws_manag.resolve_path('/data'))
        self.assertEqual(expected_path,
                         ws_manag.resolve_path(os.path.sep + 'data'))

        # manager without root_path
        ws_manag = FSWorkspaceManager()
        self.assertEqual(None, ws_manag.root_path)
        self.assertEqual(os.path.abspath('data'),
                         ws_manag.resolve_path(os.path.abspath('data')))
        self.assertEqual(os.path.abspath('data'),
                         ws_manag.resolve_path('data'))

    def test_resolve_path_permits_access(self):
        ws_manag = self.new_workspace_manager()

        # absolute path escapes root_path
        with self.assertRaises(ValueError) as cm:
            ws_manag.resolve_path(os.path.join('..', 'data'))
        self.assertTrue(f'{cm.exception}'.startswith('access denied: '))

        # relative path escapes root_path
        with self.assertRaises(ValueError) as cm:
            ws_manag.resolve_path(os.path.join(os.path.sep, '..', 'data'))
        self.assertTrue(f'{cm.exception}'.startswith('access denied: '))

    def test_resolve_workspace_dir(self):
        ws_manag = self.new_workspace_manager()
        self.assertEqual(os.path.join(ws_manag.root_path, 'workspaces', 'test-1'),
                         ws_manag.resolve_workspace_dir('test-1'))
        self.assertEqual(os.path.join(ws_manag.root_path, 'my_workspaces', 'test-1'),
                         ws_manag.resolve_workspace_dir(os.path.join('my_workspaces', 'test-1')))
        self.assertEqual(os.path.join(ws_manag.root_path, 'my_workspaces', 'test-1'),
                         ws_manag.resolve_workspace_dir(os.path.join(os.path.sep, 'my_workspaces', 'test-1')))
