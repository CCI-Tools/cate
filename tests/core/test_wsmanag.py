import unittest

import os
import shutil
import tempfile

from cate.core.pathmanag import PathManager
from cate.core.workspace import mk_op_kwargs
from cate.core.wsmanag import WorkspaceManager, FSWorkspaceManager, RelativeFSWorkspaceManager
from ..util.test_monitor import RecordingMonitor

NETCDF_TEST_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'precip_and_temp.nc')


# noinspection PyUnresolvedReferences
class WorkspaceManagerTestMixin:

    def new_workspace_manager(self) -> WorkspaceManager:
        raise NotImplementedError

    def new_base_dir(self, base_dir):
        if self._is_relative:
            abs_dir = self._path_manager.resolve(base_dir)
            if os.path.exists(abs_dir):
                shutil.rmtree(abs_dir)
            base_dir = abs_dir
        else:
            base_dir = os.path.join(self._root_dir, base_dir)
            if os.path.exists(base_dir):
                shutil.rmtree(base_dir)

        return base_dir

    def del_base_dir(self, base_dir):
        shutil.rmtree(base_dir)

    def test_new_workspace(self):
        base_dir = self.new_base_dir('TESTOMAT')

        workspace_manager = self.new_workspace_manager()
        workspace1 = workspace_manager.new_workspace(base_dir)
        workspace2 = workspace_manager.get_workspace(base_dir)

        self.assertEqual(workspace1.base_dir, workspace2.base_dir)
        self.assertEqual(workspace1.workflow.id, workspace2.workflow.id)

    def test_list_workspace_names_dir_not_existing(self):
        workspace_manager = self.new_workspace_manager()

        ws_names_list = workspace_manager.list_workspace_names()
        self.assertEqual(0, len(ws_names_list))

    def test_list_workspace_names(self):
        base_dir = 'TESTOMAT'

        workspace_manager = self.new_workspace_manager()
        workspace = workspace_manager.new_workspace(base_dir)
        workspace.save()

        ws_names_list = workspace_manager.list_workspace_names()
        self.assertEqual(1, len(ws_names_list))
        self.assertEqual("TESTOMAT", ws_names_list[0])

    def test_new_save_workspace(self):
        base_dir = self.new_base_dir('TESTOMAT')
        to_dir = self.new_base_dir('TESTOMAT2')

        workspace_manager = self.new_workspace_manager()
        workspace = workspace_manager.new_workspace(base_dir)
        workspace_manager.save_workspace(base_dir)
        self.assertTrue(os.path.exists(base_dir))
        self.assertIsNotNone(workspace)

        workspace_manager.save_workspace_as(base_dir, to_dir)
        self.assertTrue(os.path.exists(to_dir))

        self.del_base_dir(base_dir)
        self.del_base_dir(to_dir)

    def test_new_save_workspace_relative_path(self):
        base_dir = self.new_base_dir('workspaces/TESTOMAT')
        to_dir = self.new_base_dir('workspaces/TESTOMAT2')

        workspace_manager = self.new_workspace_manager()
        workspace = workspace_manager.new_workspace('TESTOMAT')
        workspace_manager.save_workspace(base_dir)
        self.assertTrue(os.path.exists(base_dir))
        self.assertIsNotNone(workspace)

        workspace_manager.save_workspace_as(base_dir, to_dir)
        self.assertTrue(os.path.exists(to_dir))

        self.del_base_dir(base_dir)
        self.del_base_dir(to_dir)

    def test_new_save_scratch_workspace_relative_path(self):
        workspace_manager = self.new_workspace_manager()

        workspace = workspace_manager.new_workspace(None)
        self.assertIsNotNone(workspace)
        workspace_manager.save_workspace(workspace.base_dir)
        self.assertTrue(os.path.exists(workspace.base_dir))

        self.del_base_dir(workspace.base_dir)

    def test_delete_workspace(self):
        base_dir = self.new_base_dir('TESTOMAT')

        workspace_manager = self.new_workspace_manager()

        workspace = workspace_manager.new_workspace(base_dir)
        workspace.save()
        self.assertTrue(os.path.exists(base_dir))
        self.assertTrue(os.path.exists(os.path.join(base_dir, '.cate-workspace')))
        self.assertIsNotNone(workspace)

        workspace_manager.delete_workspace(base_dir)
        self.assertTrue(os.path.exists(base_dir))
        self.assertFalse(os.path.exists(os.path.join(base_dir, '.cate-workspace')))
        self.assertIsNotNone(workspace)

        self.del_base_dir(base_dir)

    def test_set_workspace_resource(self):
        base_dir = self.new_base_dir('TESTOMAT')

        workspace_manager = self.new_workspace_manager()
        workspace1 = workspace_manager.new_workspace(base_dir)
        workspace1.save()
        self.assertTrue(os.path.exists(base_dir))
        workspace_manager.set_workspace_resource(base_dir,
                                                 'cate.ops.io.read_netcdf',
                                                 mk_op_kwargs(file=NETCDF_TEST_FILE),
                                                 res_name='SST')
        workspace2 = workspace_manager.get_workspace(base_dir)

        self.assertEqual(workspace2.base_dir, workspace1.base_dir)
        self.assertEqual(workspace2.workflow.id, workspace1.workflow.id)
        sst_step = workspace2.workflow.find_node('SST')
        self.assertIsNotNone(sst_step)

        self.del_base_dir(base_dir)

    def test_clean_workspace(self):
        base_dir = self.new_base_dir('TESTOMAT')

        workspace_manager = self.new_workspace_manager()
        workspace1 = workspace_manager.new_workspace(base_dir, description='test clean workspace')
        self.assertEqual(workspace1.workflow.op_meta_info.header.get('description', None), 'test clean workspace')

        workspace1.save()
        self.assertTrue(os.path.exists(base_dir))
        workspace_manager.set_workspace_resource(base_dir,
                                                 op_name='cate.ops.io.read_netcdf',
                                                 op_args=dict(file=dict(value=NETCDF_TEST_FILE)),
                                                 res_name='SST')
        workspace2 = workspace_manager.get_workspace(base_dir)

        self.assertEqual(workspace2.base_dir, workspace1.base_dir)
        self.assertEqual(workspace2.workflow.id, workspace1.workflow.id)
        sst_step = workspace2.workflow.find_node('SST')
        self.assertIsNotNone(sst_step)

        workspace_manager.clean_workspace(base_dir)
        workspace3 = workspace_manager.get_workspace(base_dir)
        steps = workspace3.workflow.steps
        # Test that all steps & resources are removed
        self.assertEqual(steps, [])
        # Test that header info is kept
        self.assertEqual(workspace3.workflow.op_meta_info.header.get('description', None), 'test clean workspace')

        self.del_base_dir(base_dir)

    def test_resource_progress(self):
        base_dir = self.new_base_dir('TESTOMAT')

        workspace_manager = self.new_workspace_manager()
        workspace1 = workspace_manager.new_workspace(base_dir)
        workspace1.save()
        self.assertTrue(os.path.exists(base_dir))
        rm = RecordingMonitor()
        workspace_manager.set_workspace_resource(base_dir,
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
        self.del_base_dir(base_dir)

    def test_session(self):
        rel_path = 'TESTOMAT'
        base_dir = self.new_base_dir(rel_path)

        workspace_manager = self.new_workspace_manager()
        workspace1 = workspace_manager.new_workspace(base_dir, description='session workspace')
        self.assertIsNotNone(workspace1)
        self.assertEqual(workspace1.base_dir, base_dir)
        self.assertEqual(workspace1.workflow.op_meta_info.header.get('description', None), 'session workspace')
        workspace_manager.save_workspace(base_dir)
        self.assertTrue(os.path.exists(base_dir))

        res_name = 'ds'
        workspace_manager.set_workspace_resource(base_dir,
                                                 'cate.ops.io.read_netcdf',
                                                 dict(file=dict(value=NETCDF_TEST_FILE)),
                                                 res_name=res_name)
        workspace2 = workspace_manager.get_workspace(base_dir)

        self.assertEqual(workspace2.base_dir, base_dir)
        self.assertEqual(workspace2.workflow.id, workspace1.workflow.id)
        self.assertEqual(len(workspace2.workflow.steps), 1)

        sst_step = workspace2.workflow.find_node(res_name)
        self.assertIsNotNone(sst_step)

        file_path = os.path.abspath(os.path.join(self._root_dir, 'TESTOMAT', 'precip_and_temp_copy.nc'))
        workspace_manager.write_workspace_resource(base_dir, res_name, file_path=file_path)
        self.assertTrue(os.path.isfile(file_path))

        run_file_path = os.path.abspath(os.path.join(self._root_dir, 'TESTOMAT', 'precip_and_temp_runcopy.nc'))
        workspace_manager.run_op_in_workspace(base_dir, 'write_netcdf4', mk_op_kwargs(obj='@ds', file=run_file_path))
        self.assertTrue(os.path.isfile(run_file_path))

        workspaces = workspace_manager.get_open_workspaces()
        self.assertIsNotNone(workspaces)
        self.assertEqual(len(workspaces), 1)
        self.assertEqual(workspaces[0].base_dir, base_dir)

        workspace3 = workspace_manager.delete_workspace_resource(base_dir, res_name)
        self.assertEqual(len(workspace3.workflow.steps), 0)

        self.del_base_dir(base_dir)

    def test_persistence(self):
        rel_path = 'TESTOMAT'
        base_dir = self.new_base_dir(rel_path)

        workspace_manager = self.new_workspace_manager()
        workspace_manager.new_workspace(base_dir)
        workspace_manager.save_workspace(base_dir)
        self.assertTrue(os.path.exists(base_dir))

        workspace_manager.set_workspace_resource(base_dir,
                                                 'cate.ops.io.read_netcdf',
                                                 dict(file=dict(value=NETCDF_TEST_FILE)),
                                                 res_name='ds')
        workspace1, _ = workspace_manager.set_workspace_resource(base_dir,
                                                                 'cate.ops.timeseries.tseries_mean',
                                                                 mk_op_kwargs(ds='@ds', var='temperature'),
                                                                 res_name='ts')
        self.assertEqual(workspace1.base_dir, base_dir)
        self.assertEqual(len(workspace1.workflow.steps), 2)
        self.assertFalse(workspace1.workflow.find_node('ds').persistent)
        self.assertFalse(workspace1.workflow.find_node('ts').persistent)
        ts_file_path = os.path.abspath(os.path.join(self._root_dir, 'TESTOMAT', '.cate-workspace', 'ts.nc'))
        self.assertFalse(os.path.isfile(ts_file_path))

        workspace3 = workspace_manager.set_workspace_resource_persistence(base_dir, 'ts', True)
        self.assertFalse(workspace3.workflow.find_node('ds').persistent)
        self.assertTrue(workspace3.workflow.find_node('ts').persistent)
        workspace4 = workspace_manager.set_workspace_resource_persistence(base_dir, 'ts', True)
        self.assertFalse(workspace4.workflow.find_node('ds').persistent)
        self.assertTrue(workspace4.workflow.find_node('ts').persistent)

        workspace_manager.save_workspace(base_dir)
        self.assertTrue(os.path.isfile(ts_file_path))

        workspace_manager.close_workspace(base_dir)
        self.assertEqual(len(workspace_manager.get_open_workspaces()), 0)
        self.assertTrue(os.path.isfile(ts_file_path))

        workspace5 = workspace_manager.open_workspace(base_dir)
        self.assertEqual(workspace4.workflow.to_json_dict(), workspace5.workflow.to_json_dict())
        workspace_manager.set_workspace_resource_persistence(base_dir, 'ts', False)
        workspace_manager.close_workspace(base_dir)
        workspace_manager.close_workspace(base_dir)  # closing a 2nd time should give no error
        self.assertFalse(os.path.isfile(ts_file_path))

        self.del_base_dir(base_dir)


class FSWorkspaceManagerTest(WorkspaceManagerTestMixin, unittest.TestCase):

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self._is_relative = False
        self._path_manager = None
        self._root_dir = tempfile.mkdtemp()

    def __del__(self):
        shutil.rmtree(self._root_dir)

    def new_workspace_manager(self):
        return FSWorkspaceManager(self._root_dir)


class RelativeFSWorkspaceManagerTest(WorkspaceManagerTestMixin, unittest.TestCase):

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self._is_relative = True
        self._root_dir = tempfile.mkdtemp()
        self._path_manager = PathManager(self._root_dir)

    def __del__(self):
        shutil.rmtree(self._root_dir)

    def new_workspace_manager(self):
        return RelativeFSWorkspaceManager(self._path_manager)
