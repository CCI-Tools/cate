import os
import shutil
import unittest

from cate.core.wsmanag import WorkspaceManager, FSWorkspaceManager
from cate.core.workspace import mk_op_kwargs
from ..util.test_monitor import RecordingMonitor

NETCDF_TEST_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'precip_and_temp.nc')


# noinspection PyUnresolvedReferences
class WorkspaceManagerTestMixin:
    def new_workspace_manager(self) -> WorkspaceManager:
        raise NotImplementedError

    def new_base_dir(self, base_dir):
        base_dir = os.path.abspath(base_dir)
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
        workspace_manager.set_workspace_resource(base_dir, res_name='SST',
                                                 op_name='cate.ops.io.read_netcdf',
                                                 op_args=mk_op_kwargs(file=NETCDF_TEST_FILE))
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
        workspace_manager.set_workspace_resource(base_dir, res_name='SST',
                                                 op_name='cate.ops.io.read_netcdf',
                                                 op_args=dict(file=dict(value=NETCDF_TEST_FILE)))
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
        workspace_manager.set_workspace_resource(base_dir, res_name='noop',
                                                 op_name='cate.ops.utility.no_op',
                                                 op_args=dict(),
                                                 monitor=rm)
        print(rm)
        self.assertEquals([
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
            ('progress', 1.0, 'Step 10 of 10 doing nothing', 100)
        ], rm.records[:11])
        # in ws case 'done' is not transmitted
        self.del_base_dir(base_dir)

    def test_session(self):
        base_dir = self.new_base_dir('TESTOMAT')

        workspace_manager = self.new_workspace_manager()
        workspace1 = workspace_manager.new_workspace(base_dir, description='session workspace')
        self.assertIsNotNone(workspace1)
        self.assertEqual(workspace1.base_dir, base_dir)
        self.assertEqual(workspace1.workflow.op_meta_info.header.get('description', None), 'session workspace')
        workspace_manager.save_workspace(base_dir)
        self.assertTrue(os.path.exists(base_dir))

        res_name = 'ds'
        workspace_manager.set_workspace_resource(base_dir, res_name=res_name,
                                                 op_name='cate.ops.io.read_netcdf',
                                                 op_args=dict(file=dict(value=NETCDF_TEST_FILE)))
        workspace2 = workspace_manager.get_workspace(base_dir)

        self.assertEqual(workspace2.base_dir, base_dir)
        self.assertEqual(workspace2.workflow.id, workspace1.workflow.id)
        self.assertEqual(len(workspace2.workflow.steps), 1)

        sst_step = workspace2.workflow.find_node(res_name)
        self.assertIsNotNone(sst_step)

        file_path = os.path.abspath(os.path.join('TESTOMAT', 'precip_and_temp_copy.nc'))
        workspace_manager.write_workspace_resource(base_dir, res_name, file_path=file_path)
        self.assertTrue(os.path.isfile(file_path))

        workspaces = workspace_manager.get_open_workspaces()
        self.assertIsNotNone(workspaces)
        self.assertEqual(len(workspaces), 1)
        self.assertEqual(workspaces[0].base_dir, base_dir)

        workspace3 = workspace_manager.delete_workspace_resource(base_dir, res_name)
        self.assertEqual(len(workspace3.workflow.steps), 0)

        self.del_base_dir(base_dir)

    def test_persitence(self):
        base_dir = self.new_base_dir('TESTOMAT')

        workspace_manager = self.new_workspace_manager()
        workspace_manager.new_workspace(base_dir)
        workspace_manager.save_workspace(base_dir)
        self.assertTrue(os.path.exists(base_dir))

        workspace_manager.set_workspace_resource(base_dir, res_name='ds',
                                                 op_name='cate.ops.io.read_netcdf',
                                                 op_args=dict(file=dict(value=NETCDF_TEST_FILE)))
        workspace1 = workspace_manager.set_workspace_resource(base_dir, res_name='ts',
                                                              op_name='cate.ops.timeseries.tseries_mean',
                                                              op_args=mk_op_kwargs(ds='@ds', var='temperature'))
        self.assertEqual(workspace1.base_dir, base_dir)
        self.assertEqual(len(workspace1.workflow.steps), 2)
        self.assertFalse(workspace1.workflow.find_node('ds').persistent)
        self.assertFalse(workspace1.workflow.find_node('ts').persistent)
        ts_file_path = os.path.abspath(os.path.join('TESTOMAT', '.cate-workspace', 'ts.nc'))
        self.assertFalse(os.path.isfile(ts_file_path))

        workspace3 = workspace_manager.set_workspace_resource_persistence(base_dir, 'ts', True)
        self.assertFalse(workspace3.workflow.find_node('ds').persistent)
        self.assertTrue(workspace3.workflow.find_node('ts').persistent)

        workspace_manager.save_workspace(base_dir)
        self.assertTrue(os.path.isfile(ts_file_path))

        self.del_base_dir(base_dir)


class FSWorkspaceManagerTest(WorkspaceManagerTestMixin, unittest.TestCase):
    def new_workspace_manager(self):
        return FSWorkspaceManager()
