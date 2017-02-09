import os
import shutil
import unittest

from cate.core.wsmanag import WorkspaceManager, FSWorkspaceManager

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

        workspace_manager = self.new_workspace_manager()
        workspace = workspace_manager.new_workspace(base_dir)
        workspace.save()
        self.assertTrue(os.path.exists(base_dir))
        self.assertIsNotNone(workspace)

        self.del_base_dir(base_dir)

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
                                                 op_args=['file=%s' % NETCDF_TEST_FILE])
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
        workspace1.save()
        self.assertTrue(os.path.exists(base_dir))
        workspace_manager.set_workspace_resource(base_dir, res_name='SST',
                                                 op_name='cate.ops.io.read_netcdf',
                                                 op_args=['file=%s' % NETCDF_TEST_FILE])
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


class FSWorkspaceManagerTest(WorkspaceManagerTestMixin, unittest.TestCase):
    def new_workspace_manager(self):
        return FSWorkspaceManager()
