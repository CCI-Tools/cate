import os
import os.path
import platform
import shutil
import unittest
from typing import Dict

from cate.core.wsmanag import FSWorkspaceManager
from cate.util.monitor import Monitor
from cate.webapi.websocket import WebSocketService


class WebSocketServiceTest(unittest.TestCase):
    def get_root_path(self):
        return None

    def get_workspace_path(self):
        return os.path.join(os.path.dirname(__file__), 'WebSocketServiceTest')

    def setUp(self):
        manager = FSWorkspaceManager(root_path=self.get_root_path())
        self.service = WebSocketService(manager)
        self._workspace_dir = manager.resolve_path(self.get_workspace_path())
        if os.path.exists(self._workspace_dir):
            shutil.rmtree(self._workspace_dir)

    def tearDown(self):
        if os.path.exists(self._workspace_dir):
            shutil.rmtree(self._workspace_dir)

    @unittest.skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
    def test_get_data_stores(self):
        data_stores = self.service.get_data_stores()
        self.assertIsInstance(data_stores, list)
        self.assertGreater(len(data_stores), 1)
        for ds in data_stores:
            self.assertIn('id', ds)
            self.assertIsInstance(ds['id'], str)
            self.assertIn('isLocal', ds)
            self.assertIsInstance(ds['isLocal'], bool)
            self.assertIn('description', ds)
            self.assertIn('notices', ds)
            self.assertIsInstance(ds['notices'], list)
        self.assertIn('local', [ds['id'] for ds in data_stores])

    @unittest.skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
    def test_get_data_sources(self):
        data_stores = self.service.get_data_stores()
        required_fields = [
            ('id', str),
        ]
        optional_fields = [
            ('title', str),
            ('metaInfo', dict),
            ("verificationFlags", list),
            ("typeSpecifier", str)
        ]
        for ds in data_stores:
            self.assertIn('id', ds)
            if ds['id'] == 'cci-zarr-store':
                continue
            self.assertIsInstance(ds['id'], str)

            data_sources = self.service.get_data_sources(ds['id'], monitor=Monitor.NONE)
            self.assertIsInstance(data_sources, list)
            for data_source in data_sources:
                data_source = dict(data_source)
                for k, t in required_fields:
                    self.assertIn(k, data_source)
                    self.assertIsInstance(data_source[k], t)
                for k, t in optional_fields:
                    if k in data_source and data_source[k] is not None:
                        self.assertIsInstance(data_source[k], t)

    def test_get_operations(self):
        ops = self.service.get_operations()
        self.assertIsInstance(ops, list)
        self.assertGreater(len(ops), 20)

        self.assertIn('open_dataset', [op['name'] for op in ops])
        open_dataset_op = [op for op in ops if op['name'] == 'open_dataset'][0]
        keys = sorted(list(open_dataset_op.keys()))
        self.assertEqual(keys, ['has_monitor', 'header', 'inputs', 'name', 'outputs', 'qualified_name'])
        keys = sorted(list(open_dataset_op['header'].keys()))
        self.assertEqual(keys, ['description', 'res_pattern', 'tags'])
        names = [props['name'] for props in open_dataset_op['inputs']]
        self.assertEqual(names, ['ds_id', 'time_range', 'region', 'var_names', 'normalize',
                                 'force_local', 'local_ds_id'])
        names = [props['name'] for props in open_dataset_op['outputs']]
        self.assertEqual(names, ['return'])

    def test_get_operations_with_deprecations(self):
        from cate.core.op import op, op_input, op_output, OpRegistry

        registry = OpRegistry()

        @op(registry=registry, deprecated=True)
        def my_deprecated_op():
            pass

        # noinspection PyUnusedLocal
        @op_input('a', registry=registry)
        @op_input('b', registry=registry, deprecated=True)
        @op_output('u', registry=registry, deprecated=True)
        @op_output('v', registry=registry)
        def my_op_with_deprecated_io(a, b=None):
            pass

        self.assertIsNotNone(registry.get_op(my_deprecated_op, fail_if_not_exists=True))
        self.assertIsNotNone(registry.get_op(my_op_with_deprecated_io, fail_if_not_exists=True))

        ops = self.service.get_operations(registry=registry)
        op_names = {op['name'] for op in ops}
        self.assertIn('tests.webapi.test_websocket.my_op_with_deprecated_io', op_names)
        self.assertNotIn('tests.webapi.test_websocket.my_deprecated_op', op_names)

        op = [op for op in ops if op['name'] == 'tests.webapi.test_websocket.my_op_with_deprecated_io'][0]
        self.assertEqual(len(op['inputs']), 1)
        self.assertEqual(op['inputs'][0]['name'], 'a')
        self.assertEqual(len(op['outputs']), 1)
        self.assertEqual(op['outputs'][0]['name'], 'v')

    def test_get_workspace_variable_statistics(self):
        self._load_precip_dataset_in_workspace()
        stat = self.service.get_workspace_variable_statistics(self.get_workspace_path(),
                                                              res_name='ds',
                                                              var_name='temperature',
                                                              var_index=[0])
        self.assertAlmostEqual(stat['min'], 5.1)
        self.assertAlmostEqual(stat['max'], 26.2)

    def test_get_resource_values(self):
        workspaces = self.service.get_open_workspaces()
        self.assertEqual(workspaces, [])
        self._load_precip_dataset_in_workspace()
        workspaces = self.service.get_open_workspaces()
        self.assertEqual(1, len(workspaces))
        self.assertEqual(1, len(workspaces[0]['workflow']['steps']))

        values = self.service.extract_pixel_values(self.get_workspace_path(), 'ds', (10.22, 34.52),
                                                   dict(time='2014-09-11'))

        self.assertAlmostEqual(values['lat'], 34.5)
        self.assertAlmostEqual(values['lon'], 10.2)
        self.assertAlmostEqual(values['precipitation'], 5.5)
        self.assertAlmostEqual(values['temperature'], 32.9)

        self.service.clean_workspace(self.get_workspace_path())
        workspaces = self.service.get_open_workspaces()
        self.assertEqual(1, len(workspaces))
        self.assertEqual(0, len(workspaces[0]['workflow']['steps']))

        self.service.close_workspace(self.get_workspace_path())
        workspaces = self.service.get_open_workspaces()
        self.assertEqual(workspaces, [])

    def test_workspace_json(self):
        workspace_json = self._load_precip_dataset_in_workspace()
        self.assertIn('base_dir', workspace_json)
        self.assertEqual(self.get_workspace_path(),
                         workspace_json['base_dir'])

    def _load_precip_dataset_in_workspace(self) -> dict:
        file = os.path.join(os.path.dirname(__file__), '..', 'data', 'precip_and_temp.nc')
        workspace_json = self.service.new_workspace(self.get_workspace_path())
        self.service.save_workspace(self.get_workspace_path(), monitor=Monitor.NONE)
        self.service.set_workspace_resource(self.get_workspace_path(),
                                            'cate.ops.io.read_netcdf',
                                            dict(file=dict(value=file)),
                                            res_name='ds',
                                            overwrite=False,
                                            monitor=Monitor.NONE)
        return workspace_json

    def test_update_file_node(self):
        path = ''
        node = self.service.update_file_node(path)
        self._assert_dir_node_props(node, '', no_stats=True)

        path = __file__
        node = self.service.update_file_node(path)
        self._assert_file_node_props(node, os.path.basename(path))

        if platform.system() == 'Windows':
            # Test with just drive letter prefix
            path = os.path.abspath('.')[0:2]
            node = self.service.update_file_node(path)
            self._assert_dir_node_props(node, path)

        path = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..'))
        node = self.service.update_file_node(path)
        self._assert_dir_node_props(node, os.path.basename(path))

    def _assert_file_node_props(self, file_node: Dict, name: str):
        self._assert_file_node_props_base(file_node, name)
        self.assertIn('isDir', file_node)
        self.assertEqual(file_node['isDir'], False)
        self.assertNotIn('childNodes', file_node)

    def _assert_dir_node_props(self, file_node: Dict, name: str, no_stats=False):
        self._assert_file_node_props_base(file_node, name, no_stats=no_stats)
        self.assertIn('isDir', file_node)
        self.assertEqual(file_node['isDir'], True)
        self.assertIn('childNodes', file_node)
        self.assertIsNotNone(file_node['childNodes'])

    def _assert_file_node_props_base(self, file_node: Dict, name: str, no_stats=False):
        self.assertIsNotNone(file_node)
        self.assertIn('name', file_node)
        self.assertEqual(file_node['name'], name)
        if not no_stats:
            self.assertIn('size', file_node)
            self.assertIn('lastModified', file_node)
        self.assertIn('isDir', file_node)


class SandboxedWebSocketServiceTest(WebSocketServiceTest):
    def get_root_path(self):
        return os.path.dirname(__file__)

    def get_workspace_path(self):
        return os.path.join(os.path.sep + 'SandboxedWebSocketServiceTest')

    def test_workspace_json(self):
        workspace_json = self._load_precip_dataset_in_workspace()
        self.assertIn('base_dir', workspace_json)
        self.assertEqual(os.path.sep + 'SandboxedWebSocketServiceTest', workspace_json['base_dir'])

    def test_update_file_node(self):
        path = ''
        node = self.service.update_file_node(path)
        self._assert_dir_node_props(node, '')

        path = '/'
        node = self.service.update_file_node(path)
        self._assert_dir_node_props(node, '')

        path = '.'
        node = self.service.update_file_node(path)
        self._assert_dir_node_props(node, '')

        path = os.path.basename(__file__)
        node = self.service.update_file_node(path)
        self._assert_file_node_props(node, path)

        path = os.path.basename(__file__)
        node = self.service.update_file_node(os.path.sep + path)
        self._assert_file_node_props(node, path)

        if platform.system() == 'Windows':
            # Test with just drive letter prefix
            path = os.path.abspath('.')[0:2] + '\\'
            with self.assertRaises(ValueError) as cm:
                self.service.update_file_node(path)
            self.assertTrue(f'{cm.exception}'.startswith('access denied: '))

        with self.assertRaises(ValueError) as cm:
            self.service.update_file_node('..')
        self.assertTrue(f'{cm.exception}'.startswith('access denied: '))

        with self.assertRaises(ValueError) as cm:
            self.service.update_file_node(os.path.sep + '..')
        self.assertTrue(f'{cm.exception}'.startswith('access denied: '))
