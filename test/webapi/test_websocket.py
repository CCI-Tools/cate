import unittest
import os
import shutil

from cate.core.wsmanag import FSWorkspaceManager
from cate.util.monitor import Monitor
from cate.webapi.websocket import WebSocketService


class WebSocketServiceTest(unittest.TestCase):
    def setUp(self):
        self.service = WebSocketService(FSWorkspaceManager())
        self.base_dir = base_dir = os.path.abspath('WebSocketServiceTest')
        if os.path.exists(self.base_dir):
            shutil.rmtree(base_dir)
        os.mkdir(self.base_dir)

    def tearDown(self):
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)

    @unittest.skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
    def test_get_data_stores(self):
        data_stores = self.service.get_data_stores()
        self.assertIsInstance(data_stores, list)
        self.assertGreater(len(data_stores), 1)
        self.assertIn('local', [ds['id'] for ds in data_stores])

    @unittest.skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
    def test_get_data_sources(self):
        data_stores = self.service.get_data_stores()
        for ds in data_stores:
            data_sources = self.service.get_data_sources(ds['id'], monitor=Monitor.NONE)
            self.assertIsInstance(data_sources, list)

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
        self.assertIn('test.webapi.test_websocket.my_op_with_deprecated_io', op_names)
        self.assertNotIn('test.webapi.test_websocket.my_deprecated_op', op_names)

        op = [op for op in ops if op['name'] == 'test.webapi.test_websocket.my_op_with_deprecated_io'][0]
        self.assertEqual(len(op['inputs']), 1)
        self.assertEqual(op['inputs'][0]['name'], 'a')
        self.assertEqual(len(op['outputs']), 1)
        self.assertEqual(op['outputs'][0]['name'], 'v')

    def test_get_workspace_variable_statistics(self):
        self.load_precip_dataset()
        stat = self.service.get_workspace_variable_statistics(self.base_dir,
                                                              res_name='ds',
                                                              var_name='temperature',
                                                              var_index=[0])
        self.assertAlmostEqual(stat['min'], 5.1)
        self.assertAlmostEqual(stat['max'], 26.2)

    def test_get_resource_values(self):
        workspaces = self.service.get_open_workspaces()
        self.assertEqual(workspaces, [])
        self.load_precip_dataset()
        workspaces = self.service.get_open_workspaces()
        self.assertEqual(1, len(workspaces))
        self.assertEqual(1, len(workspaces[0]['workflow']['steps']))

        values = self.service.extract_pixel_values(self.base_dir, 'ds', (10.22, 34.52), dict(time='2014-09-11'))

        self.assertAlmostEqual(values['lat'], 34.5)
        self.assertAlmostEqual(values['lon'], 10.2)
        self.assertAlmostEqual(values['precipitation'], 5.5)
        self.assertAlmostEqual(values['temperature'], 32.9)

        self.service.clean_workspace(self.base_dir)
        workspaces = self.service.get_open_workspaces()
        self.assertEqual(1, len(workspaces))
        self.assertEqual(0, len(workspaces[0]['workflow']['steps']))

        self.service.close_workspace(self.base_dir)
        workspaces = self.service.get_open_workspaces()
        self.assertEqual(workspaces, [])

    def load_precip_dataset(self):
        file = os.path.join(os.path.dirname(__file__), '..', 'data', 'precip_and_temp.nc')
        self.service.new_workspace(self.base_dir)
        self.service.set_workspace_resource(self.base_dir,
                                            'cate.ops.io.read_netcdf',
                                            dict(file=dict(value=file)),
                                            res_name='ds',
                                            overwrite=False,
                                            monitor=Monitor.NONE)
