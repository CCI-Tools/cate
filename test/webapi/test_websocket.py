import unittest
import os

from cate.core.wsmanag import FSWorkspaceManager
from cate.util.monitor import Monitor
from cate.webapi.websocket import WebSocketService


class WebSocketServiceTest(unittest.TestCase):
    def setUp(self):
        self.service = WebSocketService(FSWorkspaceManager())

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
        self.assertEqual(keys, ['has_monitor', 'header', 'input', 'name', 'output', 'qualified_name'])
        keys = sorted(list(open_dataset_op['header'].keys()))
        self.assertEqual(keys, ['description', 'tags'])
        names = [props['name'] for props in open_dataset_op['input']]
        self.assertEqual(names, ['ds_name', 'start_date', 'end_date', 'sync', 'protocol'])
        names = [props['name'] for props in open_dataset_op['output']]
        self.assertEqual(names, ['return'])
