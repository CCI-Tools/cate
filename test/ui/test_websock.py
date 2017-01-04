import unittest

from cate import Monitor
from cate.ui.websock import ServiceMethods
from cate.ui.wsmanag import FSWorkspaceManager


class MapServiceMethodNameTest(unittest.TestCase):

    def setUp(self):
        self.service_methods = ServiceMethods(FSWorkspaceManager())

    def test_get_data_stores(self):
        data_stores = self.service_methods.get_data_stores()
        self.assertIsInstance(data_stores, list)
        self.assertGreater(len(data_stores), 1)
        self.assertIn('local', [ds['id'] for ds in data_stores])

    def test_get_data_sources(self):
        data_stores = self.service_methods.get_data_stores()
        for ds in data_stores:
            data_sources = self.service_methods.get_data_sources(ds['id'], monitor=Monitor.NONE)
            self.assertIsInstance(data_sources, list)

    def test_get_operations(self):
        ops = self.service_methods.get_operations()
        self.assertIsInstance(ops, list)
        self.assertGreater(len(ops), 20)
        self.assertIn('open_dataset', [op['name'] for op in ops])
