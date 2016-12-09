import unittest

import cate.ui.websock as websock


class MapServiceMethodNameTest(unittest.TestCase):
    def test_map_service_method_name(self):
        self.assertEqual(websock._map_service_method_name(''), '')
        self.assertEqual(websock._map_service_method_name('newWorkspace'), 'new_workspace')
        self.assertEqual(websock._map_service_method_name('new_workspace'), 'new_workspace')
        self.assertEqual(websock._map_service_method_name('getWebAPIConfig'), 'get_web_api_config')
        self.assertEqual(websock._map_service_method_name('compute4Nodes'), 'compute_4_nodes')
        self.assertEqual(websock._map_service_method_name('computeGRAPH42'), 'compute_graph42')
        self.assertEqual(websock._map_service_method_name('compute42GRAPHS'), 'compute_42graphs')
