import unittest

from cate.util.web.serviceinfo import is_service_compatible


class IsServiceCompatibleTest(unittest.TestCase):
    def test_is_service_compatible_true(self):
        self.assertTrue(is_service_compatible(None, None, None,
                                              service_info=dict(port=8675, address=None, caller=None)))
        self.assertTrue(is_service_compatible(8675, None, None,
                                              service_info=dict(port=8675, address=None, caller=None)))
        self.assertTrue(is_service_compatible(8675, None, None,
                                              service_info=dict(port=8675, address='bibo', caller=None)))
        self.assertTrue(is_service_compatible(8675, None, None,
                                              service_info=dict(port=8675, address='bibo', caller='cate-gui')))
        self.assertTrue(is_service_compatible(8675, None, None,
                                              service_info=dict(port=8675, address=None, caller='cate-gui')))
        self.assertTrue(is_service_compatible(8675, None, 'cate',
                                              service_info=dict(port=8675, address=None, caller='cate')))

    def test_is_service_compatible_false(self):
        self.assertFalse(is_service_compatible(None, None, None,
                                               service_info=dict(port=None, address=None, caller=None)))
        self.assertFalse(is_service_compatible(7684, None, None,
                                               service_info=dict(port=None, address=None, caller=None)))
        self.assertFalse(is_service_compatible(7684, None, None,
                                               service_info=dict(port=7685, address=None, caller=None)))
        self.assertFalse(is_service_compatible(7684, None, 'cate',
                                               service_info=dict(port=7684, address=None, caller='cate-gui')))
