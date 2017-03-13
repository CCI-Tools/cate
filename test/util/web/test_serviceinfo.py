import os.path
import shutil
import unittest

from cate.util.web.serviceinfo import is_service_compatible, write_service_info, read_service_info, find_free_port, \
    is_service_running


class IsServiceCompatibleTest(unittest.TestCase):
    def test_read_write(self):
        service_info = dict(port=9999, address='localhost', caller='cate-desktop')
        file = os.path.join('service_info', 'service_info.json')
        shutil.rmtree(file, ignore_errors=True)
        write_service_info(service_info, file)
        self.assertTrue(os.path.isfile(file))
        service_info2 = read_service_info(file)
        self.assertEqual(service_info2, service_info)
        shutil.rmtree(file, ignore_errors=True)

    def test_find_free_port(self):
        port = find_free_port()
        self.assertTrue(port > 0)

    def test_is_service_running(self):
        self.assertFalse(is_service_running(port=9999, address='localhost', timeout=100))

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
