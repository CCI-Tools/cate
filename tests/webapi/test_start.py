import json
import os
import unittest

from tornado.testing import AsyncHTTPTestCase
from cate.webapi.start import create_application

NETCDF_TEST_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'precip_and_temp.nc')


# For usage of the tornado.testing.AsyncHTTPTestCase see http://www.tornadoweb.org/en/stable/testing.html

@unittest.skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
class WebAPITest(AsyncHTTPTestCase):
    def get_app(self):
        return create_application(user_root_path=None)

    def test_base_url(self):
        response = self.fetch('/')
        self.assertEqual(response.code, 200)
        json_dict = json.loads(response.body.decode('utf-8'))
        self.assertIn('status', json_dict)
        self.assertIn('content', json_dict)
        self.assertIn('name', json_dict['content'])
        self.assertIn('version', json_dict['content'])
        self.assertIn('user_root_mode', json_dict['content'])


@unittest.skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
class WebAPIRelativeFSTest(AsyncHTTPTestCase):
    def get_app(self):
        return create_application(user_root_path='/home/test')

    def test_base_url(self):
        response = self.fetch('/')
        self.assertEqual(response.code, 200)
        json_dict = json.loads(response.body.decode('utf-8'))
        self.assertIn('workspace_manager_mode', json_dict['content'])
        self.assertEqual(json_dict['content']['user_root_mode'], 'relative_fs')


@unittest.skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
class WebAPIFSTest(AsyncHTTPTestCase):
    def get_app(self):
        return create_application(user_root_path=None)

    def test_base_url(self):
        response = self.fetch('/')
        self.assertEqual(response.code, 200)
        json_dict = json.loads(response.body.decode('utf-8'))
        self.assertIn('user_root_mode', json_dict['content'])
        self.assertEqual(json_dict['content']['workspace_manager_mode'], 'fs')
