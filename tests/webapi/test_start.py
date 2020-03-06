import json
import os
import unittest
from unittest.mock import patch

from tornado.testing import AsyncHTTPTestCase
from cate.webapi.start import create_application

NETCDF_TEST_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'precip_and_temp.nc')


# For usage of the tornado.testing.AsyncHTTPTestCase see http://www.tornadoweb.org/en/stable/testing.html

@unittest.skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
class WebAPITest(AsyncHTTPTestCase):
    def get_app(self):
        self.url = '/'
        return create_application(user_root_path=None)

    def test_base_url(self):
        response = self.fetch(self.url)
        self.assertEqual(response.code, 200)
        json_dict = json.loads(response.body.decode('utf-8'))
        self.assertIn('status', json_dict)
        self.assertIn('content', json_dict)
        self.assertIn('name', json_dict['content'])
        self.assertIn('version', json_dict['content'])
        self.assertIn('user_root_mode', json_dict['content'])


# Tests if root of url can be changed, mostly relevant for CateHub context.
@unittest.skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
class WebAPIHubContextTest(WebAPITest):
    @patch.dict(os.environ, {'JUPYTERHUB_SERVICE_PREFIX': '/user/test/'})
    def get_app(self):
        self.url = os.environ.get('JUPYTERHUB_SERVICE_PREFIX')
        return create_application(user_root_path=None)

@unittest.skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
class WebAPIRelativeFSTest(AsyncHTTPTestCase):
    def get_app(self):
        return create_application(user_root_path='/home/test')

    def test_base_url(self):
        response = self.fetch('/')
        self.assertEqual(response.code, 200)
        json_dict = json.loads(response.body.decode('utf-8'))
        self.assertIn('user_root_mode', json_dict['content'])
        self.assertTrue(json_dict['content']['user_root_mode'])


@unittest.skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
class WebAPIFSTest(AsyncHTTPTestCase):
    def get_app(self):
        return create_application(user_root_path=None)

    def test_base_url(self):
        response = self.fetch('/')
        self.assertEqual(response.code, 200)
        json_dict = json.loads(response.body.decode('utf-8'))
        self.assertIn('user_root_mode', json_dict['content'])
        self.assertFalse(json_dict['content']['user_root_mode'])
