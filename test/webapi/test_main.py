import json
import os
import shutil
import unittest
import urllib.parse

from tornado.testing import AsyncHTTPTestCase

from cate.util.misc import encode_url_path
from cate.webapi.main import create_application

NETCDF_TEST_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'precip_and_temp.nc')


# For usage of the tornado.testing.AsyncHTTPTestCase see http://www.tornadoweb.org/en/stable/testing.html

@unittest.skipIf(os.environ.get('CATE_DISABLE_WEB_TESTS', None) == '1', 'CATE_DISABLE_WEB_TESTS = 1')
class WebAPITest(AsyncHTTPTestCase):
    def get_app(self):
        return create_application()

    def test_base_url(self):
        response = self.fetch('/')
        self.assertEqual(response.code, 200)
        json_dict = json.loads(response.body.decode('utf-8'))
        self.assertIn('status', json_dict)
        self.assertIn('content', json_dict)
        self.assertIn('name', json_dict['content'])
        self.assertIn('version', json_dict['content'])

    def test_workspace_session(self):
        base_dir = os.path.abspath('TEST_WORKSPACE')

        if os.path.exists(base_dir):
            shutil.rmtree(base_dir)

        response = self.fetch(encode_url_path('/ws/new',
                                              query_args=dict(base_dir=os.path.abspath('TEST_WORKSPACE'),
                                                              description='Wow!')))
        self.assertEqual(response.code, 200)
        json_dict = json.loads(response.body.decode('utf-8'))
        self.assertIn('status', json_dict, msg=json_dict)
        self.assertIn('content', json_dict, msg=json_dict)
        self.assertIn('base_dir', json_dict['content'], msg=json_dict)
        self.assertIn('workflow', json_dict['content'], msg=json_dict)

        response = self.fetch(encode_url_path('/ws/save/{base_dir}',
                                              path_args=dict(base_dir=os.path.abspath('TEST_WORKSPACE'))))

        self.assertEqual(response.code, 200)
        json_dict = json.loads(response.body.decode('utf-8'))
        self.assertIn('status', json_dict)
        self.assertEqual(json_dict['status'], 'ok', msg=json_dict)

        res_name = 'ds'

        file_path = NETCDF_TEST_FILE
        op_args = ["file='%s'" % file_path.replace('\\', '\\\\')]
        data = dict(op_name='cate.ops.io.read_netcdf', op_args=json.dumps(op_args))
        body = urllib.parse.urlencode(data)
        url = encode_url_path('/ws/res/set/{base_dir}/{res_name}',
                              path_args=dict(base_dir=os.path.abspath(base_dir),
                                             res_name=res_name))
        response = self.fetch(url, method='POST', body=body)

        self.assertEqual(response.code, 200)
        json_dict = json.loads(response.body.decode('utf-8'))
        self.assertIn('status', json_dict)
        self.assertEqual(json_dict['status'], 'ok', msg=json_dict)

        file_path = os.path.abspath(os.path.join('TEST_WORKSPACE', 'precip_and_temp_copy.nc'))
        url = encode_url_path('/ws/res/write/{base_dir}/{res_name}',
                              path_args=dict(base_dir=os.path.abspath(base_dir),
                                             res_name=res_name),
                              query_args=dict(file_path=file_path))
        response = self.fetch(url, method='GET')

        self.assertEqual(response.code, 200)
        json_dict = json.loads(response.body.decode('utf-8'))
        self.assertIn('status', json_dict)
        self.assertEqual(json_dict['status'], 'ok', msg=json_dict)

        self.assertTrue(os.path.isfile(file_path))

        if os.path.exists(base_dir):
            shutil.rmtree(base_dir)
