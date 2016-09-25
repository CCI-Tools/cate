import json
import os
import re
import shutil
import unittest
import urllib.parse

from ect.core.util import encode_url_path
from ect.ui import webapi
from tornado.testing import AsyncHTTPTestCase


# For usage of the tornado.testing.AsyncHTTPTestCase see http://www.tornadoweb.org/en/stable/testing.html

@unittest.skipIf(os.environ.get('ECT_DISABLE_WEB_TESTS', None) == '1', 'ECT_DISABLE_WEB_TESTS = 1')
class WebAPITest(AsyncHTTPTestCase):
    def get_app(self):
        return webapi.get_application()

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
                                                              save=True,
                                                              description='Wow!')))
        self.assertEqual(response.code, 200)
        json_dict = json.loads(response.body.decode('utf-8'))
        self.assertIn('status', json_dict)
        self.assertIn('content', json_dict)
        self.assertIn('base_dir', json_dict['content'])
        self.assertIn('workflow', json_dict['content'])

        res_name = 'ds'

        file_path = os.path.join(os.path.dirname(__file__), 'precip_and_temp.nc')
        op_args = ["file='%s'" % file_path.replace('\\', '\\\\')]
        data = dict(op_name='ect.ops.io.read_netcdf', op_args=json.dumps(op_args))
        body = urllib.parse.urlencode(data)
        url = encode_url_path('/ws/res/set/{base_dir}/{res_name}',
                              path_args=dict(base_dir=os.path.abspath(base_dir),
                                             res_name=res_name))
        response = self.fetch(url, method='POST', body=body)

        self.assertEqual(response.code, 200)
        json_dict = json.loads(response.body.decode('utf-8'))
        self.assertEqual(json_dict, dict(status='ok', content=None))

        file_path = os.path.abspath(os.path.join('TEST_WORKSPACE', 'precip_and_temp_copy.nc'))
        data = dict(file_path=file_path)
        body = urllib.parse.urlencode(data)
        url = encode_url_path('/ws/res/write/{base_dir}/{res_name}',
                              path_args=dict(base_dir=os.path.abspath(base_dir),
                                             res_name=res_name))
        response = self.fetch(url, method='POST', body=body)

        self.assertEqual(response.code, 200)
        json_dict = json.loads(response.body.decode('utf-8'))
        self.assertEqual(json_dict, dict(status='ok', content=None))

        self.assertTrue(os.path.isfile(file_path))

        if os.path.exists(base_dir):
            shutil.rmtree(base_dir)


class UrlPatternTest(unittest.TestCase):
    def test_url_pattern_works(self):
        re_pattern = webapi.url_pattern('/open/{{id1}}ws/{{id2}}wf')
        matcher = re.fullmatch(re_pattern, '/open/34ws/a66wf')
        self.assertIsNotNone(matcher)
        self.assertEqual(matcher.groupdict(), {'id1': '34', 'id2': 'a66'})

        re_pattern = webapi.url_pattern('/open/ws{{id1}}/wf{{id2}}')
        matcher = re.fullmatch(re_pattern, '/open/ws34/wfa66')
        self.assertIsNotNone(matcher)
        self.assertEqual(matcher.groupdict(), {'id1': '34', 'id2': 'a66'})

        x = 'C%3A%5CUsers%5CNorman%5CIdeaProjects%5Cccitools%5Cect-core%5Ctest%5Cui%5CTEST_WS_3'
        re_pattern = webapi.url_pattern('/ws/{{base_dir}}/res/{{res_name}}/add')
        matcher = re.fullmatch(re_pattern, '/ws/%s/res/SST/add' % x)
        self.assertIsNotNone(matcher)
        self.assertEqual(matcher.groupdict(), {'base_dir': x, 'res_name': 'SST'})

    def test_url_pattern_ok(self):
        self.assertEqual(webapi.url_pattern('/version'),
                         '/version')
        self.assertEqual(webapi.url_pattern('{{num}}/get'),
                         '(?P<num>[^\;\/\?\:\@\&\=\+\$\,]+)/get')
        self.assertEqual(webapi.url_pattern('/open/{{ws_name}}'),
                         '/open/(?P<ws_name>[^\;\/\?\:\@\&\=\+\$\,]+)')
        self.assertEqual(webapi.url_pattern('/open/ws{{id1}}/wf{{id2}}'),
                         '/open/ws(?P<id1>[^\;\/\?\:\@\&\=\+\$\,]+)/wf(?P<id2>[^\;\/\?\:\@\&\=\+\$\,]+)')

    def test_url_pattern_fail(self):
        with self.assertRaises(ValueError) as cm:
            webapi.url_pattern('/open/{{ws/name}}')
        self.assertEqual(str(cm.exception), 'name in {{name}} must be a valid identifier, but got "ws/name"')

        with self.assertRaises(ValueError) as cm:
            webapi.url_pattern('/info/{{id}')
        self.assertEqual(str(cm.exception), 'no matching "}}" after "{{" in "/info/{{id}"')
