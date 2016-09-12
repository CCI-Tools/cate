import json
import os
import re
import shutil
from unittest import TestCase

from ect.ui import webapi
from ect.ui.workspace import encode_path
from tornado.testing import AsyncHTTPTestCase


# see http://www.tornadoweb.org/en/stable/testing.html
class WebAPITest(AsyncHTTPTestCase):
    def get_app(self):
        return webapi.get_application()

    def test_homepage(self):
        response = self.fetch('/')
        self.assertEqual(response.code, 200)
        json_dict = json.loads(response.body.decode('utf-8'))
        self.assertIn('name', json_dict)
        self.assertIn('version', json_dict)

    def test_ws_init(self):
        base_dir = os.path.abspath('TEST_WS_1')

        if os.path.exists(base_dir):
            shutil.rmtree(base_dir)

        response = self.fetch(encode_path('/ws/init', query_args=dict(base_dir=os.path.abspath('TEST_WS_1'),
                                                                      description='Wow!')))
        self.assertEqual(response.code, 200)
        json_dict = json.loads(response.body.decode('utf-8'))
        self.assertIn('base_dir', json_dict)
        self.assertIn('workflow', json_dict)

        if os.path.exists(base_dir):
            shutil.rmtree(base_dir)

    def test_ws_get(self):
        base_dir = os.path.abspath('TEST_WS_2')

        if os.path.exists(base_dir):
            shutil.rmtree(base_dir)

        response = self.fetch(encode_path('/ws/init', query_args=dict(base_dir=os.path.abspath('TEST_WS_2'),
                                                                      description='Wow!')))
        self.assertEqual(response.code, 200)
        json_dict = json.loads(response.body.decode('utf-8'))
        self.assertIn('base_dir', json_dict)
        self.assertIn('workflow', json_dict)

        response = self.fetch(encode_path('/ws/get/{base_dir}', path_args=dict(base_dir=base_dir)))
        self.assertEqual(response.code, 200)
        json_dict = json.loads(response.body.decode('utf-8'))
        self.assertIn('base_dir', json_dict)
        self.assertIn('workflow', json_dict)

        if os.path.exists(base_dir):
            shutil.rmtree(base_dir)


class UrlPatternTest(TestCase):
    def test_url_pattern_works(self):
        re_pattern = webapi.url_pattern('/open/{{id1}}ws/{{id2}}wf')
        matcher = re.fullmatch(re_pattern, '/open/34ws/a66wf')
        self.assertIsNotNone(matcher)
        self.assertEqual(matcher.groupdict(), {'id1': '34', 'id2': 'a66'})

        re_pattern = webapi.url_pattern('/open/ws{{id1}}/wf{{id2}}')
        matcher = re.fullmatch(re_pattern, '/open/ws34/wfa66')
        self.assertIsNotNone(matcher)
        self.assertEqual(matcher.groupdict(), {'id1': '34', 'id2': 'a66'})

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
