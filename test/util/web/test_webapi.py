import re
import sys
import unittest

from cate.util.web import webapi


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
        name_chars_pattern = "[^\\;\\/\\?\\:\\@\\&\\=\\+\\$\\,]+"
        self.assertEqual(webapi.url_pattern('/version'),
                         '/version')
        self.assertEqual(webapi.url_pattern('{{num}}/get'),
                         f'(?P<num>{name_chars_pattern})/get')
        self.assertEqual(webapi.url_pattern('/open/{{ws_name}}'),
                         f'/open/(?P<ws_name>{name_chars_pattern})')
        self.assertEqual(webapi.url_pattern('/open/ws{{id1}}/wf{{id2}}'),
                         f'/open/ws(?P<id1>{name_chars_pattern})/wf(?P<id2>{name_chars_pattern})')

    def test_url_pattern_fail(self):
        with self.assertRaises(ValueError) as cm:
            webapi.url_pattern('/open/{{ws/name}}')
        self.assertEqual(str(cm.exception), 'name in {{name}} must be a valid identifier, but got "ws/name"')

        with self.assertRaises(ValueError) as cm:
            webapi.url_pattern('/info/{{id}')
        self.assertEqual(str(cm.exception), 'no matching "}}" after "{{" in "/info/{{id}"')


class WebAPIErrorTest(unittest.TestCase):
    def test_plain(self):
        self._plain(webapi.WebAPIServiceError)
        self._plain(webapi.WebAPIRequestError)

    def test_with_cause(self):
        self._with_cause(webapi.WebAPIServiceError)
        self._with_cause(webapi.WebAPIRequestError)

    def _plain(self, cls):
        try:
            raise cls("haha")
        except cls as e:
            self.assertEqual(str(e), "haha")
            self.assertEqual(e.cause, None)

    def _with_cause(self, cls):
        e1 = ValueError("a > 5")
        try:
            raise cls("hoho") from e1
        except cls as e2:
            self.assertEqual(str(e2), "hoho")
            self.assertEqual(e2.cause, e1)


class WebAPIRequestHandlerTest(unittest.TestCase):
    def test_to_status_error_empty(self):
        status = webapi.WebAPIRequestHandler._to_status_error()
        self.assertEqual(status, {'status': 'error'})

    def test_to_status_error_plain(self):
        try:
            raise ValueError("test message")
        except ValueError:
            exc_info = sys.exc_info()

            response = webapi.WebAPIRequestHandler._to_status_error(exc_info=exc_info)
            self.assertIn('status', response)
            self.assertEqual(response['status'], 'error')
            self.assertIn('error', response)
            error = response['error']
            self.assertEqual(error['message'], 'test message')
            self.assertIn('data', error)
            data = error['data']
            self.assertEqual(data['exception'], 'ValueError')
            self.assertIsNotNone(data['traceback'])
            self.assertIn('ValueError: test message', data['traceback'])

            response = webapi.WebAPIRequestHandler._to_status_error(exc_info=exc_info, message="my additional message")
            self.assertIn('status', response)
            self.assertEqual(response['status'], 'error')
            self.assertIn('error', response)
            error = response['error']
            self.assertEqual(error['message'], 'my additional message')
            self.assertIn('data', error)
            data = error['data']
            self.assertEqual(data['exception'], 'ValueError')
            self.assertIsNotNone(data['traceback'])
            self.assertIn('ValueError: test message', data['traceback'])

            response = webapi.WebAPIRequestHandler._to_status_error(message="my message")
            self.assertIn('status', response)
            self.assertEqual(response['status'], 'error')
            self.assertIn('error', response)
            error = response['error']
            self.assertEqual(error['message'], 'my message')
            self.assertNotIn('data', response)

    def test_to_status_error_chained(self):
        try:
            try:
                raise ValueError("my error 1")
            except ValueError as e:
                raise ValueError("my error 2") from e
        except ValueError:
            exc_info = sys.exc_info()
            response = webapi.WebAPIRequestHandler._to_status_error(exc_info=exc_info)
            self.assertIn('status', response)
            self.assertEqual(response['status'], 'error')
            self.assertIn('error', response)
            error = response['error']
            self.assertEqual(error['message'], 'my error 2')
            self.assertIn('data', error)
            data = error['data']
            self.assertEqual(data['exception'], 'ValueError')
            self.assertIsNotNone(data['traceback'])
            self.assertIn('ValueError: my error 1', data['traceback'])
            self.assertIn('ValueError: my error 2', data['traceback'])
