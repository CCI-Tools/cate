import json
import tempfile
import unittest

from tornado.testing import AsyncHTTPTestCase
from cate.webapi.rest import _ensure_str
from cate.webapi.start import create_application


class TestEnsureStr(unittest.TestCase):
    def test_ensure_str(self):
        expected = 'doofer'

        value = 'doofer'
        res = _ensure_str(value)

        self.assertEqual(expected, res)

        value = b'doofer'
        res = _ensure_str(value)

        self.assertEqual(expected, res)

        value = [b'doofer']
        res = _ensure_str(value)

        self.assertEqual(expected, res)

        expected = '1'
        value = 1
        res = _ensure_str(value)

        self.assertEqual(expected, res)


class TestFileUpload(AsyncHTTPTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.correct_chunk = b'------WebKitFormBoundaryqG7XDYvKKeqhloS7\r\nContent-Disposition: form-data; ' \
                             b'name="dir"\r\n\r\n./\r\n------WebKitFormBoundaryqG7XDYvKKeqhloS7\r\n' \
                             b'Content-Disposition: form-data; name="files"; filename="test.txt"\r\n' \
                             b'Content-Type: text/plain\r\n\r\ntest\n\r\n' \
                             b'------WebKitFormBoundaryqG7XDYvKKeqhloS7--\r\n'

    def get_app(self):
        root = tempfile.mkdtemp()
        return create_application(root)

    def test_http_fetch(self):
        response = self.fetch('/files/upload', method='POST', body=self.correct_chunk)
        body = response.body.decode()
        body = json.loads(body)
        self.assertEqual('success', body['status'])
        self.assertEqual('0MBs uploaded.', body['message'])


if __name__ == '__main__':
    unittest.main()
