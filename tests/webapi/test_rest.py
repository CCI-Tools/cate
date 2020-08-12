import json
import unittest
from tornado.testing import AsyncHTTPTestCase
from cate.webapi.rest import ensure_str
from cate.webapi.start import create_application


class TestEnsureStr(unittest.TestCase):
    def test_ensure_str(self):
        expected = 'doofer'

        value = 'doofer'
        res = ensure_str(value)

        self.assertEqual(expected, res)

        value = b'doofer'
        res = ensure_str(value)

        self.assertEqual(expected, res)

        value = [b'doofer']
        res = ensure_str(value)

        self.assertEqual(expected, res)

        expected = '1'
        value = 1
        res = ensure_str(value)

        self.assertEqual(expected, res)


class TestFileUpload(AsyncHTTPTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.chunk_non_existent_fp = b'------WebKitFormBoundaryqcRDeFKKo0JT59l9\r\nContent-Disposition: form-data; ' \
                     b'name="dir"\r\n\r\nnull' \
                     b'\r\n------WebKitFormBoundaryqcRDeFKKo0JT59l9\r\nContent-Disposition: form-data; name="files"; ' \
                     b'filename="ESACCI-OC-L3S-CHLOR_A-MERGED-8D_DAILY_4km_SIN_PML_OCx-19971227-fv3.1.nc"\r\n' \
                     b'Content-Type: application/x-netcdf\r\n\r\n' \
                     b'\x89HDF\r\n\x1a\n\x02\x08\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\xff' \
                     b'\xff\xff\xff\x8a\xd2\xe6\n\x00\x00\x00\x000\x00\x00\x00\x00\x00\x00\x00\xec\x83\xa46OHDR'

        self.correct_chunk = b'------WebKitFormBoundaryqG7XDYvKKeqhloS7\r\nContent-Disposition: form-data; ' \
                             b'name="dir"\r\n\r\n./\r\n------WebKitFormBoundaryqG7XDYvKKeqhloS7\r\n' \
                             b'Content-Disposition: form-data; name="files"; filename="test.txt"\r\n' \
                             b'Content-Type: text/plain\r\n\r\ntest\n\r\n' \
                             b'------WebKitFormBoundaryqG7XDYvKKeqhloS7--\r\n'

    def get_app(self):
        return create_application()

    def test_http_fetch_with_error(self):
        response = self.fetch('/files/upload', method='POST', body=self.chunk_non_existent_fp)
        body = response.body.decode()
        body = json.loads(body)
        self.assertEqual('error', body['status'])
        self.assertIn('No such file or directory', body['error'])

    def test_http_fetch(self):
        response = self.fetch('/files/upload', method='POST', body=self.correct_chunk)
        body = response.body.decode()
        body = json.loads(body)
        self.assertEqual('success', body['status'])
        self.assertEqual('0MBs uploaded.', body['message'])


if __name__ == '__main__':
    unittest.main()
