import os

from unittest import TestCase
from os import environ
from cate.core.common import initialize_proxy
from cate.conf import conf


class CommonTest(TestCase):
    @classmethod
    def setUp(cls):
        if environ.get('http_proxy'):
            os.environ.pop('http_proxy')
        if environ.get('https_proxy'):
            os.environ.pop('https_proxy')

    def test_initialize_empty_proxy(self):
        conf.get_config()
        conf.set_config({'http_proxy': ''})

        initialize_proxy()

        self.assertEqual(None, environ.get('http_proxy'))
        self.assertEqual(None, environ.get('https_proxy'))

    def test_initialize_http_proxy(self):
        conf.get_config()
        conf.set_config({'http_proxy': 'http://user:pw@testurl:9000'})

        initialize_proxy()

        self.assertEqual('http://user:pw@testurl:9000', environ.get('http_proxy'))
        self.assertEqual(None, environ.get('https_proxy'))

    def test_initialize_https_proxy(self):
        conf.get_config()
        conf.set_config({'http_proxy': 'https://user:pw@testurl:9000'})

        initialize_proxy()

        self.assertEqual(None, environ.get('http_proxy'))
        self.assertEqual('https://user:pw@testurl:9000', environ.get('https_proxy'))
