import re
from unittest import TestCase

from ect.ui.webapi import url_pattern


class UrlPatternTest(TestCase):
    def test_url_pattern_works(self):
        re_pattern = url_pattern('/open/{{id1}}ws/{{id2}}wf')
        matcher = re.fullmatch(re_pattern, '/open/34ws/a66wf')
        self.assertIsNotNone(matcher)
        self.assertEqual(matcher.groupdict(), {'id1': '34', 'id2': 'a66'})

        re_pattern = url_pattern('/open/ws{{id1}}/wf{{id2}}')
        matcher = re.fullmatch(re_pattern, '/open/ws34/wfa66')
        self.assertIsNotNone(matcher)
        self.assertEqual(matcher.groupdict(), {'id1': '34', 'id2': 'a66'})

    def test_url_pattern_ok(self):
        self.assertEqual(url_pattern('/version'),
                         '/version')
        self.assertEqual(url_pattern('{{num}}/get'),
                         '(?P<num>[^\/\?\=\&]+)/get')
        self.assertEqual(url_pattern('/open/{{ws_name}}'),
                         '/open/(?P<ws_name>[^\/\?\=\&]+)')
        self.assertEqual(url_pattern('/open/ws{{id1}}/wf{{id2}}'),
                         '/open/ws(?P<id1>[^\/\?\=\&]+)/wf(?P<id2>[^\/\?\=\&]+)')

    def test_url_pattern_fail(self):
        with self.assertRaises(ValueError) as cm:
            url_pattern('/open/{{ws/name}}')
        self.assertEqual(str(cm.exception), 'name in {{name}} must be a valid identifier, but got "ws/name"')

        with self.assertRaises(ValueError) as cm:
            url_pattern('/info/{{id}')
        self.assertEqual(str(cm.exception), 'no matching "}}" after "{{" in "/info/{{id}"')
