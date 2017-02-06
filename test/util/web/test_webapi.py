import re
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
