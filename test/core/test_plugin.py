from unittest import TestCase

from cate.core import plugin
from cate.util.misc import fetch_std_streams


class PluginTest(TestCase):
    def test_that_test_plugins_are_loaded(self):
        self.assertIsNotNone(plugin.PLUGIN_REGISTRY)

    def test_cate_init(self):
        # Yes, this is really a silly test :)
        # But this way we cover one more (empty) statement.
        plugin.cate_init(True, False, a=1, b=2)

    def test_error_reporting(self):
        with fetch_std_streams() as (stdout, stderr):
            plugin._report_plugin_error_msg('XXX')
        self.assertEqual(stderr.getvalue(), 'error: XXX\n')

        with fetch_std_streams() as (stdout, stderr):
            plugin._report_plugin_exception('YYY')
        self.assertEqual(stderr.getvalue(), 'error: YYY\n')
