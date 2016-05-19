from unittest import TestCase

from ect.core import plugin


class PluginTest(TestCase):
    def test_that_test_plugin_is_loaded(self):
        self.assertIsNotNone(plugin.REGISTRY)
        if 'test_plugin' in plugin.REGISTRY:
            # Note: if this fails, you should first do "python setup.py develop" in a terminal
            self.assertEqual(plugin.REGISTRY['test_plugin'], {'entry_point': 'test_plugin'})
        else:
            print('WARNING: PluginTest not performed, most likely because "python setup.py develop" has never been called.')




