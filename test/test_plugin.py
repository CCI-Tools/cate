from unittest import TestCase

from ect.core import plugin


class PluginTest(TestCase):
    def test_that_example_plugin_is_loaded(self):
        self.assertIsNotNone(plugin.PLUGINS)
        self.assertIn('example_plugin', plugin.PLUGINS)

        self.assertIsNotNone(plugin.CONTEXT)
        self.assertEqual('R', plugin.CONTEXT.readers['r'])
        self.assertEqual('W', plugin.CONTEXT.writers['w'])
        self.assertEqual('P', plugin.CONTEXT.processors['p'])




