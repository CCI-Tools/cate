from unittest import TestCase

from cate.conf import conf


class ConfTest(TestCase):
    def test_get_variable_display_settings(self):
        settings = conf.get_variable_display_settings('__bibo__')
        self.assertEqual(settings, dict(color_map='inferno'))

        settings = conf.get_variable_display_settings('lccs_class')
        self.assertIsNotNone(settings)
        self.assertIn('color_map', settings)

    def test_get_default_res_prefix(self):
        default_res_prefix = conf.get_default_res_prefix()
        self.assertIsNotNone(default_res_prefix)
        self.assertTrue(default_res_prefix.strip() != '')
