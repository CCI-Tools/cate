import io
import os.path
import shutil
import tempfile
import unittest

import sys

from cate.conf import conf


class ConfTest(unittest.TestCase):
    def test_get_variable_display_settings(self):
        settings = conf.get_variable_display_settings('__bibo__')
        self.assertEqual(settings, dict(color_map='inferno'))

        settings = conf.get_variable_display_settings('lccs_class')
        self.assertIsNotNone(settings)
        self.assertIn('color_map', settings)

    def test_get_default_res_prefix(self):
        default_res_prefix = conf.get_default_res_pattern()
        self.assertIsNotNone(default_res_prefix)
        self.assertTrue(default_res_prefix.strip() != '')

    def test_get_config_value(self):
        with self.assertRaises(ValueError) as e:
            conf.get_config_value(None)
        self.assertEqual(str(e.exception), 'name argument must be given')

        with self.assertRaises(ValueError) as e:
            conf.get_config_value('')
        self.assertEqual(str(e.exception), 'name argument must be given')

        value = conf.get_config_value('_im_not_in_', 'Yes!')
        self.assertEqual(value, 'Yes!')

    def test_get_config_path(self):
        value = conf.get_config_path('_im_not_in_', default='~/.cate/data_stores')
        self.assertIsNotNone(value)
        self.assertTrue(value.endswith('/.cate/data_stores'))
        self.assertNotIn('~', value)

    def test_get_config(self):
        config = conf.get_config()
        self.assertIsNotNone(config)

    def test_read_python_config_file(self):
        config = conf._read_python_config(io.StringIO("import os.path\n"
                                                      "root_dir = os.path.join('user', 'home', 'norman')"))
        self.assertIn('root_dir', config)
        self.assertEqual(config['root_dir'], os.path.join('user', 'home', 'norman'))

    def test_read_config_files(self):
        test_dir = os.path.join(tempfile.gettempdir(), "cate_test_read_config_files")
        os.mkdir(test_dir)

        cate_dir = os.path.join(test_dir, ".cate")
        os.mkdir(cate_dir)
        default_config_file = os.path.join(cate_dir, "conf.py")
        with open(default_config_file, "w") as fp:
            fp.write("a = 1\nb = 2\n")
        with open(os.path.join(cate_dir, conf.LOCATION_FILE), "w") as fp:
            fp.write("bla")

        version_dir = os.path.join(cate_dir, "2.0.0")
        os.mkdir(version_dir)
        version_config_file = os.path.join(version_dir, "conf.py")
        with open(version_config_file, "w") as fp:
            fp.write("b = 3\nc = 4\n")

        local_config_file = os.path.join(test_dir, "cate-conf.py")
        with open(local_config_file, "w") as fp:
            fp.write("c = 5\nd = 6\n")

        try:
            config = conf._read_config_files([default_config_file, version_config_file, local_config_file])
            self.assertIsNotNone(config)
            self.assertEqual(config.get('a'), 1)
            self.assertEqual(config.get('b'), 3)
            self.assertEqual(config.get('c'), 5)
            self.assertEqual(config.get('d'), 6)
        finally:
            shutil.rmtree(test_dir)

    def test_write_location_files(self):
        test_dir = os.path.join(tempfile.gettempdir(), "cate_test_write_location_files")
        cate_dir = os.path.join(test_dir, ".cate")
        version_dir = os.path.join(cate_dir, "2.0.0")

        try:
            conf._write_location_files([cate_dir, version_dir])
            self.assertTrue(os.path.exists(os.path.join(cate_dir, conf.LOCATION_FILE)))
            self.assertTrue(os.path.exists(os.path.join(version_dir, conf.LOCATION_FILE)))
            with open(os.path.join(cate_dir, conf.LOCATION_FILE)) as fp:
                self.assertEqual(fp.read(), sys.prefix)
            with open(os.path.join(version_dir, conf.LOCATION_FILE)) as fp:
                self.assertEqual(fp.read(), sys.prefix)
        finally:
            shutil.rmtree(test_dir)