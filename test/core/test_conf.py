import io
import os.path
import unittest

from cate.conf.conf import get_config_value, get_config_path, get_config, _read_python_config


class ConfTest(unittest.TestCase):
    def test_get_config_value(self):
        with self.assertRaises(ValueError) as e:
            get_config_value(None)
        self.assertEqual(str(e.exception), 'name argument must be given')

        with self.assertRaises(ValueError) as e:
            get_config_value('')
        self.assertEqual(str(e.exception), 'name argument must be given')

        value = get_config_value('_im_not_in_', 'Yes!')
        self.assertEqual(value, 'Yes!')

    def test_get_config_path(self):
        value = get_config_path('_im_not_in_', default='~/.cate/data_stores')
        self.assertIsNotNone(value)
        self.assertTrue(value.endswith('/.cate/data_stores'))
        self.assertNotIn('~', value)

    def test_get_config(self):
        config = get_config()
        self.assertIsNotNone(config)

    def test_read_python_config_file(self):
        config = _read_python_config(io.StringIO("import os.path\n"
                                                 "root_dir = os.path.join('user', 'home', 'norman')"))
        self.assertIn('root_dir', config)
        self.assertEqual(config['root_dir'], os.path.join('user', 'home', 'norman'))
