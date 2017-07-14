from unittest import TestCase
import os.path
from cate.util.tmpfile import new_temp_file, del_temp_file, del_temp_files, get_temp_files


class TempFileTest(TestCase):

    def setUp(self):
        del_temp_files(force=True)
        self.assertEqual(get_temp_files(), [])

    def test_all(self):

        p1 = new_temp_file(prefix='test-')
        p2 = new_temp_file(suffix='.nc')
        self.assertEqual(len(get_temp_files()), 2)
        self.assertEqual(get_temp_files()[0], p1)
        self.assertEqual(get_temp_files()[1], p2)
        self.assertTrue(os.path.basename(get_temp_files()[0][1]).startswith('test-'))
        self.assertTrue(os.path.basename(get_temp_files()[1][1]).endswith('.nc'))

        del_temp_files()
        self.assertEqual(len(get_temp_files()), 0)

        p1 = new_temp_file(prefix='test-')
        p2 = new_temp_file(suffix='.nc')
        del_temp_file(p1[1])
        self.assertEqual(len(get_temp_files()), 1)
        del_temp_file(p2[1])
        self.assertEqual(len(get_temp_files()), 0)
