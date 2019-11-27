import os
from unittest import TestCase

from cate.core.pathmanag import PathManager
from cate.core.types import ValidationError


class PathManagerTest(TestCase):

    def test_construct_with_root_path(self):
        path_manag = PathManager('/home/tom/somewhere')

        self.assertEqual('/home/tom/somewhere', path_manag.get_root_path())

    def test_construct_with_relative_path(self):
        path_manag = PathManager(os.curdir)

        self.assertEqual(os.path.abspath(os.curdir), path_manag.get_root_path())

    def test_resolve(self):
        root_dir = os.path.abspath(os.curdir)
        path_manag = PathManager(root_dir)

        path = path_manag.resolve("heffalump")
        self.assertEqual(os.path.join(root_dir, "heffalump"), path)

    def test_resolve_multiple_segments(self):
        root_dir = os.path.abspath(os.curdir)
        path_manag = PathManager(root_dir)

        path = path_manag.resolve("nasenmann/firlefanz")

        expected = os.path.join(root_dir, 'nasenmann', 'firlefanz')
        self.assertEqual(expected, path)

    def test_resolve_try_to_escape(self):
        root_dir = os.path.abspath(os.curdir)
        path_manag = PathManager(root_dir)

        try:
            path_manag.resolve("../../../heffalump")

            self.fail('ValidationError expected')
        except ValidationError:
            pass

    def test_resolve_absolute_path_in_root(self):
        root_dir = os.path.abspath(os.curdir)
        path_manag = PathManager(root_dir)

        test_dir = os.path.join(root_dir, "hirsebrei")
        path = path_manag.resolve(test_dir)
        self.assertEqual(os.path.join(root_dir, "hirsebrei"), path)

    def test_resolve_absolute_path_outside_root(self):
        root_dir = os.path.abspath(os.curdir)
        path_manag = PathManager(root_dir)

        test_dir = os.path.join(root_dir, "../../hirsebrei")
        test_dir = os.path.normpath(test_dir)

        try:
            path_manag.resolve(test_dir)

            self.fail('ValidationError expected')
        except ValidationError:
            pass