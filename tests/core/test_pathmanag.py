import os
from unittest import TestCase

from cate.core.pathmanag import PathManager
from cate.core.types import ValidationError
from cate.conf.defaults import SCRATCH_WORKSPACES_DIR_NAME


class PathManagerTest(TestCase):

    def test_construct_with_root_path(self):
        path_manag = PathManager('/home/tom/somewhere')

        self.assertEqual(os.path.abspath('/home/tom/somewhere'), path_manag.get_root_path())

    def test_construct_with_relative_path(self):
        path_manag = PathManager(os.curdir)

        self.assertEqual(os.path.abspath(os.curdir), path_manag.get_root_path())

    def test_get_scratch_dir_root(self):
        path_manag = PathManager(os.curdir)

        scratch_dir_root = path_manag.get_scratch_dir_root()

        expected = os.path.abspath(os.curdir)
        expected = os.path.join(expected, SCRATCH_WORKSPACES_DIR_NAME)
        self.assertEqual(expected, scratch_dir_root)

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

    def test_get_relative_path(self):
        path_manag = PathManager('/home/tom/somewhere')

        # @todo 1 tb/tb adapt to also run in windows 2019-11-29
        rel_path = path_manag.get_relative_path('/home/tom/somewhere/in/the/home.file')
        self.assertEqual('in/the/home.file', rel_path)

    def test_get_relative_path_not_in_root(self):
        path_manag = PathManager('/home/tom/somewhere')

        try:
            # @todo 1 tb/tb adapt to also run in windows 2019-11-29
            path_manag.get_relative_path('/local/var/bin/the.file')
            self.fail('ValidationError expected')
        except ValidationError:
            pass


