import os

from ..core.types import ValidationError

__author__ = "Tom Block (Brockmann Consult GmbH)"


class PathManager:

    def __init__(self, root_path):
        self._rootpath = os.path.abspath(root_path)

    def get_root_path(self):
        return self._rootpath

    def resolve(self, rel_path):
        joined_path = os.path.join(self._rootpath, rel_path)
        normed_path = os.path.normpath(joined_path)

        if self._escapes(normed_path):
            raise ValidationError("Resolves to path outside root path")

        return normed_path

    def _escapes(self, sub_path):
        reference = os.path.commonpath([self._rootpath])
        compare = os.path.commonpath([self._rootpath, sub_path])
        return reference != compare
