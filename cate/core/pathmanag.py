import os

from ..conf.defaults import SCRATCH_WORKSPACES_DIR_NAME
from ..core.types import ValidationError

__author__ = "Tom Block (Brockmann Consult GmbH)"


class PathManager:

    def __init__(self, root_path: str):
        self._root_path = os.path.abspath(root_path)

    def get_root_path(self) -> str:
        return self._root_path

    def get_scratch_dir_root(self) -> str:
        return os.path.join(self._root_path, SCRATCH_WORKSPACES_DIR_NAME)

    def resolve(self, rel_path: str) -> str:
        joined_path = os.path.join(self._root_path, rel_path)
        normed_path = os.path.normpath(joined_path)

        if self._escapes(normed_path):
            raise ValidationError("Resolves to path outside root path")

        return normed_path

    def get_relative_path(self, path: str):
        abs_path = os.path.abspath(path)
        common_prefix = os.path.commonprefix([self._root_path, abs_path])

        if common_prefix != self._root_path:
            raise ValidationError("Resolves to path outside root path")

        return os.path.relpath(abs_path, common_prefix)

    def _escapes(self, sub_path: str):
        reference = os.path.commonpath([self._root_path])
        compare = os.path.commonpath([self._root_path, sub_path])
        return reference != compare
