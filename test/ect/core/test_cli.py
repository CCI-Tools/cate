from unittest import TestCase

from ect.core import cli


class CubeConfigTest(TestCase):
    def test_validate(self):
        with self.assertRaises(ValueError):
            CubeConfig(grid_x0=1)
        with self.assertRaises(ValueError):
            CubeConfig(grid_y0=1)
        with self.assertRaises(ValueError):
            CubeConfig(grid_x0=-1)
        with self.assertRaises(ValueError):
            CubeConfig(grid_y0=-1)
