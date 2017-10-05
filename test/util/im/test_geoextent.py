from unittest import TestCase

import numpy as np

from cate.util.im.geoextent import GeoExtent


# noinspection PyMethodMayBeStatic
class GeoRectTest(TestCase):
    def test_init(self):
        rect = GeoExtent()
        self.assertEqual(rect.west, -180.)
        self.assertEqual(rect.south, -90.)
        self.assertEqual(rect.east, 180.)
        self.assertEqual(rect.north, 90.)
        self.assertEqual(rect.inv_y, False)
        self.assertEqual(rect.eps, 1e-04)
        self.assertEqual(str(rect), '-180.0, -90.0, 180.0, 90.0')
        self.assertEqual(repr(rect), 'GeoExtend()')
        self.assertEqual(rect.crosses_antimeridian, False)

        with self.assertRaises(ValueError):
            GeoExtent(west=-180.1)
        with self.assertRaises(ValueError):
            GeoExtent(south=-90.1)
        with self.assertRaises(ValueError):
            GeoExtent(east=180.1)
        with self.assertRaises(ValueError):
            GeoExtent(north=90.1)
        with self.assertRaises(ValueError):
            GeoExtent(west=20, east=20 + 0.9e-6)
        with self.assertRaises(ValueError):
            GeoExtent(south=20, north=20 + 0.9e-6)
        with self.assertRaises(ValueError):
            GeoExtent(south=21, north=20)

    def test_repr(self):
        self.assertEqual(repr(GeoExtent()), 'GeoExtend()')
        self.assertEqual(repr(GeoExtent(west=43.2)), 'GeoExtend(west=43.2)')
        self.assertEqual(repr(GeoExtent(south=43.2)), 'GeoExtend(south=43.2)')
        self.assertEqual(repr(GeoExtent(east=43.2)), 'GeoExtend(east=43.2)')
        self.assertEqual(repr(GeoExtent(north=43.2)), 'GeoExtend(north=43.2)')
        self.assertEqual(repr(GeoExtent(inv_y=True)), 'GeoExtend(inv_y=True)')
        self.assertEqual(repr(GeoExtent(inv_y=False, eps=0.001)), 'GeoExtend(eps=0.001)')
        self.assertEqual(repr(GeoExtent(12.5, 43.2, 180.0, 64.1, inv_y=True)),
                         'GeoExtend(west=12.5, south=43.2, north=64.1, inv_y=True)')

    def test_crosses_antimeridian(self):
        self.assertEqual(GeoExtent(west=170., east=-160.).crosses_antimeridian, True)
        self.assertEqual(GeoExtent(west=-170., east=160.).crosses_antimeridian, False)

    def test_from_coord_arrays(self):
        rect = GeoExtent.from_coord_arrays(np.array([1, 2, 3, 4, 5, 6]), np.array([1, 2, 3]))
        np.testing.assert_almost_equal(np.array(rect.coords), np.array((0.5, 0.5, 6.5, 3.5)))
        self.assertEqual(rect.inv_y, True)

        rect = GeoExtent.from_coord_arrays(np.array([1, 2, 3, 4, 5, 6]), np.array([3, 2, 1]))
        np.testing.assert_almost_equal(np.array(rect.coords), np.array((0.5, 0.5, 6.5, 3.5)))
        self.assertEqual(rect.inv_y, False)

        rect = GeoExtent.from_coord_arrays(np.array([-3, -2, -1, 0, 1, 2]), np.array([3, 2, 1]))
        np.testing.assert_almost_equal(np.array(rect.coords), np.array((-3.5, 0.5, 2.5, 3.5)))
        self.assertEqual(rect.inv_y, False)

        rect = GeoExtent.from_coord_arrays(np.array([177, 178, 179, -180, -179, -178]), np.array([3, 2, 1]))
        np.testing.assert_almost_equal(np.array(rect.coords), np.array((176.5, 0.5, -177.5, 3.5)))
        self.assertEqual(rect.inv_y, False)

        rect = GeoExtent.from_coord_arrays(np.array([-150., -90., -30., 30., 90., 150.]), np.array([-60., 0., 60.]))
        np.testing.assert_almost_equal(np.array(rect.coords), np.array((-180.0, -90.0, 180.0, 90.0)))
        self.assertEqual(rect.inv_y, True)

        rect = GeoExtent.from_coord_arrays(np.array([-150., -90., -30., 30., 90., 150.]), np.array([60., 0., -60.]))
        np.testing.assert_almost_equal(np.array(rect.coords), np.array((-180.0, -90.0, 180.0, 90.0)))
        self.assertEqual(rect.inv_y, False)

    def test_from_coord_arrays_with_eps(self):
        eps = 1e-4
        eps025 = 0.25 * eps
        rect = GeoExtent.from_coord_arrays(
            np.array([-150. - eps025, -90. + eps025, -30. + eps025, 30. - eps025, 90. - eps025, 150. + eps025]),
            np.array([-60. - eps025, 0. - eps025, 60. - eps025]), eps=eps)
        np.testing.assert_almost_equal(np.array(rect.coords), np.array([-180.0, -90.0, 180.0, 90.0]))

    def test_from_coord_arrays_edge_cases(self):
        rect = GeoExtent.from_coord_arrays(np.array([10.8]), np.array([4.0, 4.2, 4.4, 4.6, 4.8]))
        np.testing.assert_almost_equal(np.array(rect.coords), np.array([10.7, 3.9, 10.9, 4.9]))

        rect = GeoExtent.from_coord_arrays(np.array([10.8, 11.0, 11.2]), np.array([4.4]))
        np.testing.assert_almost_equal(np.array(rect.coords), np.array([10.7, 4.3, 11.3, 4.5]))

        with self.assertRaises(ValueError):
            GeoExtent.from_coord_arrays(np.array([10.8]), np.array([4.4]))

    def test_from_coord_arrays_2d_lon_lat(self):
        rect = GeoExtent.from_coord_arrays(
            np.array([[3.0, 3.2, 3.4], [3.0, 3.2, 0.4], [3.0, 3.2, 3.4]]),
            np.array([[4.0, 4.0, 4.0], [4.2, 4.2, 4.2], [4.4, 4.4, 4.4]]))
        np.testing.assert_almost_equal(np.array(rect.coords), np.array([2.9, 3.9, 3.5, 4.5]))

    def test_from_coord_arrays_illegal(self):
        with self.assertRaises(ValueError):
            GeoExtent.from_coord_arrays(
                np.array([0.0, 0.2, 0.4, 0.601, 0.8]),
                np.array([4.0, 4.2, 4.4, 4.6, 4.8]))

        with self.assertRaises(ValueError):
            GeoExtent.from_coord_arrays(
                np.array([0.0, 0.2, 0.4, 0.6, 0.8]),
                np.array([4.0, 4.2, 4.4, 4.601, 4.8]))
