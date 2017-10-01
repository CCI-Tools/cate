from unittest import TestCase

import numpy as np

from cate.util.im.geospatialrect import get_geo_spatial_rect


class GeoRectTest(TestCase):
    def test_get_geo_spatial_rect(self):
        self.assertEqual(get_geo_spatial_rect(np.array([1, 2, 3, 4, 5, 6]),
                                              np.array([1, 2, 3])),
                         (0.5, 0.5, 6.5, 3.5))

        self.assertEqual(get_geo_spatial_rect(np.array([1, 2, 3, 4, 5, 6]),
                                              np.array([3, 2, 1])),
                         (0.5, 3.5, 6.5, 0.5))

        self.assertEqual(get_geo_spatial_rect(np.array([-3, -2, -1, 0, 1, 2]),
                                              np.array([3, 2, 1])),
                         (-3.5, 3.5, 2.5, 0.5))

        self.assertEqual(get_geo_spatial_rect(np.array([177, 178, 179, -180, -179, -178]),
                                              np.array([3, 2, 1])),
                         (176.5, 3.5, 182.5, 0.5))

        self.assertEqual(get_geo_spatial_rect(np.array([-150., -90., -30., 30., 90., 150.]),
                                              np.array([-60., 0., 60.])),
                         (-180.0, -90.0, 180.0, 90.0))
        self.assertEqual(get_geo_spatial_rect(np.array([-150., -90., -30., 30., 90., 150.]),
                                              np.array([60., 0., -60.])),
                         (-180.0, 90.0, 180.0, -90.0))

    def test_get_geo_spatial_rect_with_eps(self):
        eps = 1e-4
        eps025 = 0.25 * eps
        self.assertEqual(get_geo_spatial_rect(
            np.array([-150. - eps025, -90. + eps025, -30. + eps025, 30. - eps025, 90. - eps025, 150. + eps025]),
            np.array([-60. - eps025, 0. - eps025, 60. - eps025]), eps=eps),
            (-180.0, -90.0, 180.0, 90.0))

    def test_get_geo_spatial_rect_edge_cases(self):
        rect = get_geo_spatial_rect(np.array([10.8]), np.array([4.0, 4.2, 4.4, 4.6, 4.8]))
        np.testing.assert_almost_equal(np.array(rect), np.array([10.7, 3.9, 10.9, 4.9]))

        rect = get_geo_spatial_rect(np.array([10.8, 11.0, 11.2]), np.array([4.4]))
        np.testing.assert_almost_equal(np.array(rect), np.array([10.7, 4.3, 11.3, 4.5]))

        with self.assertRaises(ValueError):
            get_geo_spatial_rect(np.array([10.8]), np.array([4.4]))

    def test_get_geo_spatial_rect_2d_lon_lat(self):
        rect = get_geo_spatial_rect(
            np.array([[3.0, 3.2, 3.4], [3.0, 3.2, 0.4], [3.0, 3.2, 3.4]]),
            np.array([[4.0, 4.0, 4.0], [4.2, 4.2, 4.2], [4.4, 4.4, 4.4]]))
        np.testing.assert_almost_equal(np.array(rect), np.array([2.9, 3.9, 3.5, 4.5]))

    def test_get_geo_spatial_rect_illegal(self):
        with self.assertRaises(ValueError):
            get_geo_spatial_rect(
                np.array([0.0, 0.2, 0.4, 0.601, 0.8]),
                np.array([4.0, 4.2, 4.4, 4.6, 4.8]))

        with self.assertRaises(ValueError):
            get_geo_spatial_rect(
                np.array([0.0, 0.2, 0.4, 0.6, 0.8]),
                np.array([4.0, 4.2, 4.4, 4.601, 4.8]))
