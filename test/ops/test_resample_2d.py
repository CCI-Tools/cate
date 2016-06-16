import unittest

import numpy as np
from numpy.testing import assert_almost_equal

import ect.ops.resampling as rs


def _test_resample_2d(src, out_w, out_h, ds_method, us_method, desired_out):
    actual = rs.resample_2d(np.array(src), out_w, out_h,
                            ds_method=ds_method,
                            us_method=us_method)
    assert_almost_equal(actual, np.array(desired_out))


SRC = [[0.9, 0.5, 3.0, 4.0],
       [1.1, 1.5, 1.0, 2.0],
       [4.0, 2.1, 3.0, 5.0],
       [3.0, 4.9, 3.0, 1.0]]


class Resample2dTest(unittest.TestCase):
    def test_no_op(self):
        _test_resample_2d(SRC,
                          4, 4, rs.DS_FIRST, rs.US_NEAREST,
                          SRC)

    def test_aggregate_w(self):
        _test_resample_2d(SRC,
                          2, 4, rs.DS_FIRST, rs.US_NEAREST,
                          [[0.9, 3.],
                           [1.1, 1.],
                           [4., 3.],
                           [3., 3.]])

    def test_aggregate_w_aggregate_h(self):
        _test_resample_2d(SRC,
                          2, 2, rs.DS_MEAN, rs.US_NEAREST,
                          [[1.0, 2.5],
                           [3.5, 3.0]])

    def test_aggregate_w_interpolate_h(self):
        _test_resample_2d(SRC,
                          2, 8, rs.DS_FIRST, rs.US_NEAREST,
                          [[0.9, 3.],
                           [0.9, 3.],
                           [1.1, 1.],
                           [1.1, 1.],
                           [4., 3.],
                           [4., 3.],
                           [3., 3.],
                           [3., 3.]])

    def test_aggregate_h(self):
        _test_resample_2d(SRC,
                          4, 2, rs.DS_FIRST, rs.US_NEAREST,
                          [[0.9, 0.5, 3., 4.],
                           [4., 2.1, 3., 5.]])

    def test_interpolate_w(self):
        _test_resample_2d(SRC,
                          8, 4, rs.DS_MEAN, rs.US_NEAREST,
                          [[0.9, 0.9, 0.5, 0.5, 3., 3., 4., 4.],
                           [1.1, 1.1, 1.5, 1.5, 1., 1., 2., 2.],
                           [4., 4., 2.1, 2.1, 3., 3., 5., 5.],
                           [3., 3., 4.9, 4.9, 3., 3., 1., 1.]])

    def test__interpolate_w_interpolate_h(self):
        _test_resample_2d(SRC,
                          8, 8, rs.DS_MEAN, rs.US_NEAREST,
                          [[0.9, 0.9, 0.5, 0.5, 3., 3., 4., 4.],
                           [0.9, 0.9, 0.5, 0.5, 3., 3., 4., 4.],
                           [1.1, 1.1, 1.5, 1.5, 1., 1., 2., 2.],
                           [1.1, 1.1, 1.5, 1.5, 1., 1., 2., 2.],
                           [4., 4., 2.1, 2.1, 3., 3., 5., 5.],
                           [4., 4., 2.1, 2.1, 3., 3., 5., 5.],
                           [3., 3., 4.9, 4.9, 3., 3., 1., 1.],
                           [3., 3., 4.9, 4.9, 3., 3., 1., 1.]])

    def test__interpol_w_aggregate_h(self):
        _test_resample_2d(SRC,
                          8, 2, rs.DS_MEAN, rs.US_NEAREST,
                          [[1., 1., 1., 1., 2., 2., 3., 3.],
                           [3.5, 3.5, 3.5, 3.5, 3., 3., 3., 3.]])
