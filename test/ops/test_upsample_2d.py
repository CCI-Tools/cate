import unittest

import numpy as np

import cate.ops.resampling as rs

NAN = np.nan


class Upsample2dTest(unittest.TestCase):
    def _test_upsample_2d(self, src, out_w, out_h, method, fill_value, desired):
        if not isinstance(src, (np.ndarray, np.generic)):
            src = np.array(src)
        if not isinstance(desired, (np.ndarray, np.generic)):
            desired = np.array(desired)

        actual = rs.upsample_2d(src, out_w, out_h, method=method, fill_value=fill_value)
        np.testing.assert_almost_equal(actual=desired, desired=actual)

        if isinstance(src, np.ma.MaskedArray):
            self.assertEqual(type(desired), np.ma.MaskedArray)
            np.testing.assert_equal(actual=actual.mask, desired=desired.mask)
            self.assertEqual(fill_value, desired.fill_value)

    def test_no_op(self):
        self._test_upsample_2d([[1., 2.], [3., 4.]],
                               2, 2, rs.US_NEAREST, -1.,
                               [[1., 2.], [3., 4.]])

    def test_interpolation_linear(self):
        self._test_upsample_2d([[1., 2., 3.]],
                               5, 1, rs.US_LINEAR, -1.,
                               [[1, 1.5, 2, 2.5, 3]]),

        self._test_upsample_2d([[1., 2., 3.]],
                               9, 1, rs.US_LINEAR, -1.,
                               [[1, 1.25, 1.5, 1.75, 2, 2.25, 2.5, 2.75, 3]])

        self._test_upsample_2d([[1., 2., 3., 4.]],
                               6, 1, rs.US_LINEAR, -1.,
                               [[1, 1.6, 2.2, 2.8, 3.4, 4]]),

        self._test_upsample_2d([[1., 2.],
                                [3., 4.]],
                               4, 4, rs.US_LINEAR, -1.,
                               [[3. / 3, 4. / 3, 5. / 3, 6. / 3],
                                [5. / 3, 6. / 3, 7. / 3, 8. / 3],
                                [7. / 3, 8. / 3, 9. / 3, 10. / 3],
                                [9. / 3, 10. / 3, 11. / 3, 12. / 3]]),

        self._test_upsample_2d([[1., 2.],
                                [3., NAN]],
                               4, 4, rs.US_LINEAR, 99,
                               [[1., 1., 2., 2.],
                                [1., 1., 2., 2.],
                                [3., 3., 99, 99],
                                [3., 3., 99, 99]])

        self._test_upsample_2d([[1., 2., 3.],
                                [1., 2., 3.],
                                [1., 2., NAN]],
                               5, 5, rs.US_LINEAR, 999,
                               [[1., 1.5, 2., 2.5, 3.],
                                [1., 1.5, 2., 2.5, 3.],
                                [1., 1.5, 2., 3., 3.],
                                [1., 1.5, 2., 999, 999],
                                [1., 1.5, 2., 999, 999]])

    def test_interpolation_linear_masked(self):
        self._test_upsample_2d(np.ma.array([[1., 2., 3.],
                                            [1., 2., 3.],
                                            [1., 2., 3.]],
                                           mask=[[0, 0, 0],
                                                 [0, 0, 0],
                                                 [0, 0, 1]]),
                               5, 5, rs.US_LINEAR, 999,
                               np.ma.array([[1., 1.5, 2., 2.5, 3.],
                                            [1., 1.5, 2., 2.5, 3.],
                                            [1., 1.5, 2., 3., 3.],
                                            [1., 1.5, 2., 999, 999],
                                            [1., 1.5, 2., 999, 999]],
                                           fill_value=999,
                                           mask=[[0, 0, 0, 0, 0],
                                                 [0, 0, 0, 0, 0],
                                                 [0, 0, 0, 0, 0],
                                                 [0, 0, 0, 1, 1],
                                                 [0, 0, 0, 1, 1]]))

    def test_nearest(self):
        self._test_upsample_2d([[1., 2., 3.]],
                               5, 1, rs.US_NEAREST, -1.,
                               [[1., 1., 2., 2., 3.]]),

        self._test_upsample_2d([[1., 2., 3.]],
                               9, 1, rs.US_NEAREST, -1.,
                               [[1., 1., 1., 2., 2., 2., 3., 3., 3.]])

        self._test_upsample_2d([[1., 2., 3., 4.]],
                               6, 1, rs.US_NEAREST, -1.,
                               [[1., 1., 2., 3., 3., 4.]]),

        self._test_upsample_2d([[1., 2.],
                                [3., 4.]],
                               4, 4, rs.US_NEAREST, -1.,
                               [[1., 1., 2., 2.],
                                [1., 1., 2., 2.],
                                [3., 3., 4., 4.],
                                [3., 3., 4., 4.]]),

        self._test_upsample_2d([[1., 2.],
                                [3., 4.]],
                               6, 4, rs.US_NEAREST, -1.,
                               [[1., 1., 1., 2., 2., 2.],
                                [1., 1., 1., 2., 2., 2.],
                                [3., 3., 3., 4., 4., 4.],
                                [3., 3., 3., 4., 4., 4.]])

        self._test_upsample_2d([[1., 2.],
                                [3., NAN]],
                               6, 4, rs.US_NEAREST, -1.,
                               [[1., 1., 1., 2., 2., 2.],
                                [1., 1., 1., 2., 2., 2.],
                                [3., 3., 3., -1, -1, -1],
                                [3., 3., 3., -1, -1, -1]])

        self._test_upsample_2d([[1., 2., 3.],
                                [1., 2., 3.],
                                [1., 2., NAN]],
                               5, 5, rs.US_NEAREST, -1,
                               [[1., 1., 2., 2., 3.],
                                [1., 1., 2., 2., 3.],
                                [1., 1., 2., 2., 3.],
                                [1., 1., 2., 2, 3],
                                [1., 1., 2., 2, -1]])

    def test_nearest_masked(self):
        self._test_upsample_2d(np.ma.array([[1., 2., 3.],
                                            [1., 2., 3.],
                                            [1., 2., 3.]],
                                           mask=[[0, 0, 0],
                                                 [0, 0, 0],
                                                 [0, 0, 1]]),
                               5, 5, rs.US_NEAREST, -1,
                               np.ma.array([[1., 1., 2., 2., 3.],
                                            [1., 1., 2., 2., 3.],
                                            [1., 1., 2., 2., 3.],
                                            [1., 1., 2., 2., 3.],
                                            [1., 1., 2., 2., -1]],
                                           fill_value=-1.,
                                           mask=[[0, 0, 0, 0, 0],
                                                 [0, 0, 0, 0, 0],
                                                 [0, 0, 0, 0, 0],
                                                 [0, 0, 0, 0, 0],
                                                 [0, 0, 0, 0, 1]]))
