from unittest import TestCase

import cate.util.minheap as minheap


class MinHeapTest(TestCase):
    def test_build(self):
        #self.assertEqual(minheap.build([]), [])
        #self.assertEqual(minheap.build([1]), [1])
        #self.assertEqual(minheap.build([2, 1]), [1, 2])
        self.assertEqual(minheap.build([3, 2, 1]), [1, 2, 3])
        #self.assertEqual(minheap.build([4, 3, 2, 1]), [1, 3, 2, 4])
        #self.assertEqual(minheap.build([5, 4, 3, 2, 1]), [1, 3, 2, 4, 5])

    def test_polygon_simplification(self):
        pass


# import numba
# import numpy as np
#
# @numba.jit(nopython=True)
# def area(x1: float, y1: float, x2: float, y2: float) -> float:
#     return 0.5 * abs(x1 * y2 - y1 * x2)
#
# @numba.jit(nopython=True)
# def simplify(n, x, y):
#     if n < 4:
#         return
#     m = n - 2
#     areas = np.empty(m)
#     min_area = 1e10
#     min_area_i = -1
#     for i in range(m):
#         x0 = x[i]
#         y0 = y[i]
#         a = area(x[i + 1] - x0, y[i + 1] - y0, x[i + 2] - x0, y[i + 2] - y0)
#         if a < min_area:
#             min_area = a
#             min_area_i = i
#         areas[i] = a
#
#     if min_area_i >= 0:
#         pass
